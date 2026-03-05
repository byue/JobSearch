import importlib
import os
import sys
import time
import threading
import unittest
import warnings
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

from fastapi.testclient import TestClient
from redis import Redis
from testcontainers.redis import RedisContainer
from scrapers.proxy.lease_manager import LeaseManager, LeaseState

warnings.filterwarnings("ignore", message=r"unclosed <socket\.socket.*", category=ResourceWarning)


def _fresh_import_proxy_api(redis_url: str):
    sys.modules.pop("scrapers.proxy.proxy_api", None)
    env = {
        "JOBSEARCH_PROXY_REDIS_URL": redis_url,
        "JOBSEARCH_PROXY_LEASE_TTL_SECONDS": "1",
        "JOBSEARCH_PROXY_LEASE_MAX_ATTEMPTS": "20",
        "JOBSEARCH_PROXY_BLOCKED_COOLDOWN_SECONDS": "1",
    }
    with patch.dict(os.environ, env, clear=False):
        return importlib.import_module("scrapers.proxy.proxy_api")


class ProxyApiIntegrationTest(unittest.TestCase):
    TEST_SCOPE = "integration.test"

    def _wait_for_missing_state(self, resource: str, timeout_seconds: float = 2.5) -> None:
        deadline = time.monotonic() + timeout_seconds
        while time.monotonic() < deadline:
            response = self.client.get("/state", params={"resource": resource, "scope": self.TEST_SCOPE})
            if response.status_code == 404:
                return
            time.sleep(0.05)
        self.fail(f"resource did not transition to missing within {timeout_seconds}s: {resource}")

    def _available_list_and_members(self, scope: str | None = None) -> tuple[list[str], set[str]]:
        resolved_scope = scope or self.TEST_SCOPE
        raw_list = self.redis.lrange(LeaseManager.available_key_for_scope(resolved_scope), 0, -1)
        raw_members = self.redis.smembers(LeaseManager.available_set_key_for_scope(resolved_scope))
        as_text_list = [item.decode("utf-8") for item in raw_list]
        as_text_members = {item.decode("utf-8") for item in raw_members}
        return as_text_list, as_text_members

    @classmethod
    def setUpClass(cls) -> None:
        try:
            cls._redis_container = RedisContainer("redis:7.2-alpine")
            cls._redis_container.start()
        except Exception as exc:  # pragma: no cover - exercised only when Docker is unavailable
            raise unittest.SkipTest(f"Docker/Redis container unavailable: {exc}") from exc

        host = cls._redis_container.get_container_host_ip()
        port = cls._redis_container.get_exposed_port(6379)
        cls.redis_url = f"redis://{host}:{port}/0"
        cls.redis = Redis.from_url(cls.redis_url, decode_responses=False)
        cls.api = _fresh_import_proxy_api(cls.redis_url)
        cls.client = TestClient(cls.api.app)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.client.close()
        cls.redis.close()
        cls.api.redis_client.close()
        cls._redis_container.stop()

    def setUp(self) -> None:
        self.redis.flushdb()

    def test_health_and_missing_state(self) -> None:
        health = self.client.get("/health")
        self.assertEqual(health.status_code, 200)
        self.assertEqual(health.json()["status"], "ok")

        missing = self.client.get("/state", params={"resource": "http://missing:80", "scope": self.TEST_SCOPE})
        self.assertEqual(missing.status_code, 404)

    def test_lease_and_release_flow_updates_sizes_and_state(self) -> None:
        enqueue = self.client.post(
            "/try-enqueue",
            json={"resource": "http://10.0.0.1:8080", "capacity": 5, "scope": self.TEST_SCOPE},
        )
        self.assertEqual(enqueue.status_code, 200)
        self.assertTrue(enqueue.json()["ok"])

        before_lease = self.client.get("/sizes", params={"scope": self.TEST_SCOPE}).json()
        self.assertEqual(before_lease, {"available": 1, "inuse": 0, "blocked": 0})

        lease = self.client.post("/lease", json={"scope": self.TEST_SCOPE})
        self.assertEqual(lease.status_code, 200)
        lease_payload = lease.json()
        self.assertEqual(lease_payload["resource"], "http://10.0.0.1:8080")
        self.assertIsInstance(lease_payload["token"], str)
        self.assertTrue(lease_payload["token"])

        state_inuse = self.client.get("/state", params={"resource": "http://10.0.0.1:8080", "scope": self.TEST_SCOPE})
        self.assertEqual(state_inuse.status_code, 200)
        self.assertEqual(state_inuse.json()["state"], "INUSE")

        sizes_inuse = self.client.get("/sizes", params={"scope": self.TEST_SCOPE}).json()
        self.assertEqual(sizes_inuse, {"available": 0, "inuse": 1, "blocked": 0})

        release = self.client.post(
            "/release",
            json={"resource": "http://10.0.0.1:8080", "token": lease_payload["token"], "scope": self.TEST_SCOPE},
        )
        self.assertEqual(release.status_code, 200)
        self.assertTrue(release.json()["ok"])

        state_available = self.client.get(
            "/state", params={"resource": "http://10.0.0.1:8080", "scope": self.TEST_SCOPE}
        )
        self.assertEqual(state_available.status_code, 200)
        self.assertEqual(state_available.json()["state"], "AVAILABLE")

    def test_block_flow_and_wrong_token(self) -> None:
        self.client.post("/try-enqueue", json={"resource": "http://10.0.0.2:8080", "capacity": 5, "scope": self.TEST_SCOPE})
        lease = self.client.post("/lease", json={"scope": self.TEST_SCOPE}).json()

        wrong_release = self.client.post(
            "/release",
            json={"resource": "http://10.0.0.2:8080", "token": "wrong", "scope": self.TEST_SCOPE},
        )
        self.assertFalse(wrong_release.json()["ok"])

        block = self.client.post(
            "/block",
            json={"resource": "http://10.0.0.2:8080", "token": lease["token"], "scope": self.TEST_SCOPE},
        )
        self.assertEqual(block.status_code, 200)
        self.assertTrue(block.json()["ok"])

        state_blocked = self.client.get("/state", params={"resource": "http://10.0.0.2:8080", "scope": self.TEST_SCOPE})
        self.assertEqual(state_blocked.status_code, 200)
        self.assertEqual(state_blocked.json()["state"], "BLOCKED")

        sizes = self.client.get("/sizes", params={"scope": self.TEST_SCOPE}).json()
        self.assertEqual(sizes, {"available": 0, "inuse": 0, "blocked": 1})

    def test_block_removes_all_available_duplicates_for_resource(self) -> None:
        resource = "http://10.0.0.23:8080"
        # Duplicate queue entries can happen; blocking should fully remove them from availability.
        available_key = LeaseManager.available_key_for_scope(self.TEST_SCOPE)
        self.redis.lpush(available_key, resource.encode("utf-8"))
        self.redis.lpush(available_key, resource.encode("utf-8"))

        lease = self.client.post("/lease", json={"scope": self.TEST_SCOPE})
        self.assertEqual(lease.status_code, 200)
        lease_payload = lease.json()
        self.assertIsNotNone(lease_payload)
        assert lease_payload is not None
        self.assertEqual(lease_payload["resource"], resource)

        block = self.client.post(
            "/block",
            json={"resource": resource, "token": lease_payload["token"], "scope": self.TEST_SCOPE},
        )
        self.assertEqual(block.status_code, 200)
        self.assertTrue(block.json()["ok"])

        queue = self.redis.lrange(available_key, 0, -1)
        queue_resources = [item.decode("utf-8") for item in queue]
        self.assertNotIn(resource, queue_resources)

    def test_inuse_ttl_expires_to_missing(self) -> None:
        self.client.post(
            "/try-enqueue",
            json={"resource": "http://10.0.0.21:8080", "capacity": 5, "scope": self.TEST_SCOPE},
        )
        lease = self.client.post("/lease", json={"scope": self.TEST_SCOPE})
        self.assertEqual(lease.status_code, 200)
        resource = lease.json()["resource"]
        self.assertEqual(resource, "http://10.0.0.21:8080")

        self._wait_for_missing_state(resource)

    def test_blocked_ttl_expires_and_resource_can_be_reenqueued(self) -> None:
        self.client.post(
            "/try-enqueue",
            json={"resource": "http://10.0.0.22:8080", "capacity": 5, "scope": self.TEST_SCOPE},
        )
        lease = self.client.post("/lease", json={"scope": self.TEST_SCOPE}).json()

        block = self.client.post(
            "/block",
            json={"resource": "http://10.0.0.22:8080", "token": lease["token"], "scope": self.TEST_SCOPE},
        )
        self.assertEqual(block.status_code, 200)
        self.assertTrue(block.json()["ok"])

        blocked_enqueue = self.client.post(
            "/try-enqueue",
            json={"resource": "http://10.0.0.22:8080", "capacity": 5, "scope": self.TEST_SCOPE},
        )
        self.assertEqual(blocked_enqueue.status_code, 200)
        self.assertFalse(blocked_enqueue.json()["ok"])

        self._wait_for_missing_state("http://10.0.0.22:8080")

        enqueue_after_expiry = self.client.post(
            "/try-enqueue",
            json={"resource": "http://10.0.0.22:8080", "capacity": 5, "scope": self.TEST_SCOPE},
        )
        self.assertEqual(enqueue_after_expiry.status_code, 200)
        self.assertTrue(enqueue_after_expiry.json()["ok"])

    def test_concurrent_lease_unique_resources(self) -> None:
        manager = LeaseManager(
            self.redis,
            lease_ttl_seconds=5,
            blocked_ttl_seconds=5,
            max_attempts=100,
        )
        resources = [f"http://10.1.0.{idx}:8080" for idx in range(1, 9)]
        for resource in resources:
            self.assertTrue(manager.try_enqueue(resource, capacity=100, scope=self.TEST_SCOPE))

        barrier = threading.Barrier(16)

        def _lease_once() -> tuple[str, str] | None:
            barrier.wait(timeout=3)
            return manager.lease(scope=self.TEST_SCOPE)

        with ThreadPoolExecutor(max_workers=16) as pool:
            results = list(pool.map(lambda _i: _lease_once(), range(16)))

        leased = [item for item in results if item is not None]
        leased_resources = [item[0] for item in leased]
        leased_tokens = [item[1] for item in leased]

        self.assertEqual(len(leased), len(resources))
        self.assertEqual(len(set(leased_resources)), len(resources))
        self.assertEqual(len(set(leased_tokens)), len(resources))

    def test_concurrent_release_same_lease_exactly_one_success(self) -> None:
        manager = LeaseManager(
            self.redis,
            lease_ttl_seconds=5,
            blocked_ttl_seconds=5,
            max_attempts=20,
        )
        resource = "http://10.2.0.1:8080"
        self.assertTrue(manager.try_enqueue(resource, capacity=10, scope=self.TEST_SCOPE))
        leased = manager.lease(scope=self.TEST_SCOPE)
        self.assertIsNotNone(leased)
        assert leased is not None
        token = leased[1]

        barrier = threading.Barrier(12)

        def _release_once() -> bool:
            barrier.wait(timeout=3)
            return manager.release(resource, token, scope=self.TEST_SCOPE)

        with ThreadPoolExecutor(max_workers=12) as pool:
            outcomes = list(pool.map(lambda _i: _release_once(), range(12)))

        self.assertEqual(sum(1 for ok in outcomes if ok), 1)
        self.assertEqual(manager.get_state(resource, scope=self.TEST_SCOPE), LeaseState.AVAILABLE)

    def test_concurrent_release_vs_block_same_lease_one_wins(self) -> None:
        manager = LeaseManager(
            self.redis,
            lease_ttl_seconds=5,
            blocked_ttl_seconds=5,
            max_attempts=20,
        )
        resource = "http://10.3.0.1:8080"
        self.assertTrue(manager.try_enqueue(resource, capacity=10, scope=self.TEST_SCOPE))
        leased = manager.lease(scope=self.TEST_SCOPE)
        self.assertIsNotNone(leased)
        assert leased is not None
        token = leased[1]

        barrier = threading.Barrier(2)

        def _release() -> bool:
            barrier.wait(timeout=3)
            return manager.release(resource, token, scope=self.TEST_SCOPE)

        def _block() -> bool:
            barrier.wait(timeout=3)
            return manager.block(resource, token, scope=self.TEST_SCOPE)

        with ThreadPoolExecutor(max_workers=2) as pool:
            release_result = pool.submit(_release)
            block_result = pool.submit(_block)
            outcomes = [release_result.result(), block_result.result()]

        self.assertEqual(sum(1 for ok in outcomes if ok), 1)
        self.assertIn(manager.get_state(resource, scope=self.TEST_SCOPE), {LeaseState.AVAILABLE, LeaseState.BLOCKED})

    def test_concurrent_try_enqueue_respects_capacity(self) -> None:
        manager = LeaseManager(
            self.redis,
            lease_ttl_seconds=5,
            blocked_ttl_seconds=5,
            max_attempts=20,
        )
        capacity = 5
        resources = [f"http://10.4.0.{idx}:8080" for idx in range(1, 21)]
        barrier = threading.Barrier(len(resources))

        def _enqueue_once(resource: str) -> bool:
            barrier.wait(timeout=5)
            return manager.try_enqueue(resource, capacity=capacity, scope=self.TEST_SCOPE)

        with ThreadPoolExecutor(max_workers=len(resources)) as pool:
            outcomes = list(pool.map(_enqueue_once, resources))

        self.assertEqual(sum(1 for ok in outcomes if ok), capacity)
        self.assertEqual(manager.sizes(scope=self.TEST_SCOPE)["available"], capacity)

    def test_concurrent_try_enqueue_same_resource_is_idempotent(self) -> None:
        manager = LeaseManager(
            self.redis,
            lease_ttl_seconds=5,
            blocked_ttl_seconds=5,
            max_attempts=20,
        )
        resource = "http://10.4.1.1:8080"
        barrier = threading.Barrier(24)

        def _enqueue_once() -> bool:
            barrier.wait(timeout=5)
            return manager.try_enqueue(resource, capacity=100, scope=self.TEST_SCOPE)

        with ThreadPoolExecutor(max_workers=24) as pool:
            outcomes = list(pool.map(lambda _i: _enqueue_once(), range(24)))

        self.assertEqual(sum(1 for ok in outcomes if ok), 1)
        available_list, available_members = self._available_list_and_members()
        self.assertEqual(available_list, [resource])
        self.assertEqual(available_members, {resource})
        self.assertEqual(manager.sizes(scope=self.TEST_SCOPE)["available"], 1)

    def test_release_does_not_duplicate_with_stale_list_copy(self) -> None:
        manager = LeaseManager(
            self.redis,
            lease_ttl_seconds=5,
            blocked_ttl_seconds=5,
            max_attempts=20,
        )
        resource = "http://10.4.2.1:8080"
        available_key = LeaseManager.available_key_for_scope(self.TEST_SCOPE)
        available_set_key = LeaseManager.available_set_key_for_scope(self.TEST_SCOPE)
        self.redis.lpush(available_key, resource.encode("utf-8"))
        self.redis.lpush(available_key, resource.encode("utf-8"))
        self.redis.sadd(available_set_key, resource.encode("utf-8"))

        leased = manager.lease(scope=self.TEST_SCOPE)
        self.assertIsNotNone(leased)
        assert leased is not None
        self.assertEqual(leased[0], resource)

        released = manager.release(resource, leased[1], scope=self.TEST_SCOPE)
        self.assertTrue(released)
        available_list, available_members = self._available_list_and_members()
        self.assertEqual(available_list, [resource])
        self.assertEqual(available_members, {resource})

    def test_available_members_matches_available_after_mixed_ops(self) -> None:
        manager = LeaseManager(
            self.redis,
            lease_ttl_seconds=5,
            blocked_ttl_seconds=5,
            max_attempts=100,
        )
        resources = [f"http://10.4.3.{idx}:8080" for idx in range(1, 11)]
        for resource in resources:
            manager.try_enqueue(resource, capacity=100, scope=self.TEST_SCOPE)

        leased: list[tuple[str, str]] = []
        for _ in range(5):
            item = manager.lease(scope=self.TEST_SCOPE)
            self.assertIsNotNone(item)
            assert item is not None
            leased.append(item)

        for resource, token in leased[:2]:
            self.assertTrue(manager.release(resource, token, scope=self.TEST_SCOPE))
        for resource, token in leased[2:4]:
            self.assertTrue(manager.block(resource, token, scope=self.TEST_SCOPE))

        for resource in resources[:3]:
            manager.try_enqueue(resource, capacity=100, scope=self.TEST_SCOPE)

        available_list, available_members = self._available_list_and_members()
        self.assertEqual(set(available_list), available_members)
        self.assertEqual(len(available_list), len(available_members))

    def test_concurrent_api_lease_requests_unique(self) -> None:
        resources = [f"http://10.5.0.{idx}:8080" for idx in range(1, 11)]
        for resource in resources:
            response = self.client.post(
                "/try-enqueue",
                json={"resource": resource, "capacity": 50, "scope": self.TEST_SCOPE},
            )
            self.assertEqual(response.status_code, 200)
            self.assertTrue(response.json()["ok"])

        barrier = threading.Barrier(25)

        def _lease_once() -> str | None:
            barrier.wait(timeout=5)
            response = self.client.post("/lease", json={"scope": self.TEST_SCOPE})
            self.assertEqual(response.status_code, 200)
            payload = response.json()
            if payload is None:
                return None
            return payload["resource"]

        with ThreadPoolExecutor(max_workers=25) as pool:
            leased_resources = list(pool.map(lambda _i: _lease_once(), range(25)))

        leased_non_null = [item for item in leased_resources if item is not None]
        self.assertEqual(len(leased_non_null), len(resources))
        self.assertEqual(len(set(leased_non_null)), len(resources))

    def test_concurrent_mixed_lease_and_enqueue_invariants(self) -> None:
        manager = LeaseManager(
            self.redis,
            lease_ttl_seconds=5,
            blocked_ttl_seconds=5,
            max_attempts=100,
        )
        initial_resources = [f"http://10.6.0.{idx}:8080" for idx in range(1, 4)]
        for resource in initial_resources:
            self.assertTrue(manager.try_enqueue(resource, capacity=100, scope=self.TEST_SCOPE))

        enqueue_resources = [f"http://10.6.1.{idx}:8080" for idx in range(1, 31)]
        lease_workers = 20
        capacity = 20
        barrier = threading.Barrier(lease_workers + len(enqueue_resources))

        def _lease_once() -> tuple[str, str] | None:
            barrier.wait(timeout=5)
            return manager.lease(scope=self.TEST_SCOPE)

        def _enqueue_once(resource: str) -> bool:
            barrier.wait(timeout=5)
            return manager.try_enqueue(resource, capacity=capacity, scope=self.TEST_SCOPE)

        with ThreadPoolExecutor(max_workers=lease_workers + len(enqueue_resources)) as pool:
            lease_futures = [pool.submit(_lease_once) for _ in range(lease_workers)]
            enqueue_futures = [pool.submit(_enqueue_once, resource) for resource in enqueue_resources]
            lease_results = [future.result() for future in lease_futures]
            enqueue_results = [future.result() for future in enqueue_futures]

        lease_successes = [item for item in lease_results if item is not None]
        enqueue_success_count = sum(1 for ok in enqueue_results if ok)
        sizes = manager.sizes(scope=self.TEST_SCOPE)

        self.assertEqual(sizes["inuse"], len(lease_successes))
        self.assertEqual(
            sizes["available"],
            len(initial_resources) + enqueue_success_count - len(lease_successes),
        )
        self.assertLessEqual(sizes["available"], capacity)
        leased_resources = [item[0] for item in lease_successes]
        self.assertEqual(len(leased_resources), len(set(leased_resources)))

    def test_stale_token_rejected_after_ttl_under_concurrency(self) -> None:
        manager = LeaseManager(
            self.redis,
            lease_ttl_seconds=1,
            blocked_ttl_seconds=5,
            max_attempts=20,
        )
        resource = "http://10.7.0.1:8080"
        self.assertTrue(manager.try_enqueue(resource, capacity=10, scope=self.TEST_SCOPE))
        first_lease = manager.lease(scope=self.TEST_SCOPE)
        self.assertIsNotNone(first_lease)
        assert first_lease is not None
        stale_token = first_lease[1]

        deadline = time.monotonic() + 2.5
        while time.monotonic() < deadline:
            if manager.get_state(resource, scope=self.TEST_SCOPE) == LeaseState.MISSING:
                break
            time.sleep(0.05)
        self.assertEqual(manager.get_state(resource, scope=self.TEST_SCOPE), LeaseState.MISSING)

        self.assertTrue(manager.try_enqueue(resource, capacity=10, scope=self.TEST_SCOPE))
        second_lease = manager.lease(scope=self.TEST_SCOPE)
        self.assertIsNotNone(second_lease)
        assert second_lease is not None
        valid_token = second_lease[1]

        barrier = threading.Barrier(2)

        def _release_stale() -> bool:
            barrier.wait(timeout=3)
            return manager.release(resource, stale_token, scope=self.TEST_SCOPE)

        def _release_valid() -> bool:
            barrier.wait(timeout=3)
            return manager.release(resource, valid_token, scope=self.TEST_SCOPE)

        with ThreadPoolExecutor(max_workers=2) as pool:
            stale_future = pool.submit(_release_stale)
            valid_future = pool.submit(_release_valid)
            stale_outcome = stale_future.result(timeout=5)
            valid_outcome = valid_future.result(timeout=5)

        self.assertFalse(stale_outcome)
        self.assertTrue(valid_outcome)
        self.assertEqual(manager.get_state(resource, scope=self.TEST_SCOPE), LeaseState.AVAILABLE)

    def test_concurrent_api_release_vs_block_same_lease_one_wins(self) -> None:
        resource = "http://10.8.0.1:8080"
        enqueue = self.client.post(
            "/try-enqueue", json={"resource": resource, "capacity": 10, "scope": self.TEST_SCOPE}
        )
        self.assertEqual(enqueue.status_code, 200)
        self.assertTrue(enqueue.json()["ok"])

        lease = self.client.post("/lease", json={"scope": self.TEST_SCOPE})
        self.assertEqual(lease.status_code, 200)
        payload = lease.json()
        self.assertIsNotNone(payload)
        assert payload is not None
        token = payload["token"]

        barrier = threading.Barrier(2)

        def _release() -> bool:
            barrier.wait(timeout=3)
            response = self.client.post(
                "/release",
                json={"resource": resource, "token": token, "scope": self.TEST_SCOPE},
            )
            self.assertEqual(response.status_code, 200)
            return bool(response.json()["ok"])

        def _block() -> bool:
            barrier.wait(timeout=3)
            response = self.client.post(
                "/block",
                json={"resource": resource, "token": token, "scope": self.TEST_SCOPE},
            )
            self.assertEqual(response.status_code, 200)
            return bool(response.json()["ok"])

        with ThreadPoolExecutor(max_workers=2) as pool:
            release_future = pool.submit(_release)
            block_future = pool.submit(_block)
            outcomes = [release_future.result(timeout=5), block_future.result(timeout=5)]

        self.assertEqual(sum(1 for ok in outcomes if ok), 1)
        state_response = self.client.get("/state", params={"resource": resource, "scope": self.TEST_SCOPE})
        self.assertEqual(state_response.status_code, 200)
        self.assertIn(state_response.json()["state"], {"AVAILABLE", "BLOCKED"})

    def test_release_block_race_stress_iterations(self) -> None:
        manager = LeaseManager(
            self.redis,
            lease_ttl_seconds=30,
            blocked_ttl_seconds=5,
            max_attempts=20,
        )
        for iteration in range(10):
            resource = f"http://10.9.0.{iteration}:8080"
            self.assertTrue(manager.try_enqueue(resource, capacity=100, scope=self.TEST_SCOPE))
            leased = manager.lease(scope=self.TEST_SCOPE)
            self.assertIsNotNone(leased)
            assert leased is not None
            leased_resource = leased[0]
            token = leased[1]

            barrier = threading.Barrier(2)

            def _release() -> bool:
                barrier.wait(timeout=3)
                return manager.release(leased_resource, token, scope=self.TEST_SCOPE)

            def _block() -> bool:
                barrier.wait(timeout=3)
                return manager.block(leased_resource, token, scope=self.TEST_SCOPE)

            with ThreadPoolExecutor(max_workers=2) as pool:
                release_future = pool.submit(_release)
                block_future = pool.submit(_block)
                outcomes = [release_future.result(timeout=5), block_future.result(timeout=5)]

            self.assertEqual(sum(1 for ok in outcomes if ok), 1)
            self.assertIn(manager.get_state(leased_resource, scope=self.TEST_SCOPE), {LeaseState.AVAILABLE, LeaseState.BLOCKED})
