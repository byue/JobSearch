import importlib
import os
import sys
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient
from redis import Redis

from scrapers.proxy import proxy_producer
from integration.scrapers.proxy.shared_redis_container import get_shared_redis_url


class _FakeProxyGeneratorClient:
    def __init__(self, validate_timeout_seconds: float, list_fetch_timeout_seconds: float) -> None:
        self.validate_timeout_seconds = validate_timeout_seconds
        self.list_fetch_timeout_seconds = list_fetch_timeout_seconds

    def get_proxy_urls(self) -> list[str]:
        return [
            "http://10.0.0.10:8080",
            "http://10.0.0.11:8080",
        ]

    def is_proxy_valid(self, proxy_url: str) -> bool:
        return proxy_url.endswith("10:8080")


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


class ProxyEndToEndIntegrationTest(unittest.TestCase):
    TEST_SCOPE = "default"

    @classmethod
    def setUpClass(cls) -> None:
        try:
            cls.redis_url = get_shared_redis_url()
        except Exception as exc:  # pragma: no cover - exercised only when Docker is unavailable
            raise unittest.SkipTest(f"Docker/Redis container unavailable: {exc}") from exc

        cls.redis = Redis.from_url(cls.redis_url, decode_responses=False)
        cls.api = _fresh_import_proxy_api(cls.redis_url)
        cls.client = TestClient(cls.api.app)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.client.close()
        cls.redis.close()
        cls.api.redis_client.close()

    def setUp(self) -> None:
        self.redis.flushdb()
        proxy_producer._STOP = False

    def tearDown(self) -> None:
        proxy_producer._STOP = False

    def _run_producer_once(self) -> int:
        env = {
            "JOBSEARCH_PROXY_REDIS_URL": self.redis_url,
            "JOBSEARCH_PROXY_SCOPES": "default",
            "JOBSEARCH_PROXY_QUEUE_MAX_SIZE": "5",
            "JOBSEARCH_PROXY_VALIDATE_TIMEOUT_SECONDS": "0.5",
            "JOBSEARCH_PROXY_LIST_FETCH_TIMEOUT_SECONDS": "0.5",
            "JOBSEARCH_PROXY_VALIDATE_WORKERS": "1",
            "JOBSEARCH_PROXY_PRODUCER_SLEEP_SECONDS": "0.01",
            "JOBSEARCH_PROXY_PRODUCER_HEARTBEAT_SECONDS": "0.01",
            "JOBSEARCH_PROXY_LEASE_TTL_SECONDS": "1",
            "JOBSEARCH_PROXY_LEASE_MAX_ATTEMPTS": "20",
            "JOBSEARCH_PROXY_BLOCKED_COOLDOWN_SECONDS": "1",
        }

        def _stop_after_sleep(_seconds: float) -> None:
            proxy_producer._STOP = True

        with patch.dict(os.environ, env, clear=False), patch(
            "scrapers.proxy.proxy_producer.signal.signal"
        ), patch(
            "scrapers.proxy.proxy_producer.time.sleep", side_effect=_stop_after_sleep
        ), patch(
            "scrapers.proxy.proxy_producer.ProxyGeneratorClient", _FakeProxyGeneratorClient
        ):
            return proxy_producer.main()

    def test_producer_and_api_end_to_end(self) -> None:
        producer_result = self._run_producer_once()
        self.assertEqual(producer_result, 0)

        sizes_after_produce = self.client.get("/sizes", params={"scope": self.TEST_SCOPE})
        self.assertEqual(sizes_after_produce.status_code, 200)
        self.assertEqual(sizes_after_produce.json()["available"], 1)

        lease = self.client.post("/lease", json={"scope": self.TEST_SCOPE})
        self.assertEqual(lease.status_code, 200)
        lease_payload = lease.json()
        resource = lease_payload["resource"]
        token = lease_payload["token"]
        self.assertEqual(resource, "http://10.0.0.10:8080")

        state_inuse = self.client.get("/state", params={"resource": resource, "scope": self.TEST_SCOPE})
        self.assertEqual(state_inuse.status_code, 200)
        self.assertEqual(state_inuse.json()["state"], "INUSE")

        release = self.client.post("/release", json={"resource": resource, "token": token, "scope": self.TEST_SCOPE})
        self.assertEqual(release.status_code, 200)
        self.assertTrue(release.json()["ok"])

        state_available = self.client.get("/state", params={"resource": resource, "scope": self.TEST_SCOPE})
        self.assertEqual(state_available.status_code, 200)
        self.assertEqual(state_available.json()["state"], "AVAILABLE")

        lease_again = self.client.post("/lease", json={"scope": self.TEST_SCOPE})
        self.assertEqual(lease_again.status_code, 200)
        lease_again_payload = lease_again.json()

        block = self.client.post(
            "/block",
            json={
                "resource": lease_again_payload["resource"],
                "token": lease_again_payload["token"],
                "scope": self.TEST_SCOPE,
            },
        )
        self.assertEqual(block.status_code, 200)
        self.assertTrue(block.json()["ok"])

        state_blocked = self.client.get(
            "/state",
            params={"resource": lease_again_payload["resource"], "scope": self.TEST_SCOPE},
        )
        self.assertEqual(state_blocked.status_code, 200)
        self.assertEqual(state_blocked.json()["state"], "BLOCKED")
