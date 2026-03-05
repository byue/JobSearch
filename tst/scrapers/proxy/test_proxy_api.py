import importlib
import os
import sys
import unittest
from unittest.mock import Mock, patch

from fastapi import HTTPException

from scrapers.proxy.lease_manager import LeaseState


def _fresh_import_proxy_api(env: dict[str, str]):
    sys.modules.pop("scrapers.proxy.proxy_api", None)
    with patch.dict(os.environ, env, clear=False):
        with patch("redis.Redis") as mock_redis_cls:
            with patch("scrapers.proxy.lease_manager.LeaseManager") as mock_lease_manager_cls:
                redis_client = Mock()
                lease_manager = Mock()
                mock_redis_cls.from_url.return_value = redis_client
                mock_lease_manager_cls.return_value = lease_manager
                module = importlib.import_module("scrapers.proxy.proxy_api")
                return module, mock_redis_cls, mock_lease_manager_cls, redis_client, lease_manager


class ProxyApiTest(unittest.TestCase):
    def test_import_prefers_blocked_cooldown_env(self) -> None:
        module, _from_url, lease_cls, _redis_client, _lease_manager = _fresh_import_proxy_api(
            {
                "JOBSEARCH_PROXY_REDIS_URL": "redis://unit-test:6379/0",
                "JOBSEARCH_PROXY_LEASE_TTL_SECONDS": "10",
                "JOBSEARCH_PROXY_LEASE_MAX_ATTEMPTS": "12",
                "JOBSEARCH_PROXY_BLOCKED_COOLDOWN_SECONDS": "30",
            }
        )
        self.assertEqual(module.lease_ttl_seconds, 10)
        self.assertEqual(module.lease_max_attempts, 12)
        self.assertEqual(module.blocked_cooldown_seconds, 30)
        lease_cls.assert_called_once()
        _, kwargs = lease_cls.call_args
        self.assertEqual(kwargs["lease_ttl_seconds"], 10)
        self.assertEqual(kwargs["blocked_ttl_seconds"], 30)
        self.assertEqual(kwargs["max_attempts"], 12)

    def test_import_falls_back_to_deny_cooldown_env(self) -> None:
        module, _from_url, _lease_cls, _redis_client, _lease_manager = _fresh_import_proxy_api(
            {
                "JOBSEARCH_PROXY_REDIS_URL": "redis://unit-test:6379/0",
                "JOBSEARCH_PROXY_LEASE_TTL_SECONDS": "11",
                "JOBSEARCH_PROXY_LEASE_MAX_ATTEMPTS": "13",
                "JOBSEARCH_PROXY_DENY_COOLDOWN_SECONDS": "31",
            }
        )
        self.assertEqual(module.blocked_cooldown_seconds, 31)

    def test_endpoint_functions(self) -> None:
        module, _from_url, _lease_cls, redis_client, lease_manager = _fresh_import_proxy_api(
            {
                "JOBSEARCH_PROXY_REDIS_URL": "redis://unit-test:6379/0",
                "JOBSEARCH_PROXY_LEASE_TTL_SECONDS": "10",
                "JOBSEARCH_PROXY_LEASE_MAX_ATTEMPTS": "12",
                "JOBSEARCH_PROXY_DENY_COOLDOWN_SECONDS": "30",
            }
        )
        lease_manager.sizes.return_value = {"available": 1, "inuse": 2, "blocked": 3}
        lease_manager.lease.side_effect = [("http://1.2.3.4:80", "tok"), None]
        lease_manager.release.return_value = True
        lease_manager.block.return_value = False
        lease_manager.try_enqueue.return_value = True
        lease_manager.get_state.side_effect = [LeaseState.AVAILABLE, LeaseState.MISSING]

        self.assertEqual(module.health(), {"status": "ok"})
        redis_client.ping.assert_called_once()

        self.assertEqual(module.sizes("example.com"), {"available": 1, "inuse": 2, "blocked": 3})

        leased = module.lease(module.LeaseRequest(scope="example.com"))
        self.assertIsNotNone(leased)
        assert leased is not None
        self.assertEqual(leased.resource, "http://1.2.3.4:80")
        self.assertEqual(leased.token, "tok")
        self.assertIsNone(module.lease(module.LeaseRequest(scope="example.com")))

        released = module.release(module.LeaseActionRequest(resource="r1", token="t1", scope="example.com"))
        self.assertTrue(released.ok)
        blocked = module.block(module.LeaseActionRequest(resource="r2", token="t2", scope="example.com"))
        self.assertFalse(blocked.ok)
        enqueued = module.try_enqueue(
            module.TryEnqueueRequest(resource="http://x:80", capacity=5, scope="example.com")
        )
        self.assertTrue(enqueued.ok)

        state_ok = module.state("r3", "example.com")
        self.assertEqual(state_ok.state, "AVAILABLE")
        with self.assertRaises(HTTPException) as ctx:
            module.state("missing", "example.com")
        self.assertEqual(ctx.exception.status_code, 404)


if __name__ == "__main__":
    unittest.main()
