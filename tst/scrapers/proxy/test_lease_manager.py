import sys
import types
import unittest
from unittest.mock import Mock, patch

if "redis" not in sys.modules:
    redis_stub = types.ModuleType("redis")
    redis_stub.Redis = object
    sys.modules["redis"] = redis_stub

if "curl_cffi" not in sys.modules:
    curl_mod = types.ModuleType("curl_cffi")
    curl_requests = types.SimpleNamespace(get=lambda *a, **k: None, request=lambda *a, **k: None)
    curl_mod.requests = curl_requests
    sys.modules["curl_cffi"] = curl_mod

try:
    import lxml.html  # noqa: F401
except Exception:
    if "lxml" not in sys.modules:
        lxml_mod = types.ModuleType("lxml")
        lxml_html_mod = types.ModuleType("lxml.html")
        lxml_html_mod.fromstring = lambda _html: None
        lxml_mod.html = lxml_html_mod
        sys.modules["lxml"] = lxml_mod
        sys.modules["lxml.html"] = lxml_html_mod

from scrapers.proxy.lease_manager import LeaseManager, LeaseState


class _FakeRedis:
    def __init__(self):
        self.scripts = []
        self.llen_value = 0
        self.scan_values = [(0, [])]

    def register_script(self, _script):
        fn = Mock()
        self.scripts.append(fn)
        return fn

    def llen(self, _key):
        return self.llen_value

    def scan(self, *, cursor, match, count):
        del match, count
        if not self.scan_values:
            return 0, []
        return self.scan_values.pop(0)


class LeaseManagerTest(unittest.TestCase):
    TEST_SCOPE = "test.scope"

    def _build(self):
        fake_redis = _FakeRedis()
        manager = LeaseManager(
            redis_client=fake_redis,
            lease_ttl_seconds=10,
            blocked_ttl_seconds=30,
            max_attempts=5,
        )
        return manager, fake_redis

    @patch("scrapers.proxy.lease_manager.uuid.uuid4")
    def test_lease_success_and_none(self, mock_uuid4: Mock):
        manager, fake_redis = self._build()
        mock_uuid4.return_value.hex = "tok"

        fake_redis.scripts[0].return_value = [b"http://1.2.3.4:80", b"tok"]
        self.assertEqual(manager.lease(scope=self.TEST_SCOPE), ("http://1.2.3.4:80", "tok"))
        fake_redis.scripts[0].return_value = None
        self.assertIsNone(manager.lease(scope=self.TEST_SCOPE))

    def test_release_block_try_enqueue(self):
        manager, fake_redis = self._build()
        fake_redis.scripts[1].return_value = 1
        fake_redis.scripts[2].return_value = 1
        fake_redis.scripts[3].return_value = 1
        self.assertTrue(manager.release("r", "t", scope=self.TEST_SCOPE))
        self.assertTrue(manager.block("r", "t", scope=self.TEST_SCOPE))
        self.assertTrue(manager.try_enqueue("r", 100, scope=self.TEST_SCOPE))
        fake_redis.scripts[1].return_value = 0
        self.assertFalse(manager.release("r", "t", scope=self.TEST_SCOPE))

    def test_try_enqueue_reason_codes(self):
        manager, fake_redis = self._build()
        fake_redis.scripts[3].return_value = 1
        self.assertEqual(
            manager.try_enqueue_with_reason("r", 100, scope=self.TEST_SCOPE),
            (True, LeaseManager.ENQUEUE_REASON_ENQUEUED),
        )
        fake_redis.scripts[3].return_value = 0
        self.assertEqual(
            manager.try_enqueue_with_reason("r", 100, scope=self.TEST_SCOPE),
            (False, LeaseManager.ENQUEUE_REASON_CAPACITY),
        )
        fake_redis.scripts[3].return_value = -1
        self.assertEqual(
            manager.try_enqueue_with_reason("r", 100, scope=self.TEST_SCOPE),
            (False, LeaseManager.ENQUEUE_REASON_BLOCKED),
        )
        fake_redis.scripts[3].return_value = -2
        self.assertEqual(
            manager.try_enqueue_with_reason("r", 100, scope=self.TEST_SCOPE),
            (False, LeaseManager.ENQUEUE_REASON_INUSE),
        )
        fake_redis.scripts[3].return_value = -3
        self.assertEqual(
            manager.try_enqueue_with_reason("r", 100, scope=self.TEST_SCOPE),
            (False, LeaseManager.ENQUEUE_REASON_DUPLICATE),
        )
        fake_redis.scripts[3].return_value = -4
        self.assertEqual(
            manager.try_enqueue_with_reason("r", 100, scope=self.TEST_SCOPE),
            (False, LeaseManager.ENQUEUE_REASON_INVALID_CAPACITY),
        )
        fake_redis.scripts[3].return_value = 123
        self.assertEqual(
            manager.try_enqueue_with_reason("r", 100, scope=self.TEST_SCOPE),
            (False, LeaseManager.ENQUEUE_REASON_UNKNOWN),
        )

    def test_get_state(self):
        manager, fake_redis = self._build()
        fake_redis.scripts[4].return_value = 2
        self.assertEqual(manager.get_state("r", scope=self.TEST_SCOPE), LeaseState.INUSE)
        fake_redis.scripts[4].return_value = 1
        self.assertEqual(manager.get_state("r", scope=self.TEST_SCOPE), LeaseState.BLOCKED)
        fake_redis.scripts[4].return_value = -1
        self.assertEqual(manager.get_state("r", scope=self.TEST_SCOPE), LeaseState.MISSING)
        fake_redis.scripts[4].return_value = 0
        self.assertEqual(manager.get_state("r", scope=self.TEST_SCOPE), LeaseState.AVAILABLE)

    def test_sizes(self):
        manager, fake_redis = self._build()
        fake_redis.llen_value = 7
        fake_redis.scan_values = [
            (1, [b"a", b"b"]),
            (0, [b"c"]),
            (0, [b"d", b"e"]),
        ]
        sizes = manager.sizes(scope=self.TEST_SCOPE)
        self.assertEqual(sizes["available"], 7)
        self.assertEqual(sizes["inuse"], 3)
        self.assertEqual(sizes["blocked"], 2)

    def test_scope_normalization_and_default_key_branches(self):
        self.assertEqual(LeaseManager.normalize_scope("  DeFaUlt "), "default")
        with self.assertRaises(ValueError):
            LeaseManager.normalize_scope("   ")

        self.assertEqual(LeaseManager.available_key_for_scope("default"), LeaseManager.AVAILABLE_KEY)
        self.assertEqual(LeaseManager.available_set_key_for_scope("default"), LeaseManager.AVAILABLE_SET_KEY)
        self.assertEqual(LeaseManager.inuse_prefix_for_scope("default"), f"{LeaseManager.INUSE_PREFIX}:")
        self.assertEqual(LeaseManager.blocked_prefix_for_scope("default"), f"{LeaseManager.BLOCKED_PREFIX}:")


if __name__ == "__main__":
    unittest.main()
