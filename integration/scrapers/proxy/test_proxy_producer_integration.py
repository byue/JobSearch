import os
import unittest
import warnings
from unittest.mock import patch

from redis import Redis
from testcontainers.redis import RedisContainer

from scrapers.proxy import proxy_producer
from scrapers.proxy.lease_manager import LeaseManager

warnings.filterwarnings("ignore", message=r"unclosed <socket\.socket.*", category=ResourceWarning)


class _FakeProxyGeneratorClient:
    def __init__(self, validate_timeout_seconds: float, list_fetch_timeout_seconds: float) -> None:
        self.validate_timeout_seconds = validate_timeout_seconds
        self.list_fetch_timeout_seconds = list_fetch_timeout_seconds
        self._candidates = [
            "http://10.0.0.1:8080",
            "http://10.0.0.2:8080",
            "http://10.0.0.3:8080",
        ]
        self._valid = {
            "http://10.0.0.1:8080",
            "http://10.0.0.2:8080",
        }

    def get_proxy_urls(self) -> list[str]:
        return list(self._candidates)

    def is_proxy_valid(self, proxy_url: str) -> bool:
        return proxy_url in self._valid


class ProxyProducerIntegrationTest(unittest.TestCase):
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

    @classmethod
    def tearDownClass(cls) -> None:
        cls.redis.close()
        cls._redis_container.stop()

    def setUp(self) -> None:
        self.redis.flushdb()
        proxy_producer._STOP = False

    def tearDown(self) -> None:
        proxy_producer._STOP = False

    def _run_producer_once(self, extra_env: dict[str, str] | None = None) -> int:
        env = {
            "JOBSEARCH_PROXY_REDIS_URL": self.redis_url,
            "JOBSEARCH_PROXY_SCOPES": "default",
            "JOBSEARCH_PROXY_QUEUE_MAX_SIZE": "1",
            "JOBSEARCH_PROXY_VALIDATE_TIMEOUT_SECONDS": "0.5",
            "JOBSEARCH_PROXY_LIST_FETCH_TIMEOUT_SECONDS": "0.5",
            "JOBSEARCH_PROXY_VALIDATE_WORKERS": "1",
            "JOBSEARCH_PROXY_PRODUCER_SLEEP_SECONDS": "0.01",
            "JOBSEARCH_PROXY_PRODUCER_HEARTBEAT_SECONDS": "0.01",
            "JOBSEARCH_PROXY_LEASE_TTL_SECONDS": "1",
            "JOBSEARCH_PROXY_LEASE_MAX_ATTEMPTS": "20",
            "JOBSEARCH_PROXY_BLOCKED_COOLDOWN_SECONDS": "1",
        }
        if extra_env:
            env.update(extra_env)

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

    def test_producer_enqueues_only_valid_urls_with_capacity_limit(self) -> None:
        result = self._run_producer_once()
        self.assertEqual(result, 0)

        available_size = self.redis.llen(LeaseManager.AVAILABLE_KEY)
        self.assertEqual(available_size, 1)
        queued = self.redis.lrange(LeaseManager.AVAILABLE_KEY, 0, -1)
        queued_values = [item.decode("utf-8") for item in queued]
        self.assertIn(queued_values[0], {"http://10.0.0.1:8080", "http://10.0.0.2:8080"})

    def test_producer_does_not_enqueue_blocked_resource(self) -> None:
        blocked_prefix = LeaseManager.blocked_prefix_for_scope("default")
        self.redis.setex(f"{blocked_prefix}http://10.0.0.1:8080", 60, "1")
        self.redis.setex(f"{blocked_prefix}http://10.0.0.2:8080", 60, "1")

        result = self._run_producer_once()
        self.assertEqual(result, 0)
        self.assertEqual(self.redis.llen(LeaseManager.AVAILABLE_KEY), 0)
