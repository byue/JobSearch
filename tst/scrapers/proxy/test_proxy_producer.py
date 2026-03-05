import os
import runpy
import unittest
from unittest.mock import Mock, patch

from scrapers.proxy import proxy_producer


class ProxyProducerTest(unittest.TestCase):
    def setUp(self) -> None:
        proxy_producer._STOP = False

    def tearDown(self) -> None:
        proxy_producer._STOP = False

    def test_handle_signal_sets_stop(self) -> None:
        self.assertFalse(proxy_producer._STOP)
        proxy_producer._handle_signal(2, object())
        self.assertTrue(proxy_producer._STOP)

    def test_main_runs_one_iteration_with_candidate_outcomes(self) -> None:
        future_error = Mock()
        future_error.result.side_effect = RuntimeError("bad-proxy")
        future_invalid = Mock()
        future_invalid.result.return_value = False
        future_rejected = Mock()
        future_rejected.result.return_value = True
        future_accepted = Mock()
        future_accepted.result.return_value = True

        proxy_urls = [
            "http://10.0.0.1:80",
            "http://10.0.0.2:80",
            "http://10.0.0.3:80",
            "http://10.0.0.4:80",
        ]

        with patch("scrapers.proxy.proxy_producer.os.getenv") as mock_getenv, patch(
            "scrapers.proxy.proxy_producer.require_env"
        ) as mock_require_env, patch(
            "scrapers.proxy.proxy_producer.env_int"
        ) as mock_env_int, patch("scrapers.proxy.proxy_producer.env_float") as mock_env_float, patch(
            "scrapers.proxy.proxy_producer.require_env_int"
        ) as mock_require_env_int, patch("scrapers.proxy.proxy_producer.signal.signal"), patch(
            "scrapers.proxy.proxy_producer.Redis"
        ) as mock_redis_cls, patch(
            "scrapers.proxy.proxy_producer.LeaseManager"
        ) as mock_lease_manager_cls, patch(
            "scrapers.proxy.proxy_producer.ProxyGeneratorClient"
        ) as mock_proxy_gen_cls, patch(
            "scrapers.proxy.proxy_producer.ThreadPoolExecutor"
        ) as mock_executor_cls, patch(
            "scrapers.proxy.proxy_producer.as_completed",
            return_value=[future_error, future_invalid, future_rejected, future_accepted],
        ), patch(
            "scrapers.proxy.proxy_producer.time.monotonic",
            return_value=20.0,
        ), patch("scrapers.proxy.proxy_producer.time.sleep") as mock_sleep, patch(
            "scrapers.proxy.proxy_producer.LOGGER"
        ) as mock_logger:
            mock_getenv.side_effect = lambda key, default=None: {
                "JOBSEARCH_PROXY_BLOCKED_COOLDOWN_SECONDS": "60",
                "JOBSEARCH_PROXY_SCOPES": "default",
            }.get(key, default)
            mock_require_env.side_effect = lambda key: {
                "JOBSEARCH_PROXY_REDIS_URL": "redis://unit-test:6379/0",
                "JOBSEARCH_AIRFLOW_COMPANIES": "amazon,apple,google,meta,microsoft,netflix",
            }[key]
            mock_env_int.side_effect = [128, 32]
            mock_env_float.side_effect = [2.5, 15.0, 1.0, 15.0]
            mock_require_env_int.side_effect = [300, 50, 60]
            mock_redis_cls.from_url.return_value = Mock()

            lease_manager = Mock()
            lease_manager.sizes.return_value = {"available": 10, "inuse": 1, "blocked": 2}
            lease_manager.try_enqueue_with_reason.side_effect = [
                (False, "capacity"),
                (True, "enqueued"),
            ]
            mock_lease_manager_cls.return_value = lease_manager

            proxy_generator = Mock()
            proxy_generator.get_proxy_urls.return_value = list(proxy_urls)
            mock_proxy_gen_cls.return_value = proxy_generator

            executor = Mock()
            executor.submit.side_effect = [future_error, future_invalid, future_rejected, future_accepted]
            mock_executor_cls.return_value.__enter__.return_value = executor

            def _stop_after_sleep(_seconds: float) -> None:
                proxy_producer._STOP = True

            mock_sleep.side_effect = _stop_after_sleep

            result = proxy_producer.main()

            self.assertEqual(result, 0)
            self.assertEqual(executor.submit.call_count, 4)
            enqueue_args = [call.args for call in lease_manager.try_enqueue_with_reason.call_args_list]
            self.assertIn(("http://10.0.0.3:80", 128), enqueue_args)
            self.assertIn(("http://10.0.0.4:80", 128), enqueue_args)
            mock_logger.info.assert_any_call(
                (
                    "proxy_producer_heartbeat queue_size=%s inuse_count=%s blocked_count=%s "
                    "loops=%s fetched=%s accepted=%s invalid=%s "
                    "validation_errors=%s enqueue_rejected=%s "
                    "rejected_capacity=%s rejected_blocked=%s rejected_inuse=%s "
                    "rejected_duplicate=%s rejected_invalid_capacity=%s rejected_unknown=%s"
                ),
                10,
                1,
                2,
                1,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
            )
            mock_logger.info.assert_any_call("proxy producer exiting")

    def test_main_logs_loop_error_and_uses_deny_cooldown_fallback(self) -> None:
        with patch("scrapers.proxy.proxy_producer.os.getenv") as mock_getenv, patch(
            "scrapers.proxy.proxy_producer.require_env"
        ) as mock_require_env, patch(
            "scrapers.proxy.proxy_producer.env_int"
        ) as mock_env_int, patch("scrapers.proxy.proxy_producer.env_float") as mock_env_float, patch(
            "scrapers.proxy.proxy_producer.require_env_int"
        ) as mock_require_env_int, patch("scrapers.proxy.proxy_producer.signal.signal"), patch(
            "scrapers.proxy.proxy_producer.Redis"
        ) as mock_redis_cls, patch("scrapers.proxy.proxy_producer.LeaseManager") as mock_lease_manager_cls, patch(
            "scrapers.proxy.proxy_producer.ProxyGeneratorClient"
        ) as mock_proxy_gen_cls, patch(
            "scrapers.proxy.proxy_producer.time.monotonic",
            return_value=20.0,
        ), patch("scrapers.proxy.proxy_producer.time.sleep") as mock_sleep, patch(
            "scrapers.proxy.proxy_producer.LOGGER"
        ) as mock_logger:
            mock_getenv.side_effect = lambda key, default=None: {
                "JOBSEARCH_PROXY_BLOCKED_COOLDOWN_SECONDS": None,
                "JOBSEARCH_PROXY_DENY_COOLDOWN_SECONDS": "99",
                "JOBSEARCH_PROXY_SCOPES": "default",
            }.get(key, default)
            mock_require_env.side_effect = lambda key: {
                "JOBSEARCH_PROXY_REDIS_URL": "redis://unit-test:6379/0",
                "JOBSEARCH_AIRFLOW_COMPANIES": "amazon,apple,google,meta,microsoft,netflix",
            }[key]
            mock_env_int.side_effect = [32, 8]
            mock_env_float.side_effect = [1.0, 2.0, 0.2, 15.0]
            mock_require_env_int.side_effect = [300, 50, 99]
            mock_redis_cls.from_url.return_value = Mock()

            lease_manager = Mock()
            lease_manager.sizes.return_value = {"available": 0, "inuse": 0, "blocked": 0}
            mock_lease_manager_cls.return_value = lease_manager

            proxy_generator = Mock()
            proxy_generator.get_proxy_urls.side_effect = RuntimeError("boom")
            mock_proxy_gen_cls.return_value = proxy_generator

            def _stop_after_sleep(_seconds: float) -> None:
                proxy_producer._STOP = True

            mock_sleep.side_effect = _stop_after_sleep

            result = proxy_producer.main()

            self.assertEqual(result, 0)
            mock_logger.error.assert_called_once()
            _, kwargs = mock_lease_manager_cls.call_args
            self.assertEqual(kwargs["blocked_ttl_seconds"], 99)

    def test_main_module_entrypoint_executes(self) -> None:
        env = {
            "JOBSEARCH_PROXY_REDIS_URL": "redis://unit-test:6379/0",
            "JOBSEARCH_AIRFLOW_COMPANIES": "amazon,apple,google,meta,microsoft,netflix",
            "JOBSEARCH_PROXY_LEASE_TTL_SECONDS": "10",
            "JOBSEARCH_PROXY_LEASE_MAX_ATTEMPTS": "10",
            "JOBSEARCH_PROXY_DENY_COOLDOWN_SECONDS": "10",
        }
        with patch.dict(os.environ, env, clear=False), patch("signal.signal"), patch("redis.Redis") as mock_redis_cls, patch(
            "scrapers.proxy.lease_manager.LeaseManager"
        ) as mock_lease_manager_cls, patch(
            "scrapers.proxy.proxy_generator_client.ProxyGeneratorClient"
        ) as mock_proxy_gen_cls, patch("time.sleep", side_effect=SystemExit(0)):
            mock_redis_cls.from_url.return_value = Mock()
            mock_lease_manager = Mock()
            mock_lease_manager.sizes.return_value = {"available": 0, "inuse": 0, "blocked": 0}
            mock_lease_manager_cls.return_value = mock_lease_manager
            mock_proxy_gen = Mock()
            mock_proxy_gen.get_proxy_urls.return_value = []
            mock_proxy_gen_cls.return_value = mock_proxy_gen
            with self.assertRaises(SystemExit):
                runpy.run_module("scrapers.proxy.proxy_producer", run_name="__main__")

    def test_main_raises_when_scopes_empty_after_parsing(self) -> None:
        with patch("scrapers.proxy.proxy_producer.os.getenv") as mock_getenv, patch(
            "scrapers.proxy.proxy_producer.require_env"
        ) as mock_require_env, patch(
            "scrapers.proxy.proxy_producer.env_int"
        ) as mock_env_int, patch("scrapers.proxy.proxy_producer.env_float") as mock_env_float, patch(
            "scrapers.proxy.proxy_producer.require_env_int"
        ) as mock_require_env_int:
            mock_getenv.side_effect = lambda key, default=None: {
                "JOBSEARCH_PROXY_SCOPES": " , , ",
                "JOBSEARCH_PROXY_BLOCKED_COOLDOWN_SECONDS": "60",
            }.get(key, default)
            mock_require_env.side_effect = lambda key: {
                "JOBSEARCH_PROXY_REDIS_URL": "redis://unit-test:6379/0",
                "JOBSEARCH_AIRFLOW_COMPANIES": "amazon,apple,google,meta,microsoft,netflix",
            }[key]
            mock_env_int.side_effect = [128, 32]
            mock_env_float.side_effect = [2.5, 15.0, 1.0, 15.0]
            mock_require_env_int.side_effect = [300, 50, 60]
            with self.assertRaises(ValueError):
                proxy_producer.main()

    def test_main_tracks_all_enqueue_rejection_reason_branches(self) -> None:
        future_valid = Mock()
        future_valid.result.return_value = True
        proxy_urls = ["http://10.0.0.9:80"]
        scopes = "s1,s2,s3,s4,s5,s6"

        with patch("scrapers.proxy.proxy_producer.os.getenv") as mock_getenv, patch(
            "scrapers.proxy.proxy_producer.require_env"
        ) as mock_require_env, patch(
            "scrapers.proxy.proxy_producer.env_int"
        ) as mock_env_int, patch("scrapers.proxy.proxy_producer.env_float") as mock_env_float, patch(
            "scrapers.proxy.proxy_producer.require_env_int"
        ) as mock_require_env_int, patch("scrapers.proxy.proxy_producer.signal.signal"), patch(
            "scrapers.proxy.proxy_producer.Redis"
        ) as mock_redis_cls, patch(
            "scrapers.proxy.proxy_producer.LeaseManager"
        ) as mock_lease_manager_cls, patch(
            "scrapers.proxy.proxy_producer.ProxyGeneratorClient"
        ) as mock_proxy_gen_cls, patch(
            "scrapers.proxy.proxy_producer.ThreadPoolExecutor"
        ) as mock_executor_cls, patch(
            "scrapers.proxy.proxy_producer.as_completed",
            return_value=[future_valid],
        ), patch(
            "scrapers.proxy.proxy_producer.time.monotonic",
            return_value=20.0,
        ), patch("scrapers.proxy.proxy_producer.time.sleep") as mock_sleep:
            mock_getenv.side_effect = lambda key, default=None: {
                "JOBSEARCH_PROXY_BLOCKED_COOLDOWN_SECONDS": "60",
                "JOBSEARCH_PROXY_SCOPES": scopes,
            }.get(key, default)
            mock_require_env.side_effect = lambda key: {
                "JOBSEARCH_PROXY_REDIS_URL": "redis://unit-test:6379/0",
                "JOBSEARCH_AIRFLOW_COMPANIES": "amazon,apple,google,meta,microsoft,netflix",
            }[key]
            mock_env_int.side_effect = [128, 1]
            mock_env_float.side_effect = [2.5, 15.0, 1.0, 15.0]
            mock_require_env_int.side_effect = [300, 50, 60]
            mock_redis_cls.from_url.return_value = Mock()
            mock_lease_manager_cls.normalize_scope.side_effect = lambda value: value.strip().lower()
            mock_lease_manager_cls.ENQUEUE_REASON_CAPACITY = "capacity"
            mock_lease_manager_cls.ENQUEUE_REASON_BLOCKED = "blocked"
            mock_lease_manager_cls.ENQUEUE_REASON_INUSE = "inuse"
            mock_lease_manager_cls.ENQUEUE_REASON_DUPLICATE = "duplicate"
            mock_lease_manager_cls.ENQUEUE_REASON_INVALID_CAPACITY = "invalid_capacity"
            mock_lease_manager_cls.ENQUEUE_REASON_UNKNOWN = "unknown"

            lease_manager = Mock()
            lease_manager.sizes.return_value = {"available": 0, "inuse": 0, "blocked": 0}
            lease_manager.try_enqueue_with_reason.side_effect = [
                (False, "capacity"),
                (False, "blocked"),
                (False, "inuse"),
                (False, "duplicate"),
                (False, "invalid_capacity"),
                (False, "unknown"),
            ]
            mock_lease_manager_cls.return_value = lease_manager

            proxy_generator = Mock()
            proxy_generator.get_proxy_urls.return_value = list(proxy_urls)
            mock_proxy_gen_cls.return_value = proxy_generator

            executor = Mock()
            executor.submit.side_effect = [future_valid]
            mock_executor_cls.return_value.__enter__.return_value = executor

            def _stop_after_sleep(_seconds: float) -> None:
                proxy_producer._STOP = True

            mock_sleep.side_effect = _stop_after_sleep

            result = proxy_producer.main()
            self.assertEqual(result, 0)
            self.assertEqual(lease_manager.try_enqueue_with_reason.call_count, 6)


if __name__ == "__main__":
    unittest.main()
