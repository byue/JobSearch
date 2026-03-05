import unittest

from scrapers.airflow.clients.common.request_policy import DEFAULT_RETRYABLE_STATUS_CODES, RequestPolicy


class RequestPolicyTest(unittest.TestCase):
    def test_defaults(self) -> None:
        policy = RequestPolicy(timeout_seconds=3.0, max_retries=4)
        self.assertEqual(policy.timeout_seconds, 3.0)
        self.assertEqual(policy.max_retries, 4)
        self.assertEqual(policy.backoff_factor, 0.5)
        self.assertEqual(policy.max_backoff_seconds, 6.0)
        self.assertFalse(policy.jitter)
        self.assertEqual(policy.retryable_status_codes, DEFAULT_RETRYABLE_STATUS_CODES)

    def test_custom_retryable_status_codes(self) -> None:
        policy = RequestPolicy(timeout_seconds=1.0, max_retries=1, retryable_status_codes=frozenset({418}))
        self.assertEqual(policy.retryable_status_codes, frozenset({418}))


if __name__ == "__main__":
    unittest.main()
