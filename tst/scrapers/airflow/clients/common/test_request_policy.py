import unittest

from common.request_policy import RequestPolicy
from scrapers.airflow.clients.common.request_policy import RequestPolicy as ClientRequestPolicy


class RequestPolicyTest(unittest.TestCase):
    def test_client_wrapper_reexports_common_request_policy(self) -> None:
        self.assertIs(ClientRequestPolicy, RequestPolicy)

    def test_defaults(self) -> None:
        policy = RequestPolicy(timeout_seconds=3.0, max_retries=4)
        self.assertEqual(policy.timeout_seconds, 3.0)
        self.assertEqual(policy.max_retries, 4)
        self.assertIsNone(policy.connect_timeout_seconds)
        self.assertEqual(policy.backoff_factor, 0.5)
        self.assertEqual(policy.max_backoff_seconds, 6.0)
        self.assertFalse(policy.jitter)
        self.assertEqual(policy.timeout_for_http(), 3.0)

    def test_connect_and_request_timeout_tuple(self) -> None:
        policy = RequestPolicy(timeout_seconds=20.0, connect_timeout_seconds=5.0, max_retries=2)
        self.assertEqual(policy.timeout_for_http(), (5.0, 20.0))


if __name__ == "__main__":
    unittest.main()
