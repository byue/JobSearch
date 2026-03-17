import unittest

import scrapers.airflow.clients.common as common_clients
from common.request_policy import RequestPolicy as CommonRequestPolicy


class CommonClientsInitTest(unittest.TestCase):
    def test_lazy_exports_resolve_expected_objects(self) -> None:
        self.assertEqual(common_clients.JobsClient.__name__, "JobsClient")
        self.assertEqual(common_clients.RetryableUpstreamError.__name__, "RetryableUpstreamError")
        self.assertIs(common_clients.RequestPolicy, CommonRequestPolicy)
        self.assertEqual(common_clients.get_normalized_job_level("Senior Engineer"), "senior")

    def test_unknown_export_raises_attribute_error(self) -> None:
        with self.assertRaises(AttributeError):
            getattr(common_clients, "Nope")


if __name__ == "__main__":
    unittest.main()
