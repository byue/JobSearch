import unittest

import scrapers.airflow.clients as clients


class ClientsInitTest(unittest.TestCase):
    def test_lazy_exports_resolve_expected_classes(self) -> None:
        self.assertEqual(clients.AmazonJobsClient.__name__, "AmazonJobsClient")
        self.assertEqual(clients.AppleJobsClient.__name__, "AppleJobsClient")
        self.assertEqual(clients.GoogleJobsClient.__name__, "GoogleJobsClient")
        self.assertEqual(clients.MetaJobsClient.__name__, "MetaJobsClient")
        self.assertEqual(clients.MicrosoftJobsClient.__name__, "MicrosoftJobsClient")
        self.assertEqual(clients.NetflixJobsClient.__name__, "NetflixJobsClient")

    def test_unknown_export_raises_attribute_error(self) -> None:
        with self.assertRaises(AttributeError):
            getattr(clients, "NopeClient")


if __name__ == "__main__":
    unittest.main()
