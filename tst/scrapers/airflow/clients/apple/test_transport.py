import unittest
from unittest.mock import Mock, patch

from scrapers.airflow.clients.apple.transport import AppleTransport
from common.request_policy import RequestPolicy


class AppleTransportTest(unittest.TestCase):
    @patch("scrapers.airflow.clients.apple.transport.request_text_with_backoff", return_value="<html/>")
    def test_get_html(self, _mock_request: Mock) -> None:
        transport = AppleTransport(base_url="https://jobs.apple.com/", proxy_management_client=Mock())
        out = transport.get_html(
            path="/en-us/search",
            params=[("page", "1")],
            request_policy=RequestPolicy(timeout_seconds=1.0, max_retries=1),
        )
        self.assertEqual(out, "<html/>")

    @patch("scrapers.airflow.clients.apple.transport.request_json_with_backoff", return_value={"res": {"id": "1"}})
    def test_get_json(self, _mock_request: Mock) -> None:
        transport = AppleTransport(base_url="https://jobs.apple.com/", proxy_management_client=Mock())
        out = transport.get_json(
            path="/api/v1/jobDetails/1",
            params=[],
            request_policy=RequestPolicy(timeout_seconds=1.0, max_retries=1),
        )
        self.assertEqual(out["res"]["id"], "1")

    @patch("scrapers.airflow.clients.apple.transport.request_json_with_backoff", return_value=["bad"])
    def test_get_json_requires_mapping(self, _mock_request: Mock) -> None:
        transport = AppleTransport(base_url="https://jobs.apple.com/", proxy_management_client=Mock())
        with self.assertRaises(ValueError):
            transport.get_json(
                path="/api/v1/jobDetails/1",
                params=[],
                request_policy=RequestPolicy(timeout_seconds=1.0, max_retries=1),
            )


if __name__ == "__main__":
    unittest.main()
