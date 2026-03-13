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


if __name__ == "__main__":
    unittest.main()
