import unittest
from unittest.mock import Mock, patch

from scrapers.airflow.clients.common.request_policy import RequestPolicy
from scrapers.airflow.clients.google.transport import GoogleTransport


class GoogleTransportTest(unittest.TestCase):
    @patch("scrapers.airflow.clients.google.transport.request_text_with_backoff", return_value="<html/>")
    def test_get_html(self, _mock_request: Mock) -> None:
        transport = GoogleTransport(base_url="https://google.com/", proxy_management_client=Mock())
        out = transport.get_html(
            path="/about/careers",
            params=[],
            request_policy=RequestPolicy(timeout_seconds=1.0, max_retries=1),
        )
        self.assertEqual(out, "<html/>")


if __name__ == "__main__":
    unittest.main()
