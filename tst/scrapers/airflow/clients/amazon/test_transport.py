import unittest
from unittest.mock import Mock, patch

from scrapers.airflow.clients.amazon.transport import AmazonTransport, _require_mapping
from common.request_policy import RequestPolicy


class AmazonTransportTest(unittest.TestCase):
    def test_require_mapping(self) -> None:
        self.assertEqual(_require_mapping({"a": 1}, context="x")["a"], 1)
        with self.assertRaises(ValueError):
            _require_mapping([], context="x")

    @patch("scrapers.airflow.clients.amazon.transport.request_json_with_backoff")
    def test_get_json(self, mock_request_json: Mock) -> None:
        mock_request_json.return_value = {"jobs": []}
        transport = AmazonTransport(base_url="https://a/", proxy_management_client=Mock())
        out = transport.get_json(
            "/p",
            params=[("x", "1")],
            request_policy=RequestPolicy(timeout_seconds=1.0, max_retries=1),
        )
        self.assertEqual(out, {"jobs": []})
        mock_request_json.assert_called_once()

    @patch("scrapers.airflow.clients.amazon.transport.request_text_with_backoff")
    def test_get_text(self, mock_request_text: Mock) -> None:
        mock_request_text.return_value = "<html/>"
        transport = AmazonTransport(base_url="https://a/", proxy_management_client=Mock())
        out = transport.get_text(
            "/p",
            params=[("x", "1")],
            request_policy=RequestPolicy(timeout_seconds=1.0, max_retries=1),
        )
        self.assertEqual(out, "<html/>")
        mock_request_text.assert_called_once()


if __name__ == "__main__":
    unittest.main()
