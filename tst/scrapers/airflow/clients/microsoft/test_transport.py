import unittest
from unittest.mock import Mock, patch

from common.request_policy import RequestPolicy
from scrapers.airflow.clients.microsoft.transport import MicrosoftTransport, require_mapping


class MicrosoftTransportTest(unittest.TestCase):
    def test_require_mapping(self) -> None:
        self.assertEqual(require_mapping({"a": 1}, context="x")["a"], 1)
        with self.assertRaises(ValueError):
            require_mapping([], context="x")

    @patch("scrapers.airflow.clients.microsoft.transport.request_json_with_backoff", return_value={"data": {}})
    def test_get_json(self, _mock_request: Mock) -> None:
        transport = MicrosoftTransport(base_url="https://microsoft.com/", proxy_management_client=Mock())
        out = transport.get_json(
            "/api",
            params=[],
            request_policy=RequestPolicy(timeout_seconds=1.0, max_retries=1),
        )
        self.assertEqual(out, {"data": {}})


if __name__ == "__main__":
    unittest.main()
