import sys
import types
import unittest
from unittest.mock import Mock, patch

import requests

if "redis" not in sys.modules:
    redis_stub = types.ModuleType("redis")
    redis_stub.Redis = object
    sys.modules["redis"] = redis_stub

if "curl_cffi" not in sys.modules:
    curl_mod = types.ModuleType("curl_cffi")
    curl_requests = types.SimpleNamespace(get=lambda *a, **k: None, request=lambda *a, **k: None)
    curl_mod.requests = curl_requests
    sys.modules["curl_cffi"] = curl_mod

try:
    import lxml.html  # noqa: F401
except Exception:
    if "lxml" not in sys.modules:
        lxml_mod = types.ModuleType("lxml")
        lxml_html_mod = types.ModuleType("lxml.html")
        lxml_html_mod.fromstring = lambda _html: None
        lxml_mod.html = lxml_html_mod
        sys.modules["lxml"] = lxml_mod
        sys.modules["lxml.html"] = lxml_html_mod

from scrapers.proxy.lease_manager import LeaseState
from scrapers.proxy.proxy_management_client import ProxyManagementClient


class _FakeResponse:
    def __init__(self, status_code=200, payload=None):
        self.status_code = status_code
        self._payload = payload

    def json(self):
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            error = requests.exceptions.HTTPError(f"HTTP {self.status_code}")
            error.response = self
            raise error


class ProxyManagementClientTest(unittest.TestCase):
    def test_init_validation(self) -> None:
        with self.assertRaises(ValueError):
            ProxyManagementClient(base_url="", timeout_seconds=1.0)
        with self.assertRaises(ValueError):
            ProxyManagementClient(base_url="http://x", timeout_seconds=0)
        with self.assertRaises(ValueError):
            ProxyManagementClient(base_url="http://x", timeout_seconds=1.0, lease_acquire_timeout_seconds=0)
        with self.assertRaises(ValueError):
            ProxyManagementClient(base_url="http://x", timeout_seconds=1.0, lease_poll_interval_seconds=0)

    def test_health_and_sizes(self) -> None:
        client = ProxyManagementClient(base_url="http://x", timeout_seconds=1.0)
        client._session = Mock()
        client._session.request.side_effect = [
            _FakeResponse(payload={"status": "ok"}),
            _FakeResponse(payload={"available": 2, "inuse": 1, "blocked": 4}),
        ]
        self.assertTrue(client.health())
        self.assertEqual(client.sizes(scope="example.com"), {"available": 2, "inuse": 1, "blocked": 4})

    def test_sizes_validates_payload(self) -> None:
        client = ProxyManagementClient(base_url="http://x", timeout_seconds=1.0)
        client._session = Mock()
        client._session.request.return_value = _FakeResponse(payload=["bad"])
        with self.assertRaises(ValueError):
            client.sizes(scope="example.com")

    def test_lease_paths(self) -> None:
        client = ProxyManagementClient(base_url="http://x", timeout_seconds=1.0)
        client._session = Mock()

        client._session.request.return_value = _FakeResponse(payload=None)
        self.assertIsNone(client.lease(scope="example.com"))

        client._session.request.return_value = _FakeResponse(payload="bad")
        with self.assertRaises(ValueError):
            client.lease(scope="example.com")

        client._session.request.return_value = _FakeResponse(payload={"resource": "http://1.2.3.4:80", "token": "t"})
        self.assertEqual(client.lease(scope="example.com"), ("http://1.2.3.4:80", "t"))

    def test_release_block_and_try_enqueue_payload_handling(self) -> None:
        client = ProxyManagementClient(base_url="http://x", timeout_seconds=1.0)
        client._session = Mock()

        client._session.request.return_value = _FakeResponse(payload="bad")
        self.assertFalse(client.release("r", "t", scope="example.com"))
        self.assertFalse(client.block("r", "t", scope="example.com"))
        self.assertFalse(client.try_enqueue("r", 10, scope="example.com"))

        client._session.request.return_value = _FakeResponse(payload={"ok": True})
        self.assertTrue(client.release("r", "t", scope="example.com"))
        self.assertTrue(client.block("r", "t", scope="example.com"))
        self.assertTrue(client.try_enqueue("r", 10, scope="example.com"))

    def test_get_state_handles_missing(self) -> None:
        client = ProxyManagementClient(base_url="http://x", timeout_seconds=1.0)
        client._session = Mock()
        client._session.request.return_value = _FakeResponse(status_code=404, payload={"detail": "not found"})
        self.assertEqual(client.get_state("r", scope="example.com"), LeaseState.MISSING)

    def test_get_state_validates_payload(self) -> None:
        client = ProxyManagementClient(base_url="http://x", timeout_seconds=1.0)
        client._session = Mock()
        client._session.request.return_value = _FakeResponse(payload="bad")
        with self.assertRaises(ValueError):
            client.get_state("r", scope="example.com")
        client._session.request.return_value = _FakeResponse(payload={"state": "UNKNOWN"})
        with self.assertRaises(ValueError):
            client.get_state("r", scope="example.com")

    def test_get_state_valid_enum(self) -> None:
        client = ProxyManagementClient(base_url="http://x", timeout_seconds=1.0)
        client._session = Mock()
        client._session.request.return_value = _FakeResponse(payload={"state": "AVAILABLE"})
        self.assertEqual(client.get_state("r", scope="example.com"), LeaseState.AVAILABLE)

    def test_get_state_unknown_state(self) -> None:
        client = ProxyManagementClient(base_url="http://x", timeout_seconds=1.0)
        client._session = Mock()
        client._session.request.return_value = _FakeResponse(payload={"state": "UNKNOWN"})
        with self.assertRaises(ValueError):
            client.get_state("r", scope="example.com")

    @patch("scrapers.proxy.proxy_management_client.time.sleep")
    @patch("scrapers.proxy.proxy_management_client.time.monotonic")
    def test_acquire_requests_proxy(self, mock_monotonic: Mock, _mock_sleep: Mock) -> None:
        client = ProxyManagementClient(
            base_url="http://x",
            timeout_seconds=1.0,
            lease_acquire_timeout_seconds=1.0,
            lease_poll_interval_seconds=0.01,
        )
        client.lease = Mock(side_effect=[None, ("http://1.2.3.4:9999", "tok")])
        mock_monotonic.side_effect = [0.0, 0.1, 0.2]
        proxies, resource, token = client.acquire_requests_proxy(scope="example.com")
        self.assertEqual(resource, "http://1.2.3.4:9999")
        self.assertEqual(token, "tok")
        self.assertEqual(proxies["http"], resource)
        self.assertEqual(proxies["https"], resource)

    @patch("scrapers.proxy.proxy_management_client.time.sleep")
    @patch("scrapers.proxy.proxy_management_client.time.monotonic")
    def test_acquire_requests_proxy_timeout(self, mock_monotonic: Mock, _mock_sleep: Mock) -> None:
        client = ProxyManagementClient(
            base_url="http://x",
            timeout_seconds=1.0,
            lease_acquire_timeout_seconds=0.2,
            lease_poll_interval_seconds=0.01,
        )
        client.lease = Mock(return_value=None)
        mock_monotonic.side_effect = [0.0, 0.1, 0.3]
        with self.assertRaises(requests.exceptions.ProxyError):
            client.acquire_requests_proxy(scope="example.com")

    def test_complete_requests_proxy(self) -> None:
        client = ProxyManagementClient(base_url="http://x", timeout_seconds=1.0)
        client.release = Mock(return_value=True)
        client.block = Mock(return_value=True)
        self.assertTrue(client.complete_requests_proxy(resource="r", token="t", success=True, scope="example.com"))
        self.assertTrue(client.complete_requests_proxy(resource="r", token="t", success=False, scope="example.com"))
        client.release.assert_called_once_with("r", "t", scope="example.com")
        client.block.assert_called_once_with("r", "t", scope="example.com")

    def test_scope_validation(self) -> None:
        client = ProxyManagementClient(base_url="http://x", timeout_seconds=1.0)
        with self.assertRaises(ValueError):
            client.sizes(scope="")


if __name__ == "__main__":
    unittest.main()
