import sys
import types
import unittest
from unittest.mock import Mock, patch

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

from scrapers.proxy.proxy_generator_client import ProxyGeneratorClient


class _FakeResponse:
    def __init__(self, ok=True, text="", payload=None):
        self.ok = ok
        self.text = text
        self._payload = payload or {}

    def json(self):
        return self._payload

    def raise_for_status(self):
        if not self.ok:
            raise RuntimeError("bad status")


class ProxyGeneratorClientTest(unittest.TestCase):
    def _build_client(self) -> ProxyGeneratorClient:
        with patch.object(ProxyGeneratorClient, "_get_local_public_ip", return_value="1.1.1.1"):
            return ProxyGeneratorClient(validate_timeout_seconds=0.1, list_fetch_timeout_seconds=0.1)

    def test_normalize_proxy_url(self) -> None:
        client = self._build_client()
        self.assertIsNone(client._normalize_proxy_url(None))
        self.assertIsNone(client._normalize_proxy_url("   "))
        self.assertEqual(client._normalize_proxy_url("1.2.3.4:8080"), "http://1.2.3.4:8080")
        self.assertIsNone(client._normalize_proxy_url("bad"))

    def test_is_well_formed_proxy_url(self) -> None:
        client = self._build_client()
        self.assertTrue(client._is_well_formed_proxy_url("http://1.2.3.4:8080"))
        self.assertFalse(client._is_well_formed_proxy_url("http://example.com:8080"))
        self.assertFalse(client._is_well_formed_proxy_url("http://:8080"))
        self.assertFalse(client._is_well_formed_proxy_url("http://1.2.3.4"))
        self.assertFalse(client._is_well_formed_proxy_url("http://1.2.3.4:0"))
        with self.assertRaises(ValueError):
            client._is_well_formed_proxy_url("http://1.2.3.4:70000")
        self.assertFalse(client._is_well_formed_proxy_url("socks5://1.2.3.4:8080"))

    def test_extract_ip_from_payload(self) -> None:
        client = self._build_client()
        self.assertEqual(client._extract_ip_from_payload({"ip": "8.8.8.8"}), "8.8.8.8")
        self.assertIsNone(client._extract_ip_from_payload({"ip": 123}))
        self.assertIsNone(client._extract_ip_from_payload({"ip": "not-an-ip"}))

    @patch("scrapers.proxy.proxy_generator_client.curl_get")
    @patch("scrapers.proxy.proxy_generator_client.fromstring")
    def test_get_proxy_urls(self, mock_fromstring: Mock, mock_curl_get: Mock) -> None:
        html = """
        <table><tbody>
            <tr><td>1.1.1.1</td><td>80</td></tr>
            <tr><td>not_ip</td><td>80</td></tr>
            <tr><td>2.2.2.2</td><td>8080</td></tr>
        </tbody></table>
        """
        row1 = Mock()
        row1.xpath.side_effect = [["1.1.1.1"], ["80"]]
        row2 = Mock()
        row2.xpath.side_effect = [["not_ip"], ["80"]]
        row3 = Mock()
        row3.xpath.side_effect = [["2.2.2.2"], ["8080"]]
        parser = Mock()
        parser.xpath.return_value = [row1, row2, row3]
        mock_fromstring.return_value = parser
        mock_curl_get.return_value = _FakeResponse(ok=True, text=html)
        client = self._build_client()
        proxies = client.get_proxy_urls()
        self.assertIn("http://1.1.1.1:80", proxies)
        self.assertIn("http://2.2.2.2:8080", proxies)
        self.assertEqual(len(proxies), 2)

    @patch("scrapers.proxy.proxy_generator_client.curl_get")
    def test_is_proxy_valid(self, mock_curl_get: Mock) -> None:
        client = self._build_client()
        mock_curl_get.return_value = _FakeResponse(ok=True, payload={"ip": "2.2.2.2"})
        self.assertTrue(client.is_proxy_valid("http://2.2.2.2:80"))
        mock_curl_get.return_value = _FakeResponse(ok=True, payload={"ip": "1.1.1.1"})
        self.assertFalse(client.is_proxy_valid("http://1.1.1.1:80"))

    @patch("scrapers.proxy.proxy_generator_client.curl_get")
    def test_is_proxy_valid_error_paths(self, mock_curl_get: Mock) -> None:
        client = self._build_client()
        mock_curl_get.return_value = _FakeResponse(ok=False, payload={"ip": "2.2.2.2"})
        self.assertFalse(client.is_proxy_valid("http://2.2.2.2:80"))

        mock_curl_get.return_value = _FakeResponse(ok=True, payload={"ip": "bad"})
        self.assertFalse(client.is_proxy_valid("http://2.2.2.2:80"))

        client.local_public_ip = None
        mock_curl_get.return_value = _FakeResponse(ok=True, payload={"ip": "9.9.9.9"})
        self.assertFalse(client.is_proxy_valid("http://2.2.2.2:80"))

        mock_curl_get.side_effect = RuntimeError("network")
        self.assertFalse(client.is_proxy_valid("http://2.2.2.2:80"))

    @patch("scrapers.proxy.proxy_generator_client.curl_get")
    def test_get_local_public_ip_error_paths(self, mock_curl_get: Mock) -> None:
        client = self._build_client()
        mock_curl_get.return_value = _FakeResponse(ok=False, payload={"ip": "1.1.1.1"})
        self.assertIsNone(client._get_local_public_ip())

        mock_curl_get.return_value = _FakeResponse(ok=True, payload={"ip": "not-ip"})
        self.assertIsNone(client._get_local_public_ip())

        mock_curl_get.side_effect = RuntimeError("boom")
        self.assertIsNone(client._get_local_public_ip())

    def test_init_raises_when_local_ip_unavailable(self) -> None:
        with patch.object(ProxyGeneratorClient, "_get_local_public_ip", return_value=None):
            with self.assertRaises(RuntimeError):
                ProxyGeneratorClient(validate_timeout_seconds=0.1, list_fetch_timeout_seconds=0.1)


if __name__ == "__main__":
    unittest.main()
