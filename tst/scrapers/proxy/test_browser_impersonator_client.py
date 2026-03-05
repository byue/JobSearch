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

from scrapers.proxy import browser_impersonator_client as bic


class BrowserImpersonatorClientTest(unittest.TestCase):
    @patch("scrapers.proxy.browser_impersonator_client.random.choice", return_value="chrome136")
    def test_random_browser(self, mock_choice: Mock) -> None:
        self.assertEqual(bic.random_browser(), "chrome136")
        mock_choice.assert_called_once_with(bic.BROWSERS)

    def test_normalize_proxy_mapping(self) -> None:
        result = bic.normalize_proxy_mapping({"HTTP": "1.2.3.4:8080", "https": "https://a:1"})
        self.assertEqual(result, {"http": "http://1.2.3.4:8080", "https": "https://a:1"})

    def test_normalize_proxy_mapping_skips_empty_items(self) -> None:
        result = bic.normalize_proxy_mapping({"": "1.1.1.1:80", "http": "   "})
        self.assertIsNone(result)

    def test_select_proxy_url_prefers_https(self) -> None:
        proxy_url = bic.select_proxy_url({"http": "http://h:1", "https": "http://s:2"})
        self.assertEqual(proxy_url, "http://s:2")

    def test_browser_request_requires_proxy(self) -> None:
        with self.assertRaises(requests.exceptions.ProxyError):
            bic.browser_request(method="GET", url="http://x", timeout=1.0, require_proxy=True)

    @patch("scrapers.proxy.browser_impersonator_client.curl_request")
    def test_browser_request_wraps_transport_exception(self, mock_curl_request: Mock) -> None:
        mock_curl_request.side_effect = RuntimeError("boom")
        with self.assertRaises(requests.exceptions.RequestException):
            bic.browser_request(method="GET", url="http://x", timeout=1.0)

    @patch("scrapers.proxy.browser_impersonator_client.curl_request")
    def test_browser_request_raises_http_error_on_status(self, mock_curl_request: Mock) -> None:
        response = Mock()
        response.status_code = 503
        mock_curl_request.return_value = response
        with self.assertRaises(requests.exceptions.HTTPError):
            bic.browser_request(method="GET", url="http://x", timeout=1.0)

    @patch("scrapers.proxy.browser_impersonator_client.curl_request")
    def test_browser_request_success(self, mock_curl_request: Mock) -> None:
        response = Mock()
        response.status_code = 200
        response.text = "ok"
        mock_curl_request.return_value = response
        out = bic.browser_request(
            method="POST",
            url="http://x",
            timeout=3.0,
            headers={"a": "b"},
            data={"k": "v"},
            proxies={"http": "h:1"},
        )
        self.assertIs(out, response)
        mock_curl_request.assert_called_once()

    @patch("scrapers.proxy.browser_impersonator_client.curl_requests.get")
    def test_curl_get_passes_proxy_and_impersonate(self, mock_get: Mock) -> None:
        mock_get.return_value = object()
        bic.curl_get(
            "http://x",
            timeout=1.5,
            proxy="http://1.2.3.4:80",
            impersonate="chrome136",
            headers={"a": "b"},
        )
        mock_get.assert_called_once_with(
            "http://x",
            timeout=1.5,
            proxy="http://1.2.3.4:80",
            impersonate="chrome136",
            headers={"a": "b"},
        )

    @patch("scrapers.proxy.browser_impersonator_client.random_browser", return_value="edge101")
    @patch("scrapers.proxy.browser_impersonator_client.curl_requests.get")
    def test_curl_get_uses_random_browser(self, mock_get: Mock, _mock_random_browser: Mock) -> None:
        mock_get.return_value = object()
        bic.curl_get("http://x", timeout=1.0, use_random_browser=True)
        mock_get.assert_called_once_with("http://x", timeout=1.0, impersonate="edge101")

    @patch("scrapers.proxy.browser_impersonator_client.curl_requests.request")
    def test_curl_request_passes_proxy_and_impersonate(self, mock_request: Mock) -> None:
        mock_request.return_value = object()
        bic.curl_request(
            "POST",
            "http://x",
            timeout=2.0,
            proxy="http://1.2.3.4:80",
            impersonate="chrome124",
            data={"k": "v"},
        )
        mock_request.assert_called_once_with(
            method="POST",
            url="http://x",
            timeout=2.0,
            proxy="http://1.2.3.4:80",
            impersonate="chrome124",
            data={"k": "v"},
        )

    @patch("scrapers.proxy.browser_impersonator_client.random_browser", return_value="safari15_5")
    @patch("scrapers.proxy.browser_impersonator_client.curl_requests.request")
    def test_curl_request_uses_random_browser(self, mock_request: Mock, _mock_random_browser: Mock) -> None:
        mock_request.return_value = object()
        bic.curl_request("GET", "http://x", timeout=2.0, use_random_browser=True)
        mock_request.assert_called_once_with(
            method="GET",
            url="http://x",
            timeout=2.0,
            impersonate="safari15_5",
        )


if __name__ == "__main__":
    unittest.main()
