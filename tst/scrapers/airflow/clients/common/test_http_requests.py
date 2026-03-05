import unittest
from unittest.mock import Mock, patch

import requests

from scrapers.airflow.clients.common import http_requests as http
from scrapers.airflow.clients.common.request_policy import RequestPolicy


class HttpRequestsTest(unittest.TestCase):
    def test_build_get_url(self) -> None:
        url = http.build_get_url(base_url="https://example.com", path="/jobs", params=[("q", "x"), ("page", "1")])
        self.assertEqual(url, "https://example.com/jobs?q=x&page=1")
        url_with_existing = http.build_get_url(
            base_url="https://example.com",
            path="/jobs?lang=en",
            params=[("q", "x")],
        )
        self.assertEqual(url_with_existing, "https://example.com/jobs?lang=en&q=x")

    def test_proxy_management_result(self) -> None:
        proxy_client = Mock()
        proxy_client.acquire_requests_proxy.return_value = ({"http": "1.2.3.4:80"}, "http://1.2.3.4:80", "tok")
        with patch("scrapers.airflow.clients.common.http_requests.normalize_proxy_mapping") as normalize:
            normalize.return_value = {"http": "http://1.2.3.4:80"}
            proxies, resource, token = http._proxy_management_result(proxy_client)
        self.assertEqual(proxies, {"http": "http://1.2.3.4:80"})
        self.assertEqual(resource, "http://1.2.3.4:80")
        self.assertEqual(token, "tok")

        with patch("scrapers.airflow.clients.common.http_requests.normalize_proxy_mapping", return_value=None):
            with self.assertRaises(requests.exceptions.ProxyError):
                http._proxy_management_result(proxy_client)

    def test_request_text_with_session_backoff(self) -> None:
        policy = RequestPolicy(timeout_seconds=1.0, max_retries=1)
        with self.assertRaises(ValueError):
            http.request_text_with_session_backoff(
                method=" ",
                url="https://x",
                headers={},
                request_policy=policy,
            )

        response = Mock()
        response.text = "ok"
        with patch("scrapers.airflow.clients.common.http_requests.browser_request", return_value=response) as br:
            text = http.request_text_with_session_backoff(
                method="get",
                url="https://x",
                headers={"a": "b"},
                request_policy=policy,
                proxies={"http": "http://1.2.3.4:80"},
                data={"k": "v"},
            )
        self.assertEqual(text, "ok")
        br.assert_called_once()

    def test_request_text_with_session_backoff_http_error_giveup(self) -> None:
        policy = RequestPolicy(timeout_seconds=1.0, max_retries=1, retryable_status_codes=frozenset({429}))
        http_error = requests.exceptions.HTTPError("boom")
        response = requests.Response()
        response.status_code = 500
        http_error.response = response
        with patch("scrapers.airflow.clients.common.http_requests.browser_request", side_effect=http_error):
            with self.assertRaises(requests.exceptions.HTTPError):
                http.request_text_with_session_backoff(
                    method="GET",
                    url="https://x",
                    headers={},
                    request_policy=policy,
                )
        no_response_error = requests.exceptions.HTTPError("boom")
        no_response_error.response = None
        with patch("scrapers.airflow.clients.common.http_requests.browser_request", side_effect=no_response_error):
            with self.assertRaises(requests.exceptions.HTTPError):
                http.request_text_with_session_backoff(
                    method="GET",
                    url="https://x",
                    headers={},
                    request_policy=policy,
                )
        with patch(
            "scrapers.airflow.clients.common.http_requests.browser_request",
            side_effect=requests.exceptions.RequestException("boom"),
        ):
            with self.assertRaises(requests.exceptions.RequestException):
                http.request_text_with_session_backoff(
                    method="GET",
                    url="https://x",
                    headers={},
                    request_policy=policy,
                )

    def test_request_bytes_with_backoff_success_and_failure(self) -> None:
        policy = RequestPolicy(timeout_seconds=1.0, max_retries=1)
        proxy_client = Mock()

        response = Mock()
        response.content = b"abc"
        with patch(
            "scrapers.airflow.clients.common.http_requests._proxy_management_result",
            return_value=({"http": "http://1.2.3.4:80"}, "http://1.2.3.4:80", "tok"),
        ), patch("scrapers.airflow.clients.common.http_requests.browser_request", return_value=response):
            out = http.request_bytes_with_backoff(
                url="https://x",
                headers={"h": "v"},
                request_policy=policy,
                proxy_management_client=proxy_client,
            )
        self.assertEqual(out, b"abc")
        proxy_client.complete_requests_proxy.assert_called_with(
            resource="http://1.2.3.4:80",
            token="tok",
            success=True,
            scope="x",
        )

        proxy_client.reset_mock()
        with patch(
            "scrapers.airflow.clients.common.http_requests._proxy_management_result",
            return_value=({"http": "http://1.2.3.4:80"}, "http://1.2.3.4:80", "tok"),
        ), patch(
            "scrapers.airflow.clients.common.http_requests.browser_request",
            side_effect=requests.exceptions.RequestException("boom"),
        ):
            with self.assertRaises(requests.exceptions.RequestException):
                http.request_bytes_with_backoff(
                    url="https://x",
                    headers={"h": "v"},
                    request_policy=policy,
                    proxy_management_client=proxy_client,
                )
        proxy_client.complete_requests_proxy.assert_called_with(
            resource="http://1.2.3.4:80",
            token="tok",
            success=False,
            scope="x",
        )

    def test_request_bytes_with_backoff_logs_failed_completion_paths(self) -> None:
        policy = RequestPolicy(timeout_seconds=1.0, max_retries=1)
        proxy_client = Mock()
        proxy_client.complete_requests_proxy.return_value = False

        response = Mock()
        response.content = b"abc"
        with patch(
            "scrapers.airflow.clients.common.http_requests._proxy_management_result",
            return_value=({"http": "http://1.2.3.4:80"}, "http://1.2.3.4:80", "tok"),
        ), patch("scrapers.airflow.clients.common.http_requests.browser_request", return_value=response), patch(
            "scrapers.airflow.clients.common.http_requests.LOGGER.warning"
        ) as warn:
            out = http.request_bytes_with_backoff(
                url="https://x",
                headers={"h": "v"},
                request_policy=policy,
                proxy_management_client=proxy_client,
            )
            self.assertEqual(out, b"abc")
            self.assertTrue(any("proxy_lease_complete_failed action=release" in call.args[0] for call in warn.call_args_list))

        proxy_client.reset_mock()
        proxy_client.complete_requests_proxy.return_value = False
        with patch(
            "scrapers.airflow.clients.common.http_requests._proxy_management_result",
            return_value=({"http": "http://1.2.3.4:80"}, "http://1.2.3.4:80", "tok"),
        ), patch(
            "scrapers.airflow.clients.common.http_requests.browser_request",
            side_effect=requests.exceptions.RequestException("boom"),
        ), patch("scrapers.airflow.clients.common.http_requests.LOGGER.warning") as warn:
            with self.assertRaises(requests.exceptions.RequestException):
                http.request_bytes_with_backoff(
                    url="https://x",
                    headers={"h": "v"},
                    request_policy=policy,
                    proxy_management_client=proxy_client,
                )
            self.assertTrue(any("proxy_lease_complete_failed action=block" in call.args[0] for call in warn.call_args_list))

    def test_request_bytes_with_backoff_http_error_giveup(self) -> None:
        policy = RequestPolicy(timeout_seconds=1.0, max_retries=1, retryable_status_codes=frozenset({429}))
        proxy_client = Mock()
        http_error = requests.exceptions.HTTPError("boom")
        response = requests.Response()
        response.status_code = 500
        http_error.response = response
        with patch(
            "scrapers.airflow.clients.common.http_requests._proxy_management_result",
            return_value=({"http": "http://1.2.3.4:80"}, "http://1.2.3.4:80", "tok"),
        ), patch("scrapers.airflow.clients.common.http_requests.browser_request", side_effect=http_error):
            with self.assertRaises(requests.exceptions.HTTPError):
                http.request_bytes_with_backoff(
                    url="https://x",
                    headers={},
                    request_policy=policy,
                    proxy_management_client=proxy_client,
                )
        http_error_no_response = requests.exceptions.HTTPError("boom")
        http_error_no_response.response = None
        with patch(
            "scrapers.airflow.clients.common.http_requests._proxy_management_result",
            return_value=({"http": "http://1.2.3.4:80"}, "http://1.2.3.4:80", "tok"),
        ), patch("scrapers.airflow.clients.common.http_requests.browser_request", side_effect=http_error_no_response):
            with self.assertRaises(requests.exceptions.HTTPError):
                http.request_bytes_with_backoff(
                    url="https://x",
                    headers={},
                    request_policy=policy,
                    proxy_management_client=proxy_client,
                )

    def test_request_text_and_json_with_backoff(self) -> None:
        policy = RequestPolicy(timeout_seconds=1.0, max_retries=1)
        proxy_client = Mock()
        with patch("scrapers.airflow.clients.common.http_requests.request_bytes_with_backoff", return_value=b"hello"):
            self.assertEqual(
                http.request_text_with_backoff(
                    url="https://x",
                    headers={},
                    request_policy=policy,
                    proxy_management_client=proxy_client,
                ),
                "hello",
            )

        with patch("scrapers.airflow.clients.common.http_requests.request_text_with_backoff", return_value='{"a":1}'):
            self.assertEqual(
                http.request_json_with_backoff(
                    url="https://x",
                    headers={},
                    request_policy=policy,
                    proxy_management_client=proxy_client,
                ),
                {"a": 1},
            )


if __name__ == "__main__":
    unittest.main()
