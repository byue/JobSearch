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
        policy = RequestPolicy(timeout_seconds=1.0, max_retries=1)
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
            success=True,
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
            self.assertTrue(
                any(
                    call.args
                    and "proxy_lease_complete_failed" in " ".join(str(arg) for arg in call.args)
                    and "release" in " ".join(str(arg) for arg in call.args)
                    for call in warn.call_args_list
                )
            )

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
            self.assertTrue(
                any(
                    call.args
                    and "proxy_lease_complete_failed" in " ".join(str(arg) for arg in call.args)
                    and "release" in " ".join(str(arg) for arg in call.args)
                    for call in warn.call_args_list
                )
            )

        proxy_client.reset_mock()
        proxy_client.complete_requests_proxy.return_value = False
        http_403 = requests.exceptions.HTTPError("forbidden")
        response = requests.Response()
        response.status_code = 403
        http_403.response = response
        with patch(
            "scrapers.airflow.clients.common.http_requests._proxy_management_result",
            return_value=({"http": "http://1.2.3.4:80"}, "http://1.2.3.4:80", "tok"),
        ), patch(
            "scrapers.airflow.clients.common.http_requests.browser_request",
            side_effect=http_403,
        ), patch("scrapers.airflow.clients.common.http_requests.LOGGER.warning") as warn:
            with self.assertRaises(requests.exceptions.HTTPError):
                http.request_bytes_with_backoff(
                    url="https://x",
                    headers={"h": "v"},
                    request_policy=policy,
                    proxy_management_client=proxy_client,
                )
            self.assertTrue(
                any(
                    call.args
                    and "proxy_lease_complete_failed" in " ".join(str(arg) for arg in call.args)
                    and "block" in " ".join(str(arg) for arg in call.args)
                    for call in warn.call_args_list
                )
            )

    def test_request_bytes_with_backoff_http_error_giveup(self) -> None:
        policy = RequestPolicy(timeout_seconds=1.0, max_retries=1)
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

    def test_request_helpers_pass_tuple_timeout_when_connect_timeout_set(self) -> None:
        policy = RequestPolicy(timeout_seconds=4.0, connect_timeout_seconds=1.0, max_retries=1)
        response = Mock()
        response.text = "ok"
        with patch("scrapers.airflow.clients.common.http_requests.browser_request", return_value=response) as br:
            _ = http.request_text_with_session_backoff(
                method="GET",
                url="https://x",
                headers={},
                request_policy=policy,
            )
        self.assertEqual(br.call_args.kwargs["timeout"], (1.0, 4.0))

    def test_is_connection_error_patterns(self) -> None:
        timed_out_error = requests.exceptions.RequestException("Connection timed out after 2000 milliseconds")
        self.assertTrue(http._is_connection_error(timed_out_error))

        failed_connect_error = requests.exceptions.RequestException("Failed to connect to target")
        self.assertTrue(http._is_connection_error(failed_connect_error))

        reset_by_peer_error = requests.exceptions.RequestException(
            "Failed to perform, curl: (56) Recv failure: Connection reset by peer"
        )
        self.assertTrue(http._is_connection_error(reset_by_peer_error))

        closed_abruptly_error = requests.exceptions.RequestException(
            "Failed to perform, curl: (56) Connection closed abruptly"
        )
        self.assertTrue(http._is_connection_error(closed_abruptly_error))

        tunnel_failed_error = requests.exceptions.RequestException(
            "Failed to perform, curl: (56) CONNECT tunnel failed, response 502"
        )
        self.assertTrue(http._is_connection_error(tunnel_failed_error))

        unrelated_error = requests.exceptions.RequestException("some other request failure")
        self.assertFalse(http._is_connection_error(unrelated_error))

    def test_should_block_proxy_patterns(self) -> None:
        connect_error = requests.exceptions.RequestException("Failed to connect to target")
        self.assertTrue(http._should_block_proxy(connect_error))

        tunnel_error = requests.exceptions.RequestException("CONNECT tunnel failed, response 502")
        self.assertTrue(http._should_block_proxy(tunnel_error))

        curl56_error = requests.exceptions.RequestException("curl: (56) recv failure")
        self.assertFalse(http._should_block_proxy(curl56_error))

        http_403 = requests.exceptions.HTTPError("forbidden")
        response = requests.Response()
        response.status_code = 403
        http_403.response = response
        self.assertTrue(http._should_block_proxy(http_403))

        other_error = requests.exceptions.RequestException("boom")
        self.assertFalse(http._should_block_proxy(other_error))

    def test_session_backoff_retries_on_connection_error(self) -> None:
        policy = RequestPolicy(timeout_seconds=1.0, max_retries=5, backoff_factor=0.0)
        error = requests.exceptions.RequestException("Failed to connect to target")
        with patch("scrapers.airflow.clients.common.http_requests.browser_request", side_effect=error) as br:
            with self.assertRaises(requests.exceptions.RequestException):
                http.request_text_with_session_backoff(
                    method="GET",
                    url="https://x",
                    headers={},
                    request_policy=policy,
                )
        self.assertEqual(br.call_count, 5)

    def test_bytes_backoff_retries_on_connection_error(self) -> None:
        policy = RequestPolicy(timeout_seconds=1.0, max_retries=5, backoff_factor=0.0)
        proxy_client = Mock()
        error = requests.exceptions.RequestException("Connection timed out after 2000 milliseconds")
        with patch(
            "scrapers.airflow.clients.common.http_requests._proxy_management_result",
            return_value=({"http": "http://1.2.3.4:80"}, "http://1.2.3.4:80", "tok"),
        ), patch("scrapers.airflow.clients.common.http_requests.browser_request", side_effect=error) as br:
            with self.assertRaises(requests.exceptions.RequestException):
                http.request_bytes_with_backoff(
                    url="https://x",
                    headers={},
                    request_policy=policy,
                    proxy_management_client=proxy_client,
                )
        self.assertEqual(br.call_count, 5)
        proxy_client.complete_requests_proxy.assert_called_with(
            resource="http://1.2.3.4:80",
            token="tok",
            success=False,
            scope="x",
        )

    def test_request_text_with_managed_proxy_backoff_validation_and_success(self) -> None:
        policy = RequestPolicy(timeout_seconds=1.0, max_retries=1)
        proxy_client = Mock()
        with self.assertRaises(ValueError):
            http.request_text_with_managed_proxy_backoff(
                method=" ",
                url="https://x",
                headers={},
                request_policy=policy,
                proxy_management_client=proxy_client,
            )

        response = Mock()
        response.text = "ok"
        with patch(
            "scrapers.airflow.clients.common.http_requests._proxy_management_result",
            return_value=({"http": "http://1.2.3.4:80"}, "http://1.2.3.4:80", "tok"),
        ), patch("scrapers.airflow.clients.common.http_requests.browser_request", return_value=response) as br:
            text = http.request_text_with_managed_proxy_backoff(
                method="post",
                url="https://x",
                headers={"h": "v"},
                request_policy=policy,
                proxy_management_client=proxy_client,
                data={"k": "v"},
            )
        self.assertEqual(text, "ok")
        self.assertEqual(br.call_args.kwargs["method"], "POST")
        proxy_client.complete_requests_proxy.assert_called_with(
            resource="http://1.2.3.4:80",
            token="tok",
            success=True,
            scope="x",
        )

    def test_request_text_with_managed_proxy_backoff_failure_paths(self) -> None:
        policy = RequestPolicy(timeout_seconds=1.0, max_retries=1)
        proxy_client = Mock()
        proxy_client.complete_requests_proxy.return_value = False
        with patch(
            "scrapers.airflow.clients.common.http_requests._proxy_management_result",
            return_value=({"http": "http://1.2.3.4:80"}, "http://1.2.3.4:80", "tok"),
        ), patch(
            "scrapers.airflow.clients.common.http_requests.browser_request",
            side_effect=requests.exceptions.RequestException("boom"),
        ), patch("scrapers.airflow.clients.common.http_requests.LOGGER.warning") as warn:
            with self.assertRaises(requests.exceptions.RequestException):
                http.request_text_with_managed_proxy_backoff(
                    method="GET",
                    url="https://x",
                    headers={},
                    request_policy=policy,
                    proxy_management_client=proxy_client,
                )
            self.assertTrue(
                any(
                    call.args
                    and "proxy_lease_complete_failed" in " ".join(str(arg) for arg in call.args)
                    and "release" in " ".join(str(arg) for arg in call.args)
                    for call in warn.call_args_list
                )
            )

        proxy_client.reset_mock()
        proxy_client.complete_requests_proxy.return_value = False
        http_403 = requests.exceptions.HTTPError("forbidden")
        response = requests.Response()
        response.status_code = 403
        http_403.response = response
        with patch(
            "scrapers.airflow.clients.common.http_requests._proxy_management_result",
            return_value=({"http": "http://1.2.3.4:80"}, "http://1.2.3.4:80", "tok"),
        ), patch(
            "scrapers.airflow.clients.common.http_requests.browser_request",
            side_effect=http_403,
        ), patch("scrapers.airflow.clients.common.http_requests.LOGGER.warning") as warn:
            with self.assertRaises(requests.exceptions.HTTPError):
                http.request_text_with_managed_proxy_backoff(
                    method="GET",
                    url="https://x",
                    headers={},
                    request_policy=policy,
                    proxy_management_client=proxy_client,
                )
            self.assertTrue(
                any(
                    call.args
                    and "proxy_lease_complete_failed" in " ".join(str(arg) for arg in call.args)
                    and "block" in " ".join(str(arg) for arg in call.args)
                    for call in warn.call_args_list
                )
            )

        proxy_client.reset_mock()
        proxy_client.complete_requests_proxy.return_value = False
        with patch(
            "scrapers.airflow.clients.common.http_requests._proxy_management_result",
            return_value=({"http": "http://1.2.3.4:80"}, "http://1.2.3.4:80", "tok"),
        ), patch(
            "scrapers.airflow.clients.common.http_requests.browser_request",
            side_effect=requests.exceptions.RequestException("Connection timed out after 2000 milliseconds"),
        ), patch("scrapers.airflow.clients.common.http_requests.LOGGER.warning") as warn:
            with self.assertRaises(requests.exceptions.RequestException):
                http.request_text_with_managed_proxy_backoff(
                    method="GET",
                    url="https://x",
                    headers={},
                    request_policy=policy,
                    proxy_management_client=proxy_client,
                )
            self.assertTrue(
                any(
                    call.args
                    and "proxy_lease_complete_failed" in " ".join(str(arg) for arg in call.args)
                    and "block" in " ".join(str(arg) for arg in call.args)
                    for call in warn.call_args_list
                )
            )

        proxy_client.reset_mock()
        proxy_client.complete_requests_proxy.return_value = False
        response = Mock()
        response.text = "ok"
        with patch(
            "scrapers.airflow.clients.common.http_requests._proxy_management_result",
            return_value=({"http": "http://1.2.3.4:80"}, "http://1.2.3.4:80", "tok"),
        ), patch("scrapers.airflow.clients.common.http_requests.browser_request", return_value=response), patch(
            "scrapers.airflow.clients.common.http_requests.LOGGER.warning"
        ) as warn:
            out = http.request_text_with_managed_proxy_backoff(
                method="GET",
                url="https://x",
                headers={},
                request_policy=policy,
                proxy_management_client=proxy_client,
            )
            self.assertEqual(out, "ok")
            self.assertTrue(
                any(
                    call.args
                    and "proxy_lease_complete_failed" in " ".join(str(arg) for arg in call.args)
                    and "release" in " ".join(str(arg) for arg in call.args)
                    for call in warn.call_args_list
                )
            )

    def test_request_text_with_managed_proxy_backoff_retries_on_connection_error(self) -> None:
        policy = RequestPolicy(timeout_seconds=1.0, max_retries=5, backoff_factor=0.0)
        proxy_client = Mock()
        error = requests.exceptions.RequestException("Failed to connect to target")
        with patch(
            "scrapers.airflow.clients.common.http_requests._proxy_management_result",
            return_value=({"http": "http://1.2.3.4:80"}, "http://1.2.3.4:80", "tok"),
        ), patch("scrapers.airflow.clients.common.http_requests.browser_request", side_effect=error) as br:
            with self.assertRaises(requests.exceptions.RequestException):
                http.request_text_with_managed_proxy_backoff(
                    method="GET",
                    url="https://x",
                    headers={},
                    request_policy=policy,
                    proxy_management_client=proxy_client,
                )
        self.assertEqual(br.call_count, 5)


if __name__ == "__main__":
    unittest.main()
