"""Shared HTTP request helpers for company clients."""

from __future__ import annotations

import json
import logging
import urllib.parse
from collections.abc import Iterable, Mapping
from typing import Any

import backoff
import requests

from scrapers.airflow.clients.common.request_policy import RequestPolicy
from scrapers.proxy.browser_impersonator_client import browser_request, normalize_proxy_mapping
from scrapers.proxy.proxy_management_client import ProxyManagementClient

LOGGER = logging.getLogger(__name__)


def _status_from_exception(error: Exception) -> int | None:
    if isinstance(error, requests.exceptions.HTTPError) and error.response is not None:
        return int(error.response.status_code)
    return None


def _host_from_url(url: str) -> str:
    return urllib.parse.urlparse(url).netloc or "unknown"


def _status_value(status: Any) -> int | None:
    try:
        return int(status)
    except (TypeError, ValueError):
        return None


def build_get_url(
    *,
    base_url: str,
    path: str,
    params: Iterable[tuple[str, str]],
) -> str:
    """Build a URL by appending query params to a base URL and path."""
    query = urllib.parse.urlencode(list(params), doseq=True)
    url = f"{base_url}{path}"
    if query:
        separator = "&" if "?" in url else "?"
        url = f"{url}{separator}{query}"
    return url


def _proxy_management_result(
    proxy_management_client: ProxyManagementClient,
    *,
    scope: str | None = None,
) -> tuple[dict[str, str], str, str]:
    fetched, resource, token = proxy_management_client.acquire_requests_proxy(scope=scope)
    normalized = normalize_proxy_mapping(fetched)
    if normalized is None:
        raise requests.exceptions.ProxyError("No proxy available from proxy management client")
    return normalized, resource, token


def request_text_with_session_backoff(
    *,
    method: str,
    url: str,
    headers: Mapping[str, str],
    request_policy: RequestPolicy,
    proxies: dict[str, str] | None = None,
    data: Mapping[str, Any] | None = None,
) -> str:
    """Perform one request and return text body."""
    attempts = max(request_policy.max_retries, 1)
    normalized_method = method.strip().upper()
    if not normalized_method:
        raise ValueError("method must be a non-empty HTTP verb")

    def _giveup(error: Exception) -> bool:
        if isinstance(error, requests.exceptions.HTTPError):
            status_code = error.response.status_code if error.response is not None else None
            if status_code is None:
                return False
            return status_code not in request_policy.retryable_status_codes
        return False

    @backoff.on_exception(
        backoff.expo,
        requests.exceptions.RequestException,
        max_tries=attempts,
        giveup=_giveup,
        factor=request_policy.backoff_factor,
        max_value=request_policy.max_backoff_seconds,
        jitter=backoff.full_jitter if request_policy.jitter else None,
    )
    def _request() -> str:
        response = browser_request(
            method=normalized_method,
            url=url,
            headers=dict(headers),
            timeout=float(request_policy.timeout_seconds),
            proxies=proxies,
            data=dict(data) if data is not None else None,
            use_random_browser=True,
            require_proxy=True,
        )
        return str(response.text)

    return _request()


def request_bytes_with_backoff(
    *,
    url: str,
    headers: Mapping[str, str],
    request_policy: RequestPolicy,
    proxy_management_client: ProxyManagementClient,
) -> bytes:
    """Perform one GET request with exponential-backoff retries."""
    attempts = max(request_policy.max_retries, 1)

    def _giveup(error: Exception) -> bool:
        if isinstance(error, requests.exceptions.HTTPError):
            status_code = error.response.status_code if error.response is not None else None
            if status_code is None:
                return False
            return status_code not in request_policy.retryable_status_codes
        return False

    @backoff.on_exception(
        backoff.expo,
        requests.exceptions.RequestException,
        max_tries=attempts,
        giveup=_giveup,
        factor=request_policy.backoff_factor,
        max_value=request_policy.max_backoff_seconds,
        jitter=backoff.full_jitter if request_policy.jitter else None,
    )
    def _request() -> bytes:
        target_host = _host_from_url(url)
        proxies, resource, token = _proxy_management_result(proxy_management_client, scope=target_host)
        try:
            response = browser_request(
                method="GET",
                url=url,
                headers=dict(headers),
                timeout=float(request_policy.timeout_seconds),
                proxies=proxies,
                use_random_browser=True,
                require_proxy=True,
            )
            completed = proxy_management_client.complete_requests_proxy(
                resource=resource,
                token=token,
                success=True,
                scope=target_host,
            )
            if not completed:
                LOGGER.warning(
                    "proxy_lease_complete_failed action=release resource=%s host=%s",
                    resource,
                    target_host,
                )
            else:
                LOGGER.debug(
                    "proxy_request_ok resource=%s host=%s status=%s",
                    resource,
                    target_host,
                    _status_value(getattr(response, "status_code", None)),
                )
            return bytes(response.content)
        except Exception as exc:
            LOGGER.warning(
                "proxy_request_failed resource=%s host=%s error=%s status=%s",
                resource,
                target_host,
                type(exc).__name__,
                _status_from_exception(exc),
            )
            completed = proxy_management_client.complete_requests_proxy(
                resource=resource,
                token=token,
                success=False,
                scope=target_host,
            )
            if not completed:
                LOGGER.warning(
                    "proxy_lease_complete_failed action=block resource=%s host=%s",
                    resource,
                    target_host,
                )
            raise

    return _request()


def request_text_with_backoff(
    *,
    url: str,
    headers: Mapping[str, str],
    request_policy: RequestPolicy,
    proxy_management_client: ProxyManagementClient,
) -> str:
    """Perform one GET request and decode response bytes as UTF-8 text."""
    return request_bytes_with_backoff(
        url=url,
        headers=headers,
        request_policy=request_policy,
        proxy_management_client=proxy_management_client,
    ).decode("utf-8")


def request_json_with_backoff(
    *,
    url: str,
    headers: Mapping[str, str],
    request_policy: RequestPolicy,
    proxy_management_client: ProxyManagementClient,
) -> Any:
    """Perform one GET request and parse response payload as JSON."""
    response_text = request_text_with_backoff(
        url=url,
        headers=headers,
        request_policy=request_policy,
        proxy_management_client=proxy_management_client,
    )
    return json.loads(response_text)
