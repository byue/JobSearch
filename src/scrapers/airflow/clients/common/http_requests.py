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


def _is_connection_error(error: Exception) -> bool:
    current: BaseException | None = error
    visited: set[int] = set()
    while current is not None and id(current) not in visited:
        visited.add(id(current))
        message = str(current).lower()
        if "connection timed out" in message:
            return True
        if "failed to connect" in message:
            return True
        if "connection reset by peer" in message:
            return True
        if "connection closed abruptly" in message:
            return True
        if "connect tunnel failed" in message:
            return True
        current = current.__cause__ if current.__cause__ is not None else current.__context__
    return False


def _host_from_url(url: str) -> str:
    return urllib.parse.urlparse(url).netloc or "unknown"


def _status_value(status: Any) -> int | None:
    try:
        return int(status)
    except (TypeError, ValueError):
        return None


def _error_details(error: BaseException) -> str:
    parts: list[str] = []
    current: BaseException | None = error
    visited: set[int] = set()
    while current is not None and id(current) not in visited:
        visited.add(id(current))
        parts.append(f"{type(current).__name__}: {current}")
        current = current.__cause__ if current.__cause__ is not None else current.__context__
    return " <- ".join(parts)


def _should_giveup(error: Exception) -> bool:
    if isinstance(error, requests.exceptions.HTTPError):
        status_code = error.response.status_code if error.response is not None else None
        if status_code is None:
            return False
        # Retry 429 and any 5xx; give up on all other HTTP statuses.
        return not (status_code == 429 or 500 <= status_code <= 599)
    return False


def _should_block_proxy(error: Exception) -> bool:
    if _is_connection_error(error):
        return True
    status = _status_from_exception(error)
    return status == 403


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

    @backoff.on_exception(
        backoff.expo,
        requests.exceptions.RequestException,
        max_tries=attempts,
        giveup=_should_giveup,
        factor=request_policy.backoff_factor,
        max_value=request_policy.max_backoff_seconds,
        jitter=backoff.full_jitter if request_policy.jitter else None,
    )
    def _request() -> str:
        try:
            response = browser_request(
                method=normalized_method,
                url=url,
                headers=dict(headers),
                timeout=request_policy.timeout_for_http(),
                proxies=proxies,
                data=dict(data) if data is not None else None,
                use_random_browser=True,
                require_proxy=True,
            )
            return str(response.text)
        except Exception as exc:
            LOGGER.warning(
                "request_retry_failed method=%s url=%s error_details=%s status=%s",
                normalized_method,
                url,
                _error_details(exc),
                _status_from_exception(exc),
            )
            raise

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

    @backoff.on_exception(
        backoff.expo,
        requests.exceptions.RequestException,
        max_tries=attempts,
        giveup=_should_giveup,
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
                timeout=request_policy.timeout_for_http(),
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
                "proxy_request_failed resource=%s host=%s error=%s error_details=%s status=%s",
                resource,
                target_host,
                type(exc).__name__,
                _error_details(exc),
                _status_from_exception(exc),
            )
            should_block = _should_block_proxy(exc)
            completed = proxy_management_client.complete_requests_proxy(
                resource=resource,
                token=token,
                success=not should_block,
                scope=target_host,
            )
            if not completed:
                action = "block" if should_block else "release"
                LOGGER.warning(
                    "proxy_lease_complete_failed action=%s resource=%s host=%s",
                    action,
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


def request_text_with_managed_proxy_backoff(
    *,
    method: str,
    url: str,
    headers: Mapping[str, str],
    request_policy: RequestPolicy,
    proxy_management_client: ProxyManagementClient,
    data: Mapping[str, Any] | None = None,
) -> str:
    """Perform one request with managed proxy leases and exponential-backoff retries."""
    attempts = max(request_policy.max_retries, 1)
    normalized_method = method.strip().upper()
    if not normalized_method:
        raise ValueError("method must be a non-empty HTTP verb")

    @backoff.on_exception(
        backoff.expo,
        requests.exceptions.RequestException,
        max_tries=attempts,
        giveup=_should_giveup,
        factor=request_policy.backoff_factor,
        max_value=request_policy.max_backoff_seconds,
        jitter=backoff.full_jitter if request_policy.jitter else None,
    )
    def _request() -> str:
        target_host = _host_from_url(url)
        proxies, resource, token = _proxy_management_result(proxy_management_client, scope=target_host)
        try:
            response = browser_request(
                method=normalized_method,
                url=url,
                headers=dict(headers),
                timeout=request_policy.timeout_for_http(),
                proxies=proxies,
                data=dict(data) if data is not None else None,
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
            return str(response.text)
        except Exception as exc:
            LOGGER.warning(
                "proxy_request_failed resource=%s host=%s error=%s error_details=%s status=%s",
                resource,
                target_host,
                type(exc).__name__,
                _error_details(exc),
                _status_from_exception(exc),
            )
            should_block = _should_block_proxy(exc)
            completed = proxy_management_client.complete_requests_proxy(
                resource=resource,
                token=token,
                success=not should_block,
                scope=target_host,
            )
            if not completed:
                action = "block" if should_block else "release"
                LOGGER.warning(
                    "proxy_lease_complete_failed action=%s resource=%s host=%s",
                    action,
                    resource,
                    target_host,
                )
            raise

    return _request()


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
