"""HTTP GET helper with browser impersonation for proxy workflows."""

from __future__ import annotations

import random
from collections.abc import Mapping
from typing import Any

from curl_cffi import requests as curl_requests
import requests

BROWSER = "chrome136"
BROWSERS = [
    "chrome99",
    "chrome100",
    "chrome101",
    "chrome104",
    "chrome107",
    "chrome110",
    "chrome116",
    "chrome119",
    "chrome120",
    "chrome123",
    "chrome124",
    "chrome131",
    "chrome136",
    "chrome142",
    "chrome99_android",
    "chrome131_android",
    "edge99",
    "edge101",
    "safari15_3",
    "safari15_5",
]


def random_browser() -> str:
    return random.choice(BROWSERS)


def normalize_proxy_mapping(proxy: Mapping[str, str] | None) -> dict[str, str] | None:
    if proxy is None:
        return None

    normalized: dict[str, str] = {}
    for key, value in proxy.items():
        normalized_key = str(key).strip().lower()
        normalized_value = str(value).strip()
        if not normalized_key or not normalized_value:
            continue
        if "://" not in normalized_value:
            normalized_value = f"http://{normalized_value}"
        normalized[normalized_key] = normalized_value
    return normalized or None


def select_proxy_url(proxy: Mapping[str, str] | None) -> str | None:
    normalized = normalize_proxy_mapping(proxy)
    if normalized is None:
        return None
    return normalized.get("https") or normalized.get("http")


def _http_error_for_status(*, status_code: int, url: str) -> requests.exceptions.HTTPError:
    response = requests.Response()
    response.status_code = status_code
    response.url = url
    return requests.exceptions.HTTPError(f"HTTP {status_code} for url: {url}", response=response)


def curl_get(
    url: str,
    *,
    timeout: float,
    proxy: str | None = None,
    impersonate: str | None = None,
    use_random_browser: bool = False,
    **kwargs: Any,
) -> Any:
    """Thin wrapper around curl_cffi GET with proxy/browser helpers."""
    request_kwargs: dict[str, Any] = dict(kwargs)
    request_kwargs["timeout"] = timeout
    if proxy is not None:
        request_kwargs["proxy"] = proxy
    if impersonate is not None:
        request_kwargs["impersonate"] = impersonate
    elif use_random_browser:
        request_kwargs["impersonate"] = random_browser()
    return curl_requests.get(url, **request_kwargs)


def curl_request(
    method: str,
    url: str,
    *,
    timeout: float,
    proxy: str | None = None,
    impersonate: str | None = None,
    use_random_browser: bool = False,
    **kwargs: Any,
) -> Any:
    """Thin wrapper around curl_cffi request with proxy/browser helpers."""
    request_kwargs: dict[str, Any] = dict(kwargs)
    request_kwargs["timeout"] = timeout
    if proxy is not None:
        request_kwargs["proxy"] = proxy
    if impersonate is not None:
        request_kwargs["impersonate"] = impersonate
    elif use_random_browser:
        request_kwargs["impersonate"] = random_browser()
    return curl_requests.request(method=method, url=url, **request_kwargs)


def browser_request(
    *,
    method: str,
    url: str,
    timeout: float,
    headers: Mapping[str, str] | None = None,
    proxies: Mapping[str, str] | None = None,
    data: Mapping[str, Any] | None = None,
    impersonate: str | None = None,
    use_random_browser: bool = True,
    require_proxy: bool = False,
) -> Any:
    proxy_url = select_proxy_url(proxies)
    if require_proxy and proxy_url is None:
        raise requests.exceptions.ProxyError("No proxy configured")
    try:
        response = curl_request(
            method,
            url,
            timeout=timeout,
            headers=dict(headers) if headers is not None else None,
            data=dict(data) if data is not None else None,
            proxy=proxy_url,
            impersonate=impersonate,
            use_random_browser=use_random_browser,
        )
    except Exception as exc:
        raise requests.exceptions.RequestException(str(exc)) from exc

    status_code = int(response.status_code)
    if status_code >= 400:
        raise _http_error_for_status(status_code=status_code, url=url)
    return response
