"""Amazon transport layer for HTTP calls."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from scrapers.airflow.clients.common.request_policy import RequestPolicy
from scrapers.airflow.clients.common.http_requests import build_get_url, request_json_with_backoff
from scrapers.proxy.proxy_management_client import ProxyManagementClient


def _require_mapping(value: Any, *, context: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(
            f"Unexpected Amazon API payload for {context}: expected object, got {type(value).__name__}"
        )
    return value


class AmazonTransport:
    """Thin request wrapper for Amazon endpoints."""

    def __init__(self, *, base_url: str, proxy_management_client: ProxyManagementClient) -> None:
        self.base_url = base_url.rstrip("/")
        self.proxy_management_client = proxy_management_client

    def get_json(
        self,
        path: str,
        *,
        params: Iterable[tuple[str, str]],
        request_policy: RequestPolicy,
    ) -> dict[str, Any]:
        url = build_get_url(
            base_url=self.base_url,
            path=path,
            params=params,
        )
        payload = request_json_with_backoff(
            url=url,
            headers={
                "Accept-Encoding": "gzip, deflate",
            },
            request_policy=request_policy,
            proxy_management_client=self.proxy_management_client,
        )
        parsed_payload = _require_mapping(payload, context=path)
        return dict(parsed_payload)
