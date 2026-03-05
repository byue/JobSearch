"""Microsoft transport layer for JSON endpoint calls."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from scrapers.airflow.clients.common.request_policy import RequestPolicy
from scrapers.airflow.clients.common.http_requests import build_get_url, request_json_with_backoff
from scrapers.proxy.proxy_management_client import ProxyManagementClient


def require_mapping(value: Any, *, context: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(
            f"Unexpected Microsoft API payload for {context}: expected object, got {type(value).__name__}"
        )
    return value


class MicrosoftTransport:
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
        url = build_get_url(base_url=self.base_url, path=path, params=params)
        payload = request_json_with_backoff(
            url=url,
            headers={"Accept": "application/json", "Referer": "https://jobs.careers.microsoft.com/"},
            request_policy=request_policy,
            proxy_management_client=self.proxy_management_client,
        )
        return dict(require_mapping(payload, context=path))
