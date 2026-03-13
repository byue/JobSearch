"""Apple transport layer for fetching hydration pages."""

from __future__ import annotations

from collections.abc import Iterable

from common.request_policy import RequestPolicy
from scrapers.airflow.clients.common.http_requests import build_get_url, request_text_with_backoff
from scrapers.proxy.proxy_management_client import ProxyManagementClient


class AppleTransport:
    def __init__(self, *, base_url: str, proxy_management_client: ProxyManagementClient) -> None:
        self.base_url = base_url.rstrip("/")
        self.proxy_management_client = proxy_management_client

    def get_html(
        self,
        *,
        path: str,
        params: Iterable[tuple[str, str]],
        request_policy: RequestPolicy,
    ) -> str:
        url = build_get_url(base_url=self.base_url, path=path, params=params)
        return request_text_with_backoff(
            url=url,
            headers={"Accept": "text/html"},
            request_policy=request_policy,
            proxy_management_client=self.proxy_management_client,
        )
