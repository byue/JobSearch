"""Proxy discovery and validation client used by refiller/server."""

from __future__ import annotations

import ipaddress
from typing import Any
from urllib.parse import urlparse, urlsplit

from lxml.html import fromstring

from scrapers.proxy.browser_impersonator_client import BROWSER, curl_get

IP_CHECK_URL = "https://api64.ipify.org?format=json"
PROXY_LIST_URL = "https://free-proxy-list.net"


class ProxyGeneratorClient:
    """Fetch and validate public proxy URLs."""

    def __init__(
        self,
        *,
        validate_timeout_seconds: float,
        list_fetch_timeout_seconds: float,
    ) -> None:
        self.validate_timeout_seconds = max(float(validate_timeout_seconds), 0.2)
        self.list_fetch_timeout_seconds = max(float(list_fetch_timeout_seconds), 0.2)
        self.local_public_ip = self._get_local_public_ip()
        if self.local_public_ip is None:
            raise RuntimeError("Unable to resolve local public IP")

    def _is_well_formed_proxy_url(self, proxy_url: str) -> bool:
        parsed = urlparse(proxy_url)
        if parsed.scheme not in {"http", "https"}:
            return False
        if not parsed.hostname:
            return False
        if parsed.port is None or not (1 <= parsed.port <= 65535):
            return False
        try:
            ipaddress.ip_address(parsed.hostname)
            return True
        except ValueError:
            return False

    def _normalize_proxy_url(self, proxy_url: str | None) -> str | None:
        """Normalize proxy URL and validate basic shape."""
        if proxy_url is None:
            return None
        normalized = str(proxy_url).strip()
        if not normalized:
            return None
        if "://" not in normalized:
            normalized = f"http://{normalized}"
        split = urlsplit(normalized)
        if not split.hostname or split.port is None:
            return None
        return normalized

    def _extract_ip_from_payload(self, payload: dict[str, Any]) -> str | None:
        ip_value = payload.get("ip")
        if not isinstance(ip_value, str):
            return None
        ip_value = ip_value.strip()
        try:
            ipaddress.ip_address(ip_value)
        except ValueError:
            return None
        return ip_value

    def _get_local_public_ip(self) -> str | None:
        try:
            response = curl_get(
                IP_CHECK_URL,
                timeout=self.validate_timeout_seconds,
                impersonate=BROWSER,
            )
            if not response.ok:
                return None
            return self._extract_ip_from_payload(response.json())
        except Exception:
            return None

    def get_proxy_urls(self) -> list[str]:
        """Scrape candidate proxy URLs from upstream source."""
        response = curl_get(PROXY_LIST_URL, timeout=self.list_fetch_timeout_seconds)
        response.raise_for_status()
        parser = fromstring(response.text)
        proxy_urls: set[str] = set()
        for row in parser.xpath("//tbody/tr"):
            host = row.xpath(".//td[1]/text()")
            port = row.xpath(".//td[2]/text()")
            if host and port:
                candidate = f"{host[0].strip()}:{port[0].strip()}"
                proxy_url = self._normalize_proxy_url(candidate)
                if proxy_url is not None and self._is_well_formed_proxy_url(proxy_url):
                    proxy_urls.add(proxy_url)
        return list(proxy_urls)

    def is_proxy_valid(self, proxy_url: str) -> bool:
        """Return True when proxy is reachable and changes apparent public IP."""
        try:
            response = curl_get(
                IP_CHECK_URL,
                proxy=proxy_url,
                timeout=self.validate_timeout_seconds,
                use_random_browser=True,
            )
            if not response.ok:
                return False
            proxy_ip = self._extract_ip_from_payload(response.json())
            if proxy_ip is None or self.local_public_ip is None:
                return False
            return proxy_ip != self.local_public_ip
        except Exception:
            return False
