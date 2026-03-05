"""HTTP client wrapper for internal proxy-api endpoints."""

from __future__ import annotations

import time
from typing import Any

import requests

from scrapers.proxy.lease_manager import LeaseState


class ProxyManagementClient:
    """Call proxy-api endpoints used by internal services."""

    def __init__(
        self,
        *,
        base_url: str,
        timeout_seconds: float,
        lease_acquire_timeout_seconds: float = 6.0,
        lease_poll_interval_seconds: float = 0.1,
    ) -> None:
        normalized_base_url = str(base_url).strip().rstrip("/")
        if not normalized_base_url:
            raise ValueError("base_url must be non-empty")
        self.base_url = normalized_base_url
        self.timeout_seconds = float(timeout_seconds)
        if self.timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be > 0")
        self.lease_acquire_timeout_seconds = float(lease_acquire_timeout_seconds)
        if self.lease_acquire_timeout_seconds <= 0:
            raise ValueError("lease_acquire_timeout_seconds must be > 0")
        self.lease_poll_interval_seconds = float(lease_poll_interval_seconds)
        if self.lease_poll_interval_seconds <= 0:
            raise ValueError("lease_poll_interval_seconds must be > 0")
        self._session = requests.Session()

    def _request(
        self,
        *,
        method: str,
        path: str,
        payload: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
    ) -> requests.Response:
        url = f"{self.base_url}{path}"
        response = self._session.request(
            method=method,
            url=url,
            json=payload,
            params=params,
            timeout=self.timeout_seconds,
        )
        return response

    @staticmethod
    def _require_scope(scope: str) -> str:
        normalized = str(scope).strip().lower()
        if not normalized:
            raise ValueError("scope must be non-empty")
        return normalized

    def health(self) -> bool:
        response = self._request(method="GET", path="/health")
        response.raise_for_status()
        payload = response.json()
        return isinstance(payload, dict) and payload.get("status") == "ok"

    def sizes(self, *, scope: str) -> dict[str, int]:
        params = {"scope": self._require_scope(scope)}
        response = self._request(method="GET", path="/sizes", params=params)
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            raise ValueError("Invalid sizes payload")
        return {
            "available": int(payload.get("available", 0)),
            "inuse": int(payload.get("inuse", 0)),
            "blocked": int(payload.get("blocked", 0)),
        }

    def lease(self, *, scope: str) -> tuple[str, str] | None:
        payload = {"scope": self._require_scope(scope)}
        response = self._request(method="POST", path="/lease", payload=payload)
        response.raise_for_status()
        payload = response.json()
        if payload is None:
            return None
        if not isinstance(payload, dict):
            raise ValueError("Invalid lease payload")
        resource = str(payload["resource"])
        token = str(payload["token"])
        return resource, token

    def release(self, resource: str, token: str, *, scope: str) -> bool:
        payload: dict[str, Any] = {"resource": resource, "token": token, "scope": self._require_scope(scope)}
        response = self._request(
            method="POST",
            path="/release",
            payload=payload,
        )
        response.raise_for_status()
        payload = response.json()
        return bool(payload.get("ok")) if isinstance(payload, dict) else False

    def block(self, resource: str, token: str, *, scope: str) -> bool:
        payload: dict[str, Any] = {"resource": resource, "token": token, "scope": self._require_scope(scope)}
        response = self._request(
            method="POST",
            path="/block",
            payload=payload,
        )
        response.raise_for_status()
        payload = response.json()
        return bool(payload.get("ok")) if isinstance(payload, dict) else False

    def try_enqueue(self, resource: str, capacity: int, *, scope: str) -> bool:
        payload: dict[str, Any] = {"resource": resource, "capacity": int(capacity), "scope": self._require_scope(scope)}
        response = self._request(
            method="POST",
            path="/try-enqueue",
            payload=payload,
        )
        response.raise_for_status()
        payload = response.json()
        return bool(payload.get("ok")) if isinstance(payload, dict) else False

    def get_state(self, resource: str, *, scope: str) -> LeaseState:
        params: dict[str, Any] = {"resource": resource, "scope": self._require_scope(scope)}
        response = self._request(
            method="GET",
            path="/state",
            params=params,
        )
        if response.status_code == 404:
            return LeaseState.MISSING
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            raise ValueError("Invalid state payload")
        state_name = str(payload.get("state", "MISSING"))
        try:
            return LeaseState[state_name]
        except KeyError:
            raise ValueError(f"Unknown lease state: {state_name!r}") from None

    def acquire_requests_proxy(self, *, scope: str) -> tuple[dict[str, str], str, str]:
        normalized_scope = self._require_scope(scope)
        deadline = time.monotonic() + self.lease_acquire_timeout_seconds
        while time.monotonic() < deadline:
            lease = self.lease(scope=normalized_scope)
            if lease is not None:
                resource, token = lease
                proxies = {"http": resource, "https": resource}
                return proxies, resource, token
            time.sleep(self.lease_poll_interval_seconds)
        raise requests.exceptions.ProxyError("No proxy available from proxy management API")

    def complete_requests_proxy(self, *, resource: str, token: str, success: bool, scope: str) -> bool:
        normalized_scope = self._require_scope(scope)
        if success:
            return self.release(resource, token, scope=normalized_scope)
        return self.block(resource, token, scope=normalized_scope)
