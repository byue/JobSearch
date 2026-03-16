"""HTTP client for the features service."""

from __future__ import annotations

from typing import Any

import backoff
import requests
from common.request_policy import RequestPolicy


class FeaturesClient:
    _NORMALIZE_LOCATIONS_BATCH_SIZE = 100

    def __init__(
        self,
        *,
        base_url: str,
        request_policy: RequestPolicy,
    ) -> None:
        normalized_base_url = str(base_url).strip().rstrip("/")
        if not normalized_base_url:
            raise ValueError("base_url must be non-empty")
        self.base_url = normalized_base_url
        self.request_policy = request_policy
        if float(self.request_policy.timeout_seconds) <= 0:
            raise ValueError("request_policy.timeout_seconds must be > 0")
        self._session = requests.Session()

    def _request(
        self,
        *,
        method: str,
        path: str,
        payload: dict[str, Any] | None = None,
    ) -> Any:
        attempts = max(int(self.request_policy.max_retries), 1)

        @backoff.on_exception(
            backoff.expo,
            requests.exceptions.RequestException,
            max_tries=attempts,
            factor=self.request_policy.backoff_factor,
            max_value=self.request_policy.max_backoff_seconds,
            jitter=backoff.full_jitter if self.request_policy.jitter else None,
        )
        def _do_request() -> Any:
            response = self._session.request(
                method=method,
                url=f"{self.base_url}{path}",
                json=payload,
                timeout=self.request_policy.timeout_for_http(),
            )
            response.raise_for_status()
            return response.json()

        return _do_request()

    def get_job_skills(self, *, text: str) -> dict[str, Any]:
        normalized_text = str(text).strip()
        if not normalized_text:
            raise ValueError("text must be non-empty")
        payload = self._request(
            method="POST",
            path="/job_skills",
            payload={"text": normalized_text},
        )
        if not isinstance(payload, dict):
            raise ValueError("Invalid job_skills payload")
        return payload

    def get_query_embedding(self, *, text: str) -> dict[str, Any]:
        normalized_text = str(text).strip()
        if not normalized_text:
            raise ValueError("text must be non-empty")
        payload = self._request(
            method="POST",
            path="/query_embedding",
            payload={"text": normalized_text},
        )
        if not isinstance(payload, dict):
            raise ValueError("Invalid query_embedding payload")
        return payload

    def normalize_locations(self, *, locations: list[str]) -> dict[str, Any]:
        normalized_locations = [str(location).strip() for location in locations if str(location).strip()]
        if not normalized_locations:
            raise ValueError("locations must contain at least one non-empty string")
        merged_locations: list[Any] = []
        for start in range(0, len(normalized_locations), self._NORMALIZE_LOCATIONS_BATCH_SIZE):
            chunk = normalized_locations[start : start + self._NORMALIZE_LOCATIONS_BATCH_SIZE]
            payload = self._request(
                method="POST",
                path="/normalize_locations",
                payload={"locations": chunk},
            )
            if not isinstance(payload, dict):
                raise ValueError("Invalid normalize_locations payload")
            raw_locations = payload.get("locations")
            if not isinstance(raw_locations, list):
                raise ValueError("Invalid normalize_locations payload")
            merged_locations.extend(raw_locations)
        return {
            "status": 200,
            "error": None,
            "locations": merged_locations,
        }
