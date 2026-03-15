"""Shared Elasticsearch HTTP client helpers."""

from __future__ import annotations

import json
from typing import Any

import backoff
import requests

from common.request_policy import RequestPolicy


class ElasticsearchClient:
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
        params: dict[str, Any] | None = None,
        ndjson_payload: str | None = None,
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
            headers: dict[str, str] = {}
            kwargs: dict[str, Any] = {
                "method": method,
                "url": f"{self.base_url}{path}",
                "params": params,
                "timeout": self.request_policy.timeout_for_http(),
            }
            if ndjson_payload is not None:
                headers["Content-Type"] = "application/x-ndjson"
                kwargs["data"] = ndjson_payload
            else:
                kwargs["json"] = payload
            if headers:
                kwargs["headers"] = headers
            response = self._session.request(**kwargs)
            response.raise_for_status()
            if not response.content:
                return None
            return response.json()

        return _do_request()

    def create_index(self, *, index_name: str, mapping: dict[str, Any]) -> dict[str, Any]:
        payload = {
            "mappings": mapping,
        }
        try:
            out = self._request(method="PUT", path=f"/{index_name}", payload=payload)
        except requests.exceptions.HTTPError as exc:
            response = exc.response
            if response is not None:
                try:
                    error_payload = response.json()
                except ValueError:
                    error_payload = None
                if isinstance(error_payload, dict):
                    error = error_payload.get("error")
                    if isinstance(error, dict) and error.get("type") == "resource_already_exists_exception":
                        return {"acknowledged": True, "already_exists": True}
            raise
        return out if isinstance(out, dict) else {}

    def bulk_index(self, *, index_name: str, docs: list[dict[str, Any]], refresh: bool = False) -> dict[str, Any]:
        lines: list[str] = []
        for item in docs:
            doc_id = str(item["_id"])
            source = item["_source"]
            lines.append(json.dumps({"index": {"_index": index_name, "_id": doc_id}}, separators=(",", ":")))
            lines.append(json.dumps(source, separators=(",", ":")))
        ndjson_payload = "\n".join(lines) + "\n"
        params = {"refresh": "true"} if refresh else None
        out = self._request(
            method="POST",
            path="/_bulk",
            params=params,
            ndjson_payload=ndjson_payload,
        )
        return out if isinstance(out, dict) else {}

    def count(self, *, index_name: str) -> int:
        out = self._request(method="GET", path=f"/{index_name}/_count", payload={"query": {"match_all": {}}})
        if not isinstance(out, dict):
            return 0
        count = out.get("count")
        return int(count) if isinstance(count, int) else 0

    def swap_alias(self, *, alias: str, index_name: str) -> dict[str, Any]:
        try:
            current = self._request(method="GET", path=f"/_alias/{alias}")
        except requests.exceptions.HTTPError as exc:
            response = exc.response
            if response is None or response.status_code != 404:
                raise
            current = {}
        actions: list[dict[str, Any]] = []
        if isinstance(current, dict):
            for existing_index in current.keys():
                actions.append({"remove": {"index": existing_index, "alias": alias}})
        actions.append({"add": {"index": index_name, "alias": alias}})
        out = self._request(method="POST", path="/_aliases", payload={"actions": actions})
        return out if isinstance(out, dict) else {}

    def search(self, *, index_name: str, body: dict[str, Any]) -> dict[str, Any]:
        out = self._request(method="POST", path=f"/{index_name}/_search", payload=body)
        return out if isinstance(out, dict) else {}
