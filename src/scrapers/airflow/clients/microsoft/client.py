"""Minimal Microsoft job search/details client using shared API schemas."""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING

import requests

from scrapers.airflow.clients.common.base import JobsClient
from scrapers.airflow.clients.common.errors import RetryableUpstreamError
from scrapers.airflow.clients.common.http_requests import build_get_url
from scrapers.airflow.clients.microsoft import parser
from scrapers.airflow.clients.microsoft.transport import MicrosoftTransport, require_mapping
from scrapers.airflow.clients.common.request_policy import RequestPolicy
from web.backend.schemas import GetJobDetailsResponse, GetJobsResponse, JobMetadata

if TYPE_CHECKING:
    from scrapers.proxy.proxy_management_client import ProxyManagementClient

MicrosoftJobDetailsResponseSchema = GetJobDetailsResponse


class MicrosoftJobsClient(JobsClient):
    BASE_URL = "https://apply.careers.microsoft.com"
    DOMAIN = "microsoft.com"
    PAGE_SIZE = 10
    SEARCH_POLICY_KEY = "search"
    DETAILS_POLICY_KEY = "details"
    SUPPORTED_FILTER_VALUES_BY_FILTER_TYPE: dict[str, tuple[str, ...]] = {}
    SUPPORTED_FILTER_TYPES = frozenset()

    def __init__(
        self,
        *,
        base_url: str,
        domain: str = DOMAIN,
        default_request_policy: RequestPolicy,
        endpoint_request_policies: Mapping[str, RequestPolicy] | None = None,
        proxy_management_client: "ProxyManagementClient",
    ) -> None:
        super().__init__(
            default_request_policy=default_request_policy,
            endpoint_request_policies=endpoint_request_policies,
        )
        self.base_url = base_url.rstrip("/")
        self.domain = domain
        self.transport = MicrosoftTransport(base_url=self.base_url, proxy_management_client=proxy_management_client)

    def get_jobs(self, *, page: int = 1) -> GetJobsResponse:
        if page < 1:
            raise ValueError("page must be >= 1")
        resolved_start = (page - 1) * self.PAGE_SIZE
        payload = self.transport.get_json(
            "/api/pcsx/search",
            params=[("domain", self.domain), ("start", str(resolved_start))],
            request_policy=self.get_request_policy(self.SEARCH_POLICY_KEY),
        )
        data = require_mapping(payload.get("data"), context="search.data")
        positions_raw = data.get("positions")
        if not isinstance(positions_raw, list):
            raise ValueError(
                "Unexpected Microsoft API payload for search.positions: expected array, "
                f"got {type(positions_raw).__name__}"
            )
        jobs: list[JobMetadata] = []
        for index, position in enumerate(positions_raw):
            if not isinstance(position, Mapping):
                raise ValueError(
                    "Unexpected Microsoft API payload for search.positions"
                    f"[{index}]: expected object, got {type(position).__name__}"
                )
            jobs.append(parser.parse_job_metadata(payload=position, base_url=self.base_url))

        total_results = parser.to_int(data.get("count"))
        has_next_page = resolved_start + len(jobs) < total_results if isinstance(total_results, int) else len(jobs) == self.PAGE_SIZE
        return GetJobsResponse(
            status=payload.get("status", 200),
            error=payload.get("error"),
            jobs=jobs,
            pagination_index=page,
            has_next_page=has_next_page,
            positions=jobs,
            total_results=total_results,
            page_size=self.PAGE_SIZE,
        )

    def get_job_details(self, *, job_id: str) -> MicrosoftJobDetailsResponseSchema:
        position_id = job_id.strip()
        if not position_id:
            raise ValueError("job_id must be a non-empty string")
        detail_params = [("position_id", position_id), ("domain", self.domain), ("hl", "en")]
        details_url = build_get_url(
            base_url=self.base_url,
            path="/api/pcsx/position_details",
            params=detail_params,
        )

        try:
            payload = self.transport.get_json(
                "/api/pcsx/position_details",
                params=detail_params,
                request_policy=self.get_request_policy(self.DETAILS_POLICY_KEY),
            )
        except requests.exceptions.HTTPError as exc:
            status_code = exc.response.status_code if exc.response is not None else None
            if status_code == 404:
                return MicrosoftJobDetailsResponseSchema(
                    status=404,
                    error=f"Job '{position_id}' not found for company 'microsoft' url={details_url}",
                    job=None,
                )
            raise
        status = payload.get("status", 200)
        if status == 404:
            upstream_error = payload.get("error")
            return MicrosoftJobDetailsResponseSchema(
                status=404,
                error=(
                    f"Job '{position_id}' not found for company 'microsoft' url={details_url}"
                    if not upstream_error
                    else f"{upstream_error} url={details_url}"
                ),
                job=None,
            )

        data = require_mapping(payload.get("data"), context="position_details.data")
        if not data:
            raise RetryableUpstreamError(
                "Unexpected Microsoft API payload for position_details.data: "
                f"empty object for requested job id '{position_id}' url={details_url}"
            )
        return MicrosoftJobDetailsResponseSchema(
            status=status,
            error=payload.get("error"),
            job=parser.parse_job_details(payload=data),
        )
