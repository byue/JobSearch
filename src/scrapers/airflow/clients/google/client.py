"""Google Careers client using shared API schemas."""

from __future__ import annotations

import urllib.parse
from collections.abc import Mapping
from typing import TYPE_CHECKING

import requests

from features.client import FeaturesClient
from scrapers.airflow.clients.common.base import JobsClient
from scrapers.airflow.clients.google import parser
from scrapers.airflow.clients.google.transport import GoogleTransport
from common.request_policy import RequestPolicy
from web.backend.schemas import GetJobDetailsResponse, GetJobsResponse, Location

if TYPE_CHECKING:
    from scrapers.proxy.proxy_management_client import ProxyManagementClient

GoogleJobDetailsResponseSchema = GetJobDetailsResponse


class GoogleJobsClient(JobsClient):
    BASE_URL = "https://www.google.com"
    RESULTS_PATH = "/about/careers/applications/jobs/results/"
    SEARCH_POLICY_KEY = "search"
    DETAILS_POLICY_KEY = "details"

    def __init__(
        self,
        *,
        base_url: str,
        default_request_policy: RequestPolicy,
        endpoint_request_policies: Mapping[str, RequestPolicy] | None = None,
        proxy_management_client: "ProxyManagementClient",
        features_client: FeaturesClient | None = None,
    ) -> None:
        super().__init__(
            default_request_policy=default_request_policy,
            endpoint_request_policies=endpoint_request_policies,
        )
        self.base_url = base_url.rstrip("/")
        self.transport = GoogleTransport(base_url=self.base_url, proxy_management_client=proxy_management_client)
        self.features_client = features_client

    def get_jobs(self, *, page: int = 1) -> GetJobsResponse:
        if page < 1:
            raise ValueError("page must be >= 1")
        params: list[tuple[str, str]] = []
        if page != 1:
            params.append(("page", str(page)))

        html_payload = self.transport.get_html(
            path=self.RESULTS_PATH,
            params=params,
            request_policy=self.get_request_policy(self.SEARCH_POLICY_KEY),
        )
        rows, total_results, page_size = parser.extract_rows(html_payload)
        raw_location_batches = [parser.extract_locations(parser.get(row, 9)) for row in rows]
        normalized_locations_by_job = self._normalize_locations(raw_location_batches)
        jobs = [
            parser.parse_job_metadata(
                row=row,
                page=page,
                base_url=self.base_url,
                results_path=self.RESULTS_PATH,
                locations=locations,
            )
            for row, locations in zip(rows, normalized_locations_by_job)
        ]
        return GetJobsResponse(
            status=200,
            error=None,
            jobs=jobs,
            pagination_index=page,
            has_next_page=parser.has_next_page(
                page=page,
                jobs_count=len(jobs),
                total_results=total_results,
                page_size=page_size,
            ),
            positions=jobs,
            total_results=total_results,
            page_size=page_size,
            page=page,
        )

    def _normalize_locations(self, raw_location_batches: list[list[str]]) -> list[list[Location]]:
        if not raw_location_batches:
            return []
        if self.features_client is None:
            return [[] for _ in raw_location_batches]

        flattened = [value for batch in raw_location_batches for value in batch]
        if not flattened:
            return [[] for _ in raw_location_batches]

        payload = self.features_client.normalize_locations(locations=flattened)
        raw_locations = payload.get("locations")
        if not isinstance(raw_locations, list):
            raise ValueError("Invalid normalize_locations payload")

        normalized_flat: list[Location] = []
        for item in raw_locations:
            if not isinstance(item, Mapping):
                raise ValueError("Invalid normalized location item")
            normalized_flat.append(
                Location(
                    city=str(item.get("city", "") or "").strip(),
                    state=str(item.get("region", "") or "").strip(),
                    country=str(item.get("country", "") or "").strip(),
                )
            )

        if len(normalized_flat) != len(flattened):
            raise ValueError("Normalized location count mismatch")

        out: list[list[Location]] = []
        offset = 0
        for batch in raw_location_batches:
            count = len(batch)
            out.append(normalized_flat[offset : offset + count])
            offset += count
        return out

    def get_job_details(self, *, job_id: str) -> GoogleJobDetailsResponseSchema:
        target_id = job_id.strip()
        if not target_id:
            raise ValueError("job_id must be a non-empty string")
        path = f"{self.RESULTS_PATH}{urllib.parse.quote(target_id)}-job"
        details_url = f"{self.base_url}{path}"
        try:
            html_payload = self.transport.get_html(
                path=path,
                params=[],
                request_policy=self.get_request_policy(self.DETAILS_POLICY_KEY),
            )
        except requests.exceptions.HTTPError as exc:
            status_code = exc.response.status_code if exc.response is not None else None
            if status_code == 404:
                return GoogleJobDetailsResponseSchema(
                    status=404,
                    error=f"Job '{target_id}' not found for company 'google' on direct job page url={details_url}",
                    jobDescription=None,
                    detailsUrl=details_url,
                )
            raise
        row = parser.extract_row_from_ds0(html_payload)
        if row is None:
            return GoogleJobDetailsResponseSchema(
                status=404,
                error=f"Job '{target_id}' not found for company 'google' on direct job page url={details_url}",
                jobDescription=None,
                detailsUrl=details_url,
            )
        row_job_id = parser.as_str(parser.get(row, 0))
        if row_job_id != target_id:
            raise ValueError(
                "Unexpected Google payload for direct job page: "
                f"requested id '{target_id}' but found '{row_job_id}'"
            )
        return GoogleJobDetailsResponseSchema(
            status=200,
            error=None,
            jobDescription=parser.parse_job_details(row=row).jobDescription,
            detailsUrl=details_url,
        )
