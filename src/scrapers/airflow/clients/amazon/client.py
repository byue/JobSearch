"""Amazon jobs client using the public search JSON endpoint."""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING

import requests

from features.client import FeaturesClient
from scrapers.airflow.clients.amazon.parser import extract_location_strings, parse_job_metadata, render_job_description, to_int
from scrapers.airflow.clients.amazon.transport import AmazonTransport
from scrapers.airflow.clients.common.base import JobsClient
from scrapers.airflow.clients.common.html_text import extract_text
from common.request_policy import RequestPolicy
from web.backend.schemas import GetJobDetailsResponse, GetJobsResponse, JobMetadata, Location

if TYPE_CHECKING:
    from scrapers.proxy.proxy_management_client import ProxyManagementClient

AmazonJobDetailsResponseSchema = GetJobDetailsResponse


class AmazonJobsClient(JobsClient):
    """Client for Amazon Jobs search/details endpoints."""

    BASE_URL = "https://www.amazon.jobs"
    SEARCH_PATH = "/en/search.json"
    PAGE_SIZE = 10
    SEARCH_POLICY_KEY = "search"
    DETAILS_POLICY_KEY = "details"
    CATEGORY_FILTERS: tuple[str, ...] = ("software-development", "machine-learning-science")

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
        self.transport = AmazonTransport(
            base_url=self.base_url,
            proxy_management_client=proxy_management_client,
        )
        self.features_client = features_client

    def get_jobs(
        self,
        *,
        page: int = 1,
    ) -> GetJobsResponse:
        """Fetch one Amazon jobs page by 1-based index."""
        if page < 1:
            raise ValueError("page must be >= 1")
        resolved_offset = (page - 1) * self.PAGE_SIZE

        payload = self.transport.get_json(
            self.SEARCH_PATH,
            params=[
                ("offset", str(resolved_offset)),
                ("result_limit", str(self.PAGE_SIZE)),
                ("sort", "relevant"),
                *[("category[]", category) for category in self.CATEGORY_FILTERS],
            ],
            request_policy=self.get_request_policy(self.SEARCH_POLICY_KEY),
        )
        jobs_raw = payload.get("jobs")
        if not isinstance(jobs_raw, list):
            raise ValueError(
                "Unexpected Amazon API payload for search.jobs: expected array, "
                f"got {type(jobs_raw).__name__}"
            )

        jobs: list[JobMetadata] = []
        raw_location_batches: list[list[str]] = []
        for index, item in enumerate(jobs_raw):
            if not isinstance(item, Mapping):
                raise ValueError(
                    f"Unexpected Amazon API payload for search.jobs[{index}]: "
                    f"expected object, got {type(item).__name__}"
                )
            raw_location_batches.append(extract_location_strings(item))

        normalized_locations_by_job = self._normalize_locations(raw_location_batches)

        for item, locations in zip(jobs_raw, normalized_locations_by_job):
            jobs.append(
                parse_job_metadata(
                    payload=item,
                    base_url=self.base_url,
                    locations=locations,
                )
            )

        total_results = to_int(payload.get("hits"))
        if isinstance(total_results, int):
            has_next_page = resolved_offset + len(jobs) < total_results
        else:
            has_next_page = len(jobs) == self.PAGE_SIZE

        return GetJobsResponse(
            status=200,
            error=payload.get("error"),
            jobs=jobs,
            pagination_index=page,
            has_next_page=has_next_page,
            positions=jobs,
            total_results=total_results,
            page_size=self.PAGE_SIZE,
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

    def get_job_details(
        self,
        *,
        job_id: str,
    ) -> AmazonJobDetailsResponseSchema:
        """Fetch detailed data for one Amazon job id."""
        normalized_job_id = job_id.strip()
        if not normalized_job_id:
            raise ValueError("job_id must be a non-empty string")
        details_path = f"/en/jobs/{normalized_job_id}"
        error_url = f"{self.base_url}{details_path}"

        try:
            html_payload = self.transport.get_text(
                details_path,
                request_policy=self.get_request_policy(self.DETAILS_POLICY_KEY),
            )
        except requests.exceptions.HTTPError as exc:
            status_code = exc.response.status_code if exc.response is not None else None
            if status_code == 404:
                return AmazonJobDetailsResponseSchema(
                    status=404,
                    error=f"Job '{normalized_job_id}' not found for company 'amazon' url={error_url}",
                    jobDescription=None,
                    detailsUrl=error_url,
                )
            raise

        html_job_description = render_job_description(html_payload)
        if not html_job_description:
            html_job_description = extract_text(html_payload, full_document=True)
        if not html_job_description:
            return AmazonJobDetailsResponseSchema(
                status=404,
                error=f"Job '{normalized_job_id}' not found for company 'amazon' url={error_url}",
                jobDescription=None,
                detailsUrl=error_url,
            )

        return AmazonJobDetailsResponseSchema(
            status=200,
            error=None,
            jobDescription=html_job_description,
            detailsUrl=error_url,
        )
