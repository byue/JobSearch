"""Apple jobs client using search-page hydration and details JSON."""

from __future__ import annotations

import urllib.parse
from collections.abc import Mapping
from typing import TYPE_CHECKING

from scrapers.airflow.clients.apple import parser
from scrapers.airflow.clients.apple.transport import AppleTransport
from scrapers.airflow.clients.common.base import JobsClient
from common.request_policy import RequestPolicy
from web.backend.schemas import GetJobDetailsResponse, GetJobsResponse, JobMetadata

if TYPE_CHECKING:
    from scrapers.proxy.proxy_management_client import ProxyManagementClient

AppleJobDetailsResponseSchema = GetJobDetailsResponse


class AppleJobsClient(JobsClient):
    """Client for Apple Careers public search/details endpoints."""

    BASE_URL = "https://jobs.apple.com"
    DEFAULT_LOCALE = "en-us"
    PAGE_SIZE = 20
    SEARCH_POLICY_KEY = "search"
    DETAILS_POLICY_KEY = "details"

    def __init__(
        self,
        *,
        base_url: str,
        locale: str = DEFAULT_LOCALE,
        default_request_policy: RequestPolicy,
        endpoint_request_policies: Mapping[str, RequestPolicy] | None = None,
        proxy_management_client: "ProxyManagementClient",
    ) -> None:
        super().__init__(
            default_request_policy=default_request_policy,
            endpoint_request_policies=endpoint_request_policies,
        )
        self.base_url = base_url.rstrip("/")
        self.locale = locale.strip().lower() or self.DEFAULT_LOCALE
        self.transport = AppleTransport(
            base_url=self.base_url,
            proxy_management_client=proxy_management_client,
        )

    def get_jobs(self, *, page: int = 1) -> GetJobsResponse:
        if page < 1:
            raise ValueError("page must be >= 1")

        html_payload = self.transport.get_html(
            path=f"/{self.locale}/search",
            params=[("page", str(page))],
            request_policy=self.get_request_policy(self.SEARCH_POLICY_KEY),
        )
        payload = parser.extract_hydration_payload(html_payload=html_payload, context="search")
        loader_data = parser.require_mapping(payload.get("loaderData"), context="search.loaderData")
        search_data = parser.require_mapping(loader_data.get("search"), context="search.loaderData.search")

        results_raw = search_data.get("searchResults")
        if not isinstance(results_raw, list):
            raise ValueError(
                "Unexpected Apple payload for search.searchResults: expected array, "
                f"got {type(results_raw).__name__}"
            )

        jobs: list[JobMetadata] = []
        for index, item in enumerate(results_raw):
            if not isinstance(item, Mapping):
                raise ValueError(
                    f"Unexpected Apple payload for search.searchResults[{index}]: "
                    f"expected object, got {type(item).__name__}"
                )
            jobs.append(parser.parse_job_metadata(payload=item, base_url=self.base_url, locale=self.locale))

        total_results = parser.to_int(search_data.get("totalRecords"))
        has_next_page = page * self.PAGE_SIZE < total_results if isinstance(total_results, int) else len(jobs) == self.PAGE_SIZE

        return GetJobsResponse(
            status=200,
            error=None,
            jobs=jobs,
            pagination_index=page,
            has_next_page=has_next_page,
            positions=jobs,
            total_results=total_results,
            page_size=self.PAGE_SIZE,
            page=page,
        )

    def get_job_details(self, *, job_id: str) -> AppleJobDetailsResponseSchema:
        normalized_job_id = job_id.strip()
        if not normalized_job_id:
            raise ValueError("job_id must be a non-empty string")
        encoded_job_id = urllib.parse.quote(normalized_job_id)
        details_url = f"{self.base_url}/{self.locale}/details/{encoded_job_id}"
        payload = self.transport.get_json(
            path=f"/api/v1/jobDetails/{encoded_job_id}",
            params=[],
            request_policy=self.get_request_policy(self.DETAILS_POLICY_KEY),
        )
        details_payload = parser.require_mapping(payload.get("res"), context="details.res")
        job_details = parser.parse_job_details(payload=details_payload)
        if not job_details.jobDescription:
            return AppleJobDetailsResponseSchema(
                status=404,
                error=f"Job '{normalized_job_id}' not found for company 'apple' url={details_url}",
                jobDescription=None,
                detailsUrl=details_url,
            )

        return AppleJobDetailsResponseSchema(
            status=200,
            error=None,
            jobDescription=job_details.jobDescription,
            detailsUrl=details_url,
        )
