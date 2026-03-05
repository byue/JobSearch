"""Amazon jobs client using the public search JSON endpoint."""

from __future__ import annotations

import urllib.parse
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

from scrapers.airflow.clients.amazon.parser import parse_job_details, parse_job_metadata, to_int, to_optional_str
from scrapers.airflow.clients.amazon.transport import AmazonTransport
from scrapers.airflow.clients.common.base import JobsClient
from scrapers.airflow.clients.common.errors import RetryableUpstreamError
from scrapers.airflow.clients.common.request_policy import RequestPolicy
from web.backend.schemas import GetJobDetailsResponse, GetJobsResponse, JobMetadata

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

    def __init__(
        self,
        *,
        base_url: str,
        default_request_policy: RequestPolicy,
        endpoint_request_policies: Mapping[str, RequestPolicy] | None = None,
        proxy_management_client: "ProxyManagementClient",
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
        for index, item in enumerate(jobs_raw):
            if not isinstance(item, Mapping):
                raise ValueError(
                    f"Unexpected Amazon API payload for search.jobs[{index}]: "
                    f"expected object, got {type(item).__name__}"
                )
            jobs.append(parse_job_metadata(payload=item, base_url=self.base_url))

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

    def get_job_details(
        self,
        *,
        job_id: str,
    ) -> AmazonJobDetailsResponseSchema:
        """Fetch detailed data for one Amazon job id."""
        normalized_job_id = job_id.strip()
        if not normalized_job_id:
            raise ValueError("job_id must be a non-empty string")
        detail_query_url = (
            f"{self.base_url}{self.SEARCH_PATH}?"
            f"{urllib.parse.urlencode([('job_id_icims[]', normalized_job_id), ('offset', '0'), ('result_limit', '1'), ('sort', 'relevant')])}"
        )

        payload = self.transport.get_json(
            self.SEARCH_PATH,
            params=[
                ("job_id_icims[]", normalized_job_id),
                ("offset", "0"),
                ("result_limit", "1"),
                ("sort", "relevant"),
            ],
            request_policy=self.get_request_policy(self.DETAILS_POLICY_KEY),
        )
        jobs_raw = payload.get("jobs")
        if not isinstance(jobs_raw, list):
            raise ValueError(
                "Unexpected Amazon API payload for detail.jobs: expected array, "
                f"got {type(jobs_raw).__name__}"
            )

        target_payload: Mapping[str, Any] | None = None
        for item in jobs_raw:
            if not isinstance(item, Mapping):
                continue
            candidate_id = to_optional_str(item.get("id_icims")) or to_optional_str(item.get("id"))
            if candidate_id == normalized_job_id:
                target_payload = item
                break

        if target_payload is None:
            raise RetryableUpstreamError(
                "Unexpected Amazon API payload for detail.jobs: "
                f"no matching job id '{normalized_job_id}' in successful response url={detail_query_url}"
            )

        return AmazonJobDetailsResponseSchema(
            status=200,
            error=None,
            job=parse_job_details(payload=target_payload),
        )
