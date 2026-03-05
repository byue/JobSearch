"""Google Careers client using shared API schemas."""

from __future__ import annotations

import urllib.parse
from collections.abc import Mapping
from typing import TYPE_CHECKING

import requests

from scrapers.airflow.clients.common.base import JobsClient
from scrapers.airflow.clients.google import parser
from scrapers.airflow.clients.google.transport import GoogleTransport
from scrapers.airflow.clients.common.request_policy import RequestPolicy
from web.backend.schemas import GetJobDetailsResponse, GetJobsResponse, JobMetadata

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
    ) -> None:
        super().__init__(
            default_request_policy=default_request_policy,
            endpoint_request_policies=endpoint_request_policies,
        )
        self.base_url = base_url.rstrip("/")
        self.transport = GoogleTransport(base_url=self.base_url, proxy_management_client=proxy_management_client)

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
        jobs = [
            parser.parse_job_metadata(row=row, page=page, base_url=self.base_url, results_path=self.RESULTS_PATH)
            for row in rows
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
                    job=None,
                )
            raise
        row = parser.extract_row_from_ds0(html_payload)
        if row is None:
            raise ValueError(
                f"Unexpected Google payload for direct job page: unable to find structured job row for id '{target_id}'"
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
            job=parser.parse_job_details(row=row),
        )
