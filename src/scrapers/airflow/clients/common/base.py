"""Abstract base client for company job integrations."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Mapping

from scrapers.airflow.clients.common.request_policy import RequestPolicy

from web.backend.schemas import GetJobDetailsResponse, GetJobsResponse


class JobsClient(ABC):
    """Common interface each company jobs client must implement."""

    def __init__(
        self,
        *,
        default_request_policy: RequestPolicy,
        endpoint_request_policies: Mapping[str, RequestPolicy] | None = None,
    ) -> None:
        self.default_request_policy = default_request_policy
        self.endpoint_request_policies = dict(endpoint_request_policies or {})

    def get_request_policy(self, endpoint_key: str) -> RequestPolicy:
        return self.endpoint_request_policies.get(endpoint_key, self.default_request_policy)

    @abstractmethod
    def get_jobs(self, *, page: int = 1) -> GetJobsResponse:
        """Fetch one jobs page by 1-based index."""

    @abstractmethod
    def get_job_details(self, *, job_id: str) -> GetJobDetailsResponse:
        """Fetch details for one job id."""
