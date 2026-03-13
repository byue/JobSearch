"""Factory helpers for constructing company clients."""

from __future__ import annotations

from collections.abc import Mapping

from scrapers.airflow.clients.amazon import AmazonJobsClient
from scrapers.airflow.clients.apple import AppleJobsClient
from scrapers.airflow.clients.common.base import JobsClient
from scrapers.airflow.clients.google import GoogleJobsClient
from scrapers.airflow.clients.meta import MetaJobsClient
from scrapers.airflow.clients.microsoft import MicrosoftJobsClient
from scrapers.airflow.clients.netflix import NetflixJobsClient
from common.request_policy import RequestPolicy
from scrapers.proxy.proxy_management_client import ProxyManagementClient


def _resolve_endpoint_policies(
    *,
    company: str,
    default_request_policy: RequestPolicy,
    endpoint_request_policies: Mapping[str, RequestPolicy] | None,
) -> dict[str, RequestPolicy]:
    return dict(endpoint_request_policies or {})


def build_client(
    *,
    company: str,
    proxy_management_client: ProxyManagementClient,
    default_request_policy: RequestPolicy,
    endpoint_request_policies: Mapping[str, RequestPolicy] | None = None,
) -> JobsClient:
    resolved_endpoint_policies = _resolve_endpoint_policies(
        company=company,
        default_request_policy=default_request_policy,
        endpoint_request_policies=endpoint_request_policies,
    )
    if company == "amazon":
        return AmazonJobsClient(
            base_url=AmazonJobsClient.BASE_URL,
            default_request_policy=default_request_policy,
            endpoint_request_policies=resolved_endpoint_policies,
            proxy_management_client=proxy_management_client,
        )
    if company == "apple":
        return AppleJobsClient(
            base_url=AppleJobsClient.BASE_URL,
            default_request_policy=default_request_policy,
            endpoint_request_policies=resolved_endpoint_policies,
            proxy_management_client=proxy_management_client,
        )
    if company == "google":
        return GoogleJobsClient(
            base_url=GoogleJobsClient.BASE_URL,
            default_request_policy=default_request_policy,
            endpoint_request_policies=resolved_endpoint_policies,
            proxy_management_client=proxy_management_client,
        )
    if company == "meta":
        return MetaJobsClient(
            base_url=MetaJobsClient.BASE_URL,
            default_request_policy=default_request_policy,
            endpoint_request_policies=resolved_endpoint_policies,
            proxy_management_client=proxy_management_client,
        )
    if company == "microsoft":
        return MicrosoftJobsClient(
            base_url=MicrosoftJobsClient.BASE_URL,
            default_request_policy=default_request_policy,
            endpoint_request_policies=resolved_endpoint_policies,
            proxy_management_client=proxy_management_client,
        )
    if company == "netflix":
        return NetflixJobsClient(
            base_url=NetflixJobsClient.BASE_URL,
            default_request_policy=default_request_policy,
            endpoint_request_policies=resolved_endpoint_policies,
            proxy_management_client=proxy_management_client,
        )
    raise ValueError(f"Unsupported company: {company}")
