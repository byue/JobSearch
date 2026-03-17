"""Shared client-layer modules."""

from __future__ import annotations

__all__ = ["JobsClient", "RequestPolicy", "RetryableUpstreamError", "get_normalized_job_level"]


def __getattr__(name: str) -> object:
    if name == "JobsClient":
        from .base import JobsClient

        return JobsClient
    if name == "RetryableUpstreamError":
        from .errors import RetryableUpstreamError

        return RetryableUpstreamError
    if name == "RequestPolicy":
        from .request_policy import RequestPolicy

        return RequestPolicy
    if name == "get_normalized_job_level":
        from .job_levels import get_normalized_job_level

        return get_normalized_job_level
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
