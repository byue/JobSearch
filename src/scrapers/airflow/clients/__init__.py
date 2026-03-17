"""Company-specific job clients."""

from __future__ import annotations

__all__ = [
    "JobsClient",
    "GoogleJobsClient",
    "MicrosoftJobsClient",
    "AmazonJobsClient",
    "AppleJobsClient",
    "NetflixJobsClient",
    "MetaJobsClient",
]


def __getattr__(name: str) -> object:
    if name == "AmazonJobsClient":
        from .amazon import AmazonJobsClient

        return AmazonJobsClient
    if name == "AppleJobsClient":
        from .apple import AppleJobsClient

        return AppleJobsClient
    if name == "GoogleJobsClient":
        from .google import GoogleJobsClient

        return GoogleJobsClient
    if name == "MetaJobsClient":
        from .meta import MetaJobsClient

        return MetaJobsClient
    if name == "MicrosoftJobsClient":
        from .microsoft import MicrosoftJobsClient

        return MicrosoftJobsClient
    if name == "NetflixJobsClient":
        from .netflix import NetflixJobsClient

        return NetflixJobsClient
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
