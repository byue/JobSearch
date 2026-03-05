"""Company-specific job clients."""

from .amazon import AmazonJobsClient
from .apple import AppleJobsClient
from .common.base import JobsClient
from .google import GoogleJobsClient
from .meta import MetaJobsClient
from .microsoft import MicrosoftJobsClient
from .netflix import NetflixJobsClient

__all__ = [
    "JobsClient",
    "GoogleJobsClient",
    "MicrosoftJobsClient",
    "AmazonJobsClient",
    "AppleJobsClient",
    "NetflixJobsClient",
    "MetaJobsClient",
]
