"""Typed request policy for scraper HTTP calls."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import FrozenSet

DEFAULT_RETRYABLE_STATUS_CODES: FrozenSet[int] = frozenset({429, 500, 502, 503, 504})


@dataclass(frozen=True)
class RequestPolicy:
    """Timeout and retry settings for one request endpoint."""

    timeout_seconds: float
    max_retries: int
    backoff_factor: float = 0.5
    max_backoff_seconds: float = 6.0
    jitter: bool = False
    retryable_status_codes: FrozenSet[int] = field(default_factory=lambda: DEFAULT_RETRYABLE_STATUS_CODES)

