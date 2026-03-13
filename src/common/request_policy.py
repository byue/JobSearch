"""Typed request policy for shared HTTP calls."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class RequestPolicy:
    """Timeout and retry settings for one request endpoint."""

    timeout_seconds: float
    max_retries: int
    connect_timeout_seconds: float | None = None
    backoff_factor: float = 0.5
    max_backoff_seconds: float = 6.0
    jitter: bool = False

    def timeout_for_http(self) -> float | tuple[float, float]:
        if self.connect_timeout_seconds is None:
            return float(self.timeout_seconds)
        return (float(self.connect_timeout_seconds), float(self.timeout_seconds))
