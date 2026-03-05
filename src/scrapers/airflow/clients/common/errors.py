"""Shared client-layer error types."""

from __future__ import annotations


class RetryableUpstreamError(ValueError):
    """Raised when upstream returned an ambiguous/invalid success payload."""

