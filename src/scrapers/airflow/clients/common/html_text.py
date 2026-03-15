"""Shared HTML-to-text helpers for job detail extraction."""

from __future__ import annotations

from typing import Any

import trafilatura


def extract_text(value: Any, *, full_document: bool = False) -> str | None:
    """Convert HTML or plain text into extracted text."""
    if not isinstance(value, str):
        return None

    raw = value.strip()
    if not raw:
        return None

    html_input = raw if full_document else f"<html><body>{raw}</body></html>"
    try:
        return trafilatura.extract(
            html_input,
            include_comments=False,
            include_tables=True,
            no_fallback=False,
        )
    except Exception:
        return None
