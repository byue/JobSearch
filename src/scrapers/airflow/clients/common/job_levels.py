"""Helpers for normalizing job levels from job titles."""

from __future__ import annotations

import re

NORMALIZED_JOB_LEVEL_INTERN = "intern"
NORMALIZED_JOB_LEVEL_JUNIOR = "junior"
NORMALIZED_JOB_LEVEL_MID = "mid"
NORMALIZED_JOB_LEVEL_SENIOR = "senior"
NORMALIZED_JOB_LEVEL_STAFF = "staff"
NORMALIZED_JOB_LEVEL_PRINCIPAL = "principal"
NORMALIZED_JOB_LEVEL_DISTINGUISHED = "distinguished"
NORMALIZED_JOB_LEVEL_FELLOW = "fellow"
NORMALIZED_JOB_LEVEL_DIRECTOR = "director"
NORMALIZED_JOB_LEVEL_UNKNOWN = "unknown"
ALLOWED_JOB_LEVELS: frozenset[str] = frozenset(
    {
        NORMALIZED_JOB_LEVEL_INTERN,
        NORMALIZED_JOB_LEVEL_JUNIOR,
        NORMALIZED_JOB_LEVEL_MID,
        NORMALIZED_JOB_LEVEL_SENIOR,
        NORMALIZED_JOB_LEVEL_STAFF,
        NORMALIZED_JOB_LEVEL_PRINCIPAL,
        NORMALIZED_JOB_LEVEL_DISTINGUISHED,
        NORMALIZED_JOB_LEVEL_FELLOW,
        NORMALIZED_JOB_LEVEL_DIRECTOR,
        NORMALIZED_JOB_LEVEL_UNKNOWN,
    }
)

_GENERIC_LEVEL_RULES: tuple[tuple[str, tuple[re.Pattern[str], ...]], ...] = (
    (
        NORMALIZED_JOB_LEVEL_FELLOW,
        (
            re.compile(r"\bfellow\b", re.I),
        ),
    ),
    (
        NORMALIZED_JOB_LEVEL_DIRECTOR,
        (
            re.compile(r"\bdirector\b", re.I),
        ),
    ),
    (
        NORMALIZED_JOB_LEVEL_DISTINGUISHED,
        (
            re.compile(r"\bdistinguished\b", re.I),
        ),
    ),
    (
        NORMALIZED_JOB_LEVEL_PRINCIPAL,
        (
            re.compile(r"\bprincipal\b", re.I),
        ),
    ),
    (
        NORMALIZED_JOB_LEVEL_STAFF,
        (
            re.compile(r"\bstaff\b", re.I),
        ),
    ),
    (
        NORMALIZED_JOB_LEVEL_SENIOR,
        (
            re.compile(r"\bsenior\b", re.I),
            re.compile(r"\bsr\.?\b", re.I),
        ),
    ),
    (
        NORMALIZED_JOB_LEVEL_INTERN,
        (
            re.compile(r"\bintern(ship)?\b", re.I),
            re.compile(r"\bco[\s-]?op\b", re.I),
            re.compile(r"\bapprentice(ship)?\b", re.I),
        ),
    ),
    (
        NORMALIZED_JOB_LEVEL_JUNIOR,
        (
            re.compile(r"\bjunior\b", re.I),
            re.compile(r"\bjr\.?\b", re.I),
            re.compile(r"\bentry[\s-]?level\b", re.I),
            re.compile(r"\bnew[\s-]?grad\b", re.I),
        ),
    ),
    (
        NORMALIZED_JOB_LEVEL_MID,
        (
            re.compile(r"\bmid(?:[\s-]?level)?\b", re.I),
            re.compile(r"\bintermediate\b", re.I),
        ),
    ),
)

_COMPANY_SPECIFIC_LEVEL_RULES: dict[str, tuple[tuple[str, tuple[re.Pattern[str], ...]], ...]] = {
    "amazon": (
        (
            NORMALIZED_JOB_LEVEL_JUNIOR,
            (
                re.compile(r"\b(?:1|i)\b", re.I),
            ),
        ),
        (
            NORMALIZED_JOB_LEVEL_MID,
            (
                re.compile(r"\b(?:2|ii)\b", re.I),
            ),
        ),
        (
            NORMALIZED_JOB_LEVEL_SENIOR,
            (
                re.compile(r"\b(?:3|iii)\b", re.I),
            ),
        ),
    ),
    "apple": (),
    "google": (
        (
            NORMALIZED_JOB_LEVEL_JUNIOR,
            (
                re.compile(r"\b(?:2|ii)\b", re.I),
            ),
        ),
        (
            NORMALIZED_JOB_LEVEL_MID,
            (
                re.compile(r"\b(?:3|iii)\b", re.I),
            ),
        ),
        (
            NORMALIZED_JOB_LEVEL_SENIOR,
            (
                re.compile(r"\b(?:4|iv)\b", re.I),
            ),
        ),
    ),
    "meta": (),
    "microsoft": (
        (
            NORMALIZED_JOB_LEVEL_PRINCIPAL,
            (
                re.compile(r"\bic\s*(?:5|6|7)\b", re.I),
            ),
        ),
        (
            NORMALIZED_JOB_LEVEL_JUNIOR,
            (
                re.compile(r"\bic\s*2\b", re.I),
            ),
        ),
        (
            NORMALIZED_JOB_LEVEL_MID,
            (
                re.compile(r"\bic\s*3\b", re.I),
            ),
        ),
        (
            NORMALIZED_JOB_LEVEL_SENIOR,
            (
                re.compile(r"\bic\s*4\b", re.I),
            ),
        ),
        (
            NORMALIZED_JOB_LEVEL_JUNIOR,
            (
                re.compile(r"\b(?:1|i)\b", re.I),
            ),
        ),
        (
            NORMALIZED_JOB_LEVEL_MID,
            (
                re.compile(r"\b(?:2|ii)\b", re.I),
            ),
        ),
        (
            NORMALIZED_JOB_LEVEL_SENIOR,
            (
                re.compile(r"\b(?:3|iii)\b", re.I),
            ),
        ),
    ),
    "netflix": (
        (
            NORMALIZED_JOB_LEVEL_JUNIOR,
            (
                re.compile(r"(?<!/)\bl\s*3\b(?!\s*/)", re.I),
            ),
        ),
        (
            NORMALIZED_JOB_LEVEL_MID,
            (
                re.compile(r"(?<!/)\bl\s*4\b(?!\s*/)", re.I),
            ),
        ),
        (
            NORMALIZED_JOB_LEVEL_SENIOR,
            (
                re.compile(r"(?<!/)\bl\s*5\b(?!\s*/)", re.I),
            ),
        ),
    ),
}


def _matches_any(title: str, patterns: tuple[re.Pattern[str], ...]) -> bool:
    return any(pattern.search(title) for pattern in patterns)


def _normalize_title(job_title: str) -> str:
    return str(job_title or "").strip()


def _normalize_company_name(company_name: str | None) -> str:
    return str(company_name or "").strip().lower()


def _get_company_agnostic_job_level(normalized_title: str) -> str | None:
    for level, patterns in _GENERIC_LEVEL_RULES:
        if _matches_any(normalized_title, patterns):
            return level
    return None


def _get_company_specific_job_level(normalized_title: str, normalized_company_name: str) -> str | None:
    company_rules = _COMPANY_SPECIFIC_LEVEL_RULES.get(normalized_company_name)
    if company_rules is None:
        return None

    for level, patterns in company_rules:
        if _matches_any(normalized_title, patterns):
            return level
    return None


def get_normalized_job_level(job_title: str, company_name: str | None = None) -> str:
    """Normalize a title into a coarse job level."""
    normalized_title = _normalize_title(job_title)
    if not normalized_title:
        return NORMALIZED_JOB_LEVEL_MID

    generic_level = _get_company_agnostic_job_level(normalized_title)
    if generic_level is not None:
        return generic_level

    normalized_company_name = _normalize_company_name(company_name)
    company_specific_level = _get_company_specific_job_level(normalized_title, normalized_company_name)
    if company_specific_level is not None:
        return company_specific_level

    return NORMALIZED_JOB_LEVEL_MID
