"""Cross-company role taxonomy normalization helpers."""

from __future__ import annotations

import re

NORMALIZED_JOB_CATEGORY_SOFTWARE_ENGINEERING = "software_engineer"
NORMALIZED_JOB_CATEGORY_MACHINE_LEARNING_ENGINEER = "machine_learning_engineer"
NORMALIZED_JOB_CATEGORY_DATA_SCIENTIST = "data_scientist"
NORMALIZED_JOB_CATEGORY_MANAGER = "manager"

ALLOWED_JOB_CATEGORIES: frozenset[str] = frozenset(
    {
        NORMALIZED_JOB_CATEGORY_MANAGER,
        NORMALIZED_JOB_CATEGORY_MACHINE_LEARNING_ENGINEER,
        NORMALIZED_JOB_CATEGORY_DATA_SCIENTIST,
        NORMALIZED_JOB_CATEGORY_SOFTWARE_ENGINEERING,
    }
)

_NON_PEOPLE_MANAGER_TITLE_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"\bprogram manager\b", re.I),
    re.compile(r"\bproduct manager\b", re.I),
    re.compile(r"\bproject manager\b", re.I),
    re.compile(r"\btechnical program manager\b", re.I),
    re.compile(r"\bprogram management\b", re.I),
)

_PEOPLE_MANAGER_TITLE_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"\bengineering manager\b", re.I),
    re.compile(r"\bsoftware engineering manager\b", re.I),
    re.compile(r"\bsoftware development manager\b", re.I),
    re.compile(r"\bmachine learning manager\b", re.I),
    re.compile(r"\bdata science manager\b", re.I),
    re.compile(r"\bresearch scientist manager\b", re.I),
    re.compile(r"\bmanager,\s*software engineering\b", re.I),
    re.compile(r"\bmanager,\s*machine learning\b", re.I),
    re.compile(r"\bmanager,\s*data science\b", re.I),
)

_MACHINE_LEARNING_ENGINEER_TITLE_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"\bmachine learning engineer\b", re.I),
    re.compile(r"\bmachine learning infrastructure engineer\b", re.I),
    re.compile(r"\bmachine learning\b.*\bengineer\b", re.I),
    re.compile(r"\bengineer\b.*\bmachine learning\b", re.I),
)

_DATA_SCIENTIST_TITLE_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"\bapplied scientist\b", re.I),
    re.compile(r"\bresearch scientist\b", re.I),
    re.compile(r"\bdata scientist\b", re.I),
)

_SOFTWARE_TITLE_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"\bsoftware engineer\b", re.I),
    re.compile(r"\bsoftware development engineer\b", re.I),
    re.compile(r"\bsoftware dev engineer\b", re.I),
    re.compile(r"\bsoftware developer\b", re.I),
    re.compile(r"\bdeveloper,\s*software\b", re.I),
    re.compile(r"\bdata engineer\b", re.I),
    re.compile(r"\bbackend engineer\b", re.I),
    re.compile(r"\bfrontend engineer\b", re.I),
    re.compile(r"\bfront end engineer\b", re.I),
    re.compile(r"\bfull stack engineer\b", re.I),
    re.compile(r"\bfull-stack engineer\b", re.I),
    re.compile(r"\bembedded software engineer\b", re.I),
    re.compile(r"\bfirmware engineer\b", re.I),
)


def _matches_any(title: str, patterns: tuple[re.Pattern[str], ...]) -> bool:
    return any(pattern.search(title) for pattern in patterns)


def _is_people_manager_title(title: str) -> bool:
    if _matches_any(title, _NON_PEOPLE_MANAGER_TITLE_PATTERNS):
        return False
    return _matches_any(title, _PEOPLE_MANAGER_TITLE_PATTERNS)


def infer_job_category_from_title(*, title: str | None) -> str | None:
    normalized_title = str(title or "").strip()
    if not normalized_title:
        return None
    if _is_people_manager_title(normalized_title):
        return NORMALIZED_JOB_CATEGORY_MANAGER
    if _matches_any(normalized_title, _MACHINE_LEARNING_ENGINEER_TITLE_PATTERNS):
        return NORMALIZED_JOB_CATEGORY_MACHINE_LEARNING_ENGINEER
    if _matches_any(normalized_title, _DATA_SCIENTIST_TITLE_PATTERNS):
        return NORMALIZED_JOB_CATEGORY_DATA_SCIENTIST
    if _matches_any(normalized_title, _SOFTWARE_TITLE_PATTERNS):
        return NORMALIZED_JOB_CATEGORY_SOFTWARE_ENGINEERING
    return None
