"""Shared company->proxy-scope mapping and helpers."""

from __future__ import annotations

DEFAULT_COMPANIES: tuple[str, ...] = ("amazon", "apple", "google", "meta", "microsoft", "netflix")

# Must match the effective request host each client uses.
COMPANY_SCOPE_MAP: dict[str, str] = {
    "amazon": "www.amazon.jobs",
    "apple": "jobs.apple.com",
    "google": "www.google.com",
    "meta": "www.metacareers.com",
    "microsoft": "apply.careers.microsoft.com",
    "netflix": "explore.jobs.netflix.net",
}


def resolve_companies(raw: str | None) -> list[str]:
    if raw is None:
        return list(DEFAULT_COMPANIES)
    parsed = [item.strip().lower() for item in raw.split(",") if item.strip()]
    if not parsed:
        return list(DEFAULT_COMPANIES)
    allowed = set(DEFAULT_COMPANIES)
    return [item for item in parsed if item in allowed]


def resolve_scopes(companies: list[str]) -> list[str]:
    ordered: list[str] = []
    seen: set[str] = set()
    for company in companies:
        scope = COMPANY_SCOPE_MAP.get(company)
        if scope and scope not in seen:
            seen.add(scope)
            ordered.append(scope)
    return ordered
