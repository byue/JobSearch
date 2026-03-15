"""Google payload parsing helpers."""

from __future__ import annotations

import json
import re
import urllib.parse
from typing import Any

from scrapers.airflow.clients.common.html_text import extract_text
from web.backend.schemas import JobDetailsSchema, JobMetadata, Location

_DS1_PATTERN = re.compile(
    r"AF_initDataCallback\(\{key: 'ds:1', hash: '[^']+', data:(.*?), sideChannel: \{\}\}\);",
    re.DOTALL,
)
_DS0_PATTERN = re.compile(
    r"AF_initDataCallback\(\{key: 'ds:0', hash: '[^']+', data:(.*?), sideChannel: \{\}\}\);",
    re.DOTALL,
)


def extract_rows(html_payload: str) -> tuple[list[list[Any]], int | None, int | None]:
    data = extract_ds1_payload(html_payload)
    rows_raw = data[0] if isinstance(data, list) and len(data) >= 1 else []
    if not isinstance(rows_raw, list):
        rows_raw = []
    rows = [row for row in rows_raw if isinstance(row, list)]
    total_results = data[2] if isinstance(data, list) and len(data) >= 3 else None
    page_size = data[3] if isinstance(data, list) and len(data) >= 4 else None
    if not isinstance(total_results, int):
        total_results = None
    if not isinstance(page_size, int):
        page_size = None
    return rows, total_results, page_size


def extract_ds1_payload(html_payload: str) -> list[Any]:
    match = _DS1_PATTERN.search(html_payload)
    if not match:
        raise ValueError("Unable to extract Google Careers payload from HTML response")
    return json.loads(match.group(1))


def extract_row_from_ds0(html_payload: str) -> list[Any] | None:
    match = _DS0_PATTERN.search(html_payload)
    if not match:
        return None
    try:
        payload = json.loads(match.group(1))
    except json.JSONDecodeError:
        return None
    if not isinstance(payload, list) or not payload:
        return None
    row = payload[0]
    return row if isinstance(row, list) else None


def parse_job_metadata(*, row: list[Any], page: int, base_url: str, results_path: str) -> JobMetadata:
    job_id = as_str(get(row, 0))
    name = as_optional_str(get(row, 1))
    apply_url = as_optional_str(get(row, 2))
    standardized_locations = extract_locations(get(row, 9))
    posted_ts = extract_ts_seconds(get(row, 12)) or extract_ts_seconds(get(row, 13))

    return JobMetadata(
        id=job_id or None,
        name=name,
        company="google",
        locations=to_locations(standardized_locations),
        postedTs=posted_ts,
        applyUrl=apply_url,
        detailsUrl=build_public_url(job_id=job_id, name=name, page=page, base_url=base_url, results_path=results_path),
    )


def parse_job_details(*, row: list[Any]) -> JobDetailsSchema:
    parts = [
        as_optional_str(get(row, 1)),
        _format_section("About the job", extract_text(extract_html_text(get(row, 10)))),
        _format_qualifications(extract_text(extract_html_text(get(row, 4)))),
        _format_section("Responsibilities", _strip_list_markers(extract_text(extract_html_text(get(row, 3))))),
    ]
    job_description = "\n\n".join(part for part in parts if part) or None

    return JobDetailsSchema(
        jobDescription=job_description,
    )


def _format_section(title: str, content: str | None) -> str | None:
    if not content:
        return None
    return f"{title}\n{content}"


def _format_qualifications(content: str | None) -> str | None:
    if not content:
        return None

    normalized = content.strip()
    if not normalized:
        return None

    lines = [line.rstrip() for line in normalized.splitlines()]
    compact = "\n".join(line for line in lines if line.strip())

    preferred_pattern = re.compile(r"(?im)^Preferred qualifications:?\s*$")
    compact = preferred_pattern.sub("\nPreferred Qualifications", compact, count=1)

    minimum_pattern = re.compile(r"(?im)^Minimum qualifications:?\s*$")
    if minimum_pattern.search(compact):
        compact = minimum_pattern.sub("Minimum Qualifications", compact, count=1)
        return compact

    return f"Minimum Qualifications\n{compact}"


def _strip_list_markers(content: str | None) -> str | None:
    if not content:
        return None

    cleaned_lines: list[str] = []
    for line in content.splitlines():
        stripped = line.lstrip()
        for prefix in ("- ", "* ", "• "):
            if stripped.startswith(prefix):
                stripped = stripped[len(prefix) :]
                break
        cleaned_lines.append(stripped)

    normalized = "\n".join(cleaned_lines).strip()
    return normalized or None


def build_public_url(*, job_id: str, name: str | None, page: int, base_url: str, results_path: str) -> str:
    slug = "job"
    if name:
        slug = re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-") or "job"
    path = f"{results_path.rstrip('/')}/{job_id}-{slug}"
    if page > 1:
        path = f"{path}?page={page}"
    return urllib.parse.urljoin(f"{base_url}/", path.lstrip("/"))


def to_locations(locations: list[str]) -> list[Location]:
    out: list[Location] = []
    for location in locations:
        parts = [part.strip() for part in location.split(",") if part.strip()]
        city = ""
        state = ""
        country = ""
        if len(parts) == 1:
            country = parts[0]
        elif len(parts) == 2:
            state = parts[0]
            country = parts[1]
        elif len(parts) >= 3:
            city = parts[0]
            state = parts[1]
            country = ", ".join(parts[2:])
        out.append(Location(city=city, state=state, country=country))
    return out


def get(values: list[Any], index: int) -> Any:
    if index < 0 or index >= len(values):
        return None
    return values[index]


def as_optional_str(value: Any) -> str | None:
    return value if isinstance(value, str) else None


def as_str(value: Any) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, int):
        return str(value)
    return ""


def extract_locations(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    out: list[str] = []
    for item in value:
        if isinstance(item, str):
            out.append(item)
            continue
        if isinstance(item, list):
            label = as_optional_str(get(item, 0))
            if label:
                out.append(label)
    return out


def extract_ts_seconds(value: Any) -> int | None:
    if isinstance(value, int):
        return value
    if isinstance(value, list):
        first = get(value, 0)
        if isinstance(first, int):
            return first
    return None


def extract_html_text(value: Any) -> str | None:
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        raw_html = get(value, 1)
        if isinstance(raw_html, str):
            return raw_html
    return None


def clean_html_fragment(value: str) -> str:
    return extract_text(value) or ""


def has_next_page(*, page: int, jobs_count: int, total_results: int | None, page_size: int | None) -> bool:
    if isinstance(total_results, int) and isinstance(page_size, int) and page_size > 0:
        return page * page_size < total_results
    if isinstance(page_size, int) and page_size > 0:
        return jobs_count == page_size
    return jobs_count > 0
