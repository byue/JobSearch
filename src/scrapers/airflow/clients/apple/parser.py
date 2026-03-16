"""Apple payload parsing helpers."""

from __future__ import annotations

import datetime as dt
import json
import re
import urllib.parse
from collections.abc import Mapping
from typing import Any

from common.job_taxonomy import infer_job_category_from_title
from scrapers.airflow.clients.common.html_text import extract_text
from web.backend.schemas import JobDetailsSchema, JobMetadata, Location

_HYDRATION_PATTERN = re.compile(
    r'window\.__staticRouterHydrationData\s*=\s*JSON\.parse\("((?:\\.|[^"\\])*)"\);',
    re.DOTALL,
)


def to_optional_str(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = value.strip()
    return normalized or None


def to_int(value: Any) -> int | None:
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        normalized = value.strip()
        if normalized and normalized.lstrip("-").isdigit():
            return int(normalized)
    return None


def dedupe(values: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out


def require_mapping(value: Any, *, context: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"Unexpected Apple payload for {context}: expected object, got {type(value).__name__}")
    return value


def extract_hydration_payload(*, html_payload: str, context: str) -> Mapping[str, Any]:
    match = _HYDRATION_PATTERN.search(html_payload)
    if not match:
        raise ValueError(f"Unable to extract Apple hydration payload for {context}")

    encoded_payload = match.group(1)
    try:
        serialized_payload = json.loads(f'"{encoded_payload}"')
    except json.JSONDecodeError:
        serialized_payload = bytes(encoded_payload, "utf-8").decode("unicode_escape")

    parsed_payload = json.loads(serialized_payload)
    return require_mapping(parsed_payload, context=context)


def parse_job_metadata(*, payload: Mapping[str, Any], base_url: str, locale: str) -> JobMetadata:
    job_id = to_optional_str(payload.get("positionId")) or to_optional_str(payload.get("reqId"))
    if not job_id:
        raise ValueError("Unexpected Apple payload for job metadata: missing required field 'positionId'")

    name = to_optional_str(payload.get("postingTitle"))
    transformed_title = to_optional_str(payload.get("transformedPostingTitle")) or slugify_title(name or "job")

    return JobMetadata(
        id=job_id,
        name=name,
        company="apple",
        jobCategory=infer_job_category_from_title(title=name),
        locations=extract_locations(payload.get("locations")),
        postedTs=parse_posted_ts(payload.get("postDateInGMT")) or parse_posting_date(payload.get("postingDate")),
        detailsUrl=build_details_url(base_url=base_url, locale=locale, job_id=job_id, transformed_title=transformed_title),
        applyUrl=f"{base_url}/app/{locale}/apply/{urllib.parse.quote(job_id)}",
    )


def parse_job_details(*, payload: Mapping[str, Any]) -> JobDetailsSchema:
    job_description_parts = [
        extract_text(payload.get("postingTitle")),
        _format_section("Summary", extract_text(payload.get("jobSummary"))),
        _format_section("Description", extract_text(payload.get("description"))),
        _format_section("Minimum Qualifications", extract_text(payload.get("minimumQualifications"))),
        _format_section("Preferred Qualifications", extract_text(payload.get("preferredQualifications"))),
        _format_section("Responsibilities", extract_text(payload.get("responsibilities"))),
        extract_text(payload.get("eeoContent")),
    ]
    job_description = "\n\n".join(part for part in job_description_parts if part) or None

    return JobDetailsSchema(
        jobDescription=job_description,
    )


def _format_section(title: str, content: str | None) -> str | None:
    if not content:
        return None
    return f"{title}\n{content}"


def build_details_url(*, base_url: str, locale: str, job_id: str, transformed_title: str) -> str:
    encoded_job_id = urllib.parse.quote(job_id)
    encoded_title = urllib.parse.quote(transformed_title)
    return f"{base_url}/{locale}/details/{encoded_job_id}/{encoded_title}"


def extract_locations(value: Any) -> list[Location]:
    if not isinstance(value, list):
        return []

    out: list[Location] = []
    for item in value:
        if not isinstance(item, Mapping):
            continue
        city = to_optional_str(item.get("city")) or ""
        state = to_optional_str(item.get("stateProvince")) or to_optional_str(item.get("region")) or ""
        country = to_optional_str(item.get("countryName")) or ""
        fallback_name = to_optional_str(item.get("name"))
        if not country and fallback_name:
            country = fallback_name

        out.append(Location(city=city, state=state, country=country))
    return out


def parse_posting_date(value: Any) -> int | None:
    posted_date = to_optional_str(value)
    if not posted_date:
        return None

    normalized = re.sub(r"\s+", " ", posted_date)
    for fmt in ("%b %d, %Y", "%B %d, %Y"):
        try:
            parsed = dt.datetime.strptime(normalized, fmt).replace(tzinfo=dt.timezone.utc)
            return int(parsed.timestamp())
        except ValueError:
            continue
    return None


def parse_posted_ts(value: Any) -> int | None:
    posted_ts = to_optional_str(value)
    if not posted_ts:
        return None

    normalized = posted_ts.replace("Z", "+00:00")
    try:
        parsed = dt.datetime.fromisoformat(normalized)
    except ValueError:
        return None
    return int(parsed.timestamp())


def slugify_title(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-") or "job"
