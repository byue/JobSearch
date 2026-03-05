"""Amazon payload parsing helpers."""

from __future__ import annotations

import datetime as dt
import html
import json
import re
from collections.abc import Mapping
from typing import Any

from scrapers.airflow.clients.common.pay import extract_pay_details_from_description
from web.backend.schemas import JobDetailsSchema, JobMetadata, Location


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


def parse_job_metadata(*, payload: Mapping[str, Any], base_url: str) -> JobMetadata:
    job_id = to_optional_str(payload.get("id_icims")) or to_optional_str(payload.get("id"))
    if not job_id:
        raise ValueError("Unexpected Amazon API payload for job metadata: missing required field 'id_icims'")

    name = to_optional_str(payload.get("title"))
    details_url = build_details_url(
        job_path=payload.get("job_path"),
        job_id=job_id,
        base_url=base_url,
    )
    apply_url = build_apply_url(
        job_id=job_id,
        raw_apply_url=payload.get("url_next_step"),
        base_url=base_url,
    )
    posted_ts = parse_posted_ts(payload.get("posted_date"))

    return JobMetadata(
        id=job_id,
        name=name,
        company="amazon",
        locations=extract_locations(payload),
        postedTs=posted_ts,
        applyUrl=apply_url,
        detailsUrl=details_url,
    )


def parse_job_details(*, payload: Mapping[str, Any]) -> JobDetailsSchema:
    job_description = to_optional_str(payload.get("description")) or to_optional_str(
        payload.get("description_short")
    )
    minimum_qualifications = coerce_detail_list(payload.get("basic_qualifications"))
    preferred_qualifications = coerce_detail_list(payload.get("preferred_qualifications"))

    responsibilities_section = extract_section(
        job_description,
        heading="Key job responsibilities",
    )
    responsibilities = coerce_detail_list(responsibilities_section)

    return JobDetailsSchema(
        jobDescription=job_description,
        minimumQualifications=dedupe(minimum_qualifications),
        preferredQualifications=dedupe(preferred_qualifications),
        responsibilities=dedupe(responsibilities),
        payDetails=extract_pay_details_from_description(job_description),
    )


def build_details_url(*, job_path: Any, job_id: str, base_url: str) -> str | None:
    normalized_job_path = to_optional_str(job_path)
    if normalized_job_path:
        if normalized_job_path.startswith("http://") or normalized_job_path.startswith("https://"):
            return normalized_job_path
        if normalized_job_path.startswith("/"):
            return f"{base_url}{normalized_job_path}"
        return f"{base_url}/{normalized_job_path}"
    return f"{base_url}/en/jobs/{job_id}"


def build_apply_url(*, job_id: str, raw_apply_url: Any, base_url: str) -> str | None:
    normalized_job_id = job_id.strip()
    if not normalized_job_id:
        return None

    canonical = f"{base_url}/applicant/jobs/{normalized_job_id}/apply"
    normalized_raw = to_optional_str(raw_apply_url)
    if not normalized_raw:
        return canonical

    if "account.amazon.com/jobs/" in normalized_raw and normalized_raw.endswith("/apply"):
        return canonical
    if normalized_raw.startswith("/jobs/") and normalized_raw.endswith("/apply"):
        return canonical
    if normalized_raw.startswith("/applicant/jobs/") and normalized_raw.endswith("/apply"):
        return f"{base_url}{normalized_raw}"
    if normalized_raw.startswith("http://") or normalized_raw.startswith("https://"):
        return normalized_raw
    if normalized_raw.startswith("/"):
        return f"{base_url}{normalized_raw}"
    return normalized_raw


def parse_posted_ts(value: Any) -> int | None:
    posted_date = to_optional_str(value)
    if not posted_date:
        return None
    normalized = re.sub(r"\s+", " ", posted_date)
    for fmt in ("%B %d, %Y", "%b %d, %Y"):
        try:
            parsed = dt.datetime.strptime(normalized, fmt).replace(tzinfo=dt.timezone.utc)
            return int(parsed.timestamp())
        except ValueError:
            continue
    return None


def extract_locations(payload: Mapping[str, Any]) -> list[Location]:
    out: list[Location] = []
    raw_locations = payload.get("locations")

    if isinstance(raw_locations, list):
        for item in raw_locations:
            parsed_item: Mapping[str, Any] | None = None
            if isinstance(item, Mapping):
                parsed_item = item
            elif isinstance(item, str):
                try:
                    maybe_mapping = json.loads(item)
                except json.JSONDecodeError:
                    maybe_mapping = None
                if isinstance(maybe_mapping, Mapping):
                    parsed_item = maybe_mapping

            if not isinstance(parsed_item, Mapping):
                continue

            city = to_optional_str(parsed_item.get("city")) or ""
            state = (
                to_optional_str(parsed_item.get("normalizedStateName"))
                or to_optional_str(parsed_item.get("state"))
                or to_optional_str(parsed_item.get("region"))
                or ""
            )
            country = (
                to_optional_str(parsed_item.get("countryIso3a"))
                or to_optional_str(parsed_item.get("normalizedCountryCode"))
                or to_optional_str(parsed_item.get("normalizedCountryName"))
                or ""
            )
            out.append(Location(city=city, state=state, country=country))

    if out:
        return out

    city = to_optional_str(payload.get("city")) or ""
    state = to_optional_str(payload.get("state")) or ""
    country = to_optional_str(payload.get("country_code")) or ""
    fallback_location = to_optional_str(payload.get("location"))

    if not any((city, state, country)) and fallback_location:
        parts = [part.strip() for part in fallback_location.split(",") if part.strip()]
        if len(parts) == 1:
            country = parts[0]
        elif len(parts) == 2:
            country, city = parts
        elif len(parts) >= 3:
            city = parts[0]
            state = parts[1]
            country = ", ".join(parts[2:])

    if any((city, state, country)):
        return [Location(city=city, state=state, country=country)]
    return []


def coerce_detail_list(value: Any) -> list[str]:
    if isinstance(value, list):
        combined: list[str] = []
        for item in value:
            combined.extend(coerce_detail_list(item))
        return combined

    if not isinstance(value, str):
        return []

    raw = value.strip()
    if not raw:
        return []

    li_sections = re.findall(r"(?is)<li[^>]*>(.*?)</li>", raw)
    candidates = li_sections if li_sections else [raw]

    out: list[str] = []
    seen: set[str] = set()
    for candidate in candidates:
        text = clean_html_fragment(candidate)
        if not text:
            continue
        fragments = re.split(r"(?:\n|•|\- )+", text)
        for fragment in fragments:
            normalized = fragment.strip()
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            out.append(normalized)
    return out


def clean_html_fragment(value: str) -> str:
    normalized = (
        value.replace("\r\n", "\n")
        .replace("\r", "\n")
        .replace("<br/>", "\n")
        .replace("<br />", "\n")
        .replace("<br>", "\n")
        .replace("</p>", "\n")
        .replace("</li>", "\n")
    )
    normalized = re.sub(r"(?is)<[^>]+>", " ", normalized)
    normalized = html.unescape(normalized)
    normalized = re.sub(r"[ \t]+", " ", normalized)
    normalized = re.sub(r"\n{2,}", "\n", normalized)
    return normalized.strip()


def extract_section(value: str | None, *, heading: str) -> str | None:
    if not value:
        return None
    heading_pattern = re.escape(heading.strip())
    match = re.search(
        rf"(?is)<h[1-6][^>]*>\s*{heading_pattern}:?\s*</h[1-6]>\s*(.*?)(?=<h[1-6][^>]*>|$)",
        value,
    )
    if match:
        section = match.group(1).strip()
        return section or None
    return None
