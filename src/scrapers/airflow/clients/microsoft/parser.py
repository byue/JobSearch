"""Microsoft payload parsing helpers."""

from __future__ import annotations

import html
import re
from collections.abc import Mapping
from typing import Any

from scrapers.airflow.clients.common.pay import extract_pay_details_from_description
from web.backend.schemas import JobDetailsSchema, JobMetadata, Location


def string_list(value: Any) -> list[str]:
    if isinstance(value, str):
        normalized = value.strip()
        return [normalized] if normalized else []
    if not isinstance(value, list):
        return []
    return [item.strip() for item in value if isinstance(item, str) and item.strip()]


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


def normalize_url(url: Any) -> str | None:
    normalized = str(url or "").strip()
    if not normalized:
        return None
    if normalized.startswith("http://"):
        return f"https://{normalized[len('http://'):] }"
    return normalized


def dedupe(values: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out


def require_non_empty_string_field(payload: Mapping[str, Any], *, field: str, context: str) -> str:
    value = payload.get(field)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(
            f"Unexpected Microsoft API payload for {context}: missing required string field '{field}'"
        )
    return value.strip()


def parse_job_metadata(*, payload: Mapping[str, Any], base_url: str) -> JobMetadata:
    raw_job_id = payload.get("id")
    job_id = str(raw_job_id).strip()
    if not job_id:
        raise ValueError("Unexpected Microsoft API payload for job metadata: missing required field 'id'")
    name = require_non_empty_string_field(payload, field="name", context=f"job '{job_id}'")
    posted_ts = to_int(payload.get("postedTs"))
    if posted_ts is None:
        raise ValueError(
            f"Unexpected Microsoft API payload for job '{job_id}': missing required integer field 'postedTs'"
        )
    standardized_locations = string_list(payload.get("standardizedLocations"))

    return JobMetadata(
        id=job_id,
        name=name,
        company="microsoft",
        locations=to_locations(standardized_locations),
        postedTs=posted_ts,
        detailsUrl=build_details_url(
            position_url=payload.get("positionUrl"),
            public_url=payload.get("publicUrl"),
            job_id=job_id,
            base_url=base_url,
        ),
        applyUrl=build_apply_url(job_id=job_id, base_url=base_url),
    )


def parse_job_details(*, payload: Mapping[str, Any]) -> JobDetailsSchema:
    job_description = to_optional_str(payload.get("jobDescription"))

    minimum_qualifications = coerce_detail_list(payload.get("minimumQualifications"))
    minimum_qualifications.extend(coerce_detail_list(payload.get("qualification")))
    minimum_qualifications.extend(coerce_detail_list(payload.get("qualifications")))
    minimum_qualifications.extend(coerce_detail_list(payload.get("otherQualifications")))

    preferred_qualifications = coerce_detail_list(payload.get("preferredQualifications"))
    preferred_qualifications.extend(coerce_detail_list(payload.get("additionalPreferredQualifications")))
    responsibilities = coerce_detail_list(payload.get("responsibilities"))

    return JobDetailsSchema(
        jobDescription=job_description,
        minimumQualifications=dedupe(minimum_qualifications),
        preferredQualifications=dedupe(preferred_qualifications),
        responsibilities=dedupe(responsibilities),
        payDetails=extract_pay_details_from_description(job_description),
    )


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


def build_apply_url(*, job_id: str, base_url: str) -> str | None:
    normalized_job_id = job_id.strip()
    if not normalized_job_id:
        return None
    return f"{base_url}/careers/apply?pid={normalized_job_id}"


def build_details_url(*, position_url: Any = None, public_url: Any = None, job_id: str = "", base_url: str) -> str | None:
    normalized_position_url = normalize_url(position_url)
    if normalized_position_url:
        if normalized_position_url.startswith("https://"):
            return normalized_position_url
        if normalized_position_url.startswith("/"):
            return f"{base_url}{normalized_position_url}"
        return f"{base_url}/{normalized_position_url.lstrip('/')}"

    normalized_public_url = normalize_url(public_url)
    if normalized_public_url:
        return normalized_public_url

    normalized_job_id = job_id.strip()
    if normalized_job_id:
        return f"{base_url}/careers/job/{normalized_job_id}"
    return None


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
        fragments = re.split(r"(?:\n|•)+", text)
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
