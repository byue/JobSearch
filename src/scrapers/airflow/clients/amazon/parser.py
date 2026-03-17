"""Amazon payload parsing helpers."""

from __future__ import annotations

import copy
import datetime as dt
import json
import urllib.parse
from collections.abc import Mapping
from typing import Any

from lxml import html as lxml_html

from scrapers.airflow.clients.common.html_text import extract_text
from scrapers.airflow.clients.common.job_levels import get_normalized_job_level
from common.job_taxonomy import (
    infer_job_category_from_title,
)
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


def parse_job_metadata(*, payload: Mapping[str, Any], base_url: str, locations: list[Location] | None = None) -> JobMetadata:
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
    job_type = infer_job_category_from_title(title=name)

    return JobMetadata(
        id=job_id,
        name=name,
        company="amazon",
        jobCategory=job_type,
        jobLevel=get_normalized_job_level(name or "", "amazon"),
        locations=list(locations or []),
        postedTs=posted_ts,
        applyUrl=apply_url,
        detailsUrl=details_url,
    )


def parse_job_details(*, payload: Mapping[str, Any]) -> JobDetailsSchema:
    raw_job_description = to_optional_str(payload.get("description")) or to_optional_str(
        payload.get("description_short")
    )
    job_description = extract_text(raw_job_description)

    return JobDetailsSchema(
        jobDescription=job_description,
    )


def build_details_url(*, job_path: Any, job_id: str, base_url: str) -> str | None:
    details_path = build_details_path(job_path=job_path, job_id=job_id)
    return f"{base_url}{details_path}"


def build_details_path(*, job_path: Any, job_id: str) -> str | None:
    normalized_job_path = to_optional_str(job_path)
    if normalized_job_path:
        if normalized_job_path.startswith("/"):
            return normalized_job_path
        if normalized_job_path.startswith("http://") or normalized_job_path.startswith("https://"):
            parsed = urllib.parse.urlsplit(normalized_job_path)
            path = parsed.path or "/"
            if parsed.query:
                return f"{path}?{parsed.query}"
            return path
        return f"/{normalized_job_path}"
    return f"/en/jobs/{job_id}"


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
    normalized = " ".join(posted_date.split())
    for fmt in ("%B %d, %Y", "%b %d, %Y"):
        try:
            parsed = dt.datetime.strptime(normalized, fmt).replace(tzinfo=dt.timezone.utc)
            return int(parsed.timestamp())
        except ValueError:
            continue
    return None


def extract_location_strings(payload: Mapping[str, Any]) -> list[str]:
    out: list[str] = []
    raw_locations = payload.get("locations")

    if isinstance(raw_locations, list):
        for item in raw_locations:
            if not isinstance(item, str):
                continue
            try:
                parsed = json.loads(item)
            except json.JSONDecodeError:
                continue
            if not isinstance(parsed, Mapping):
                continue
            normalized = to_optional_str(parsed.get("normalizedLocation"))
            if normalized:
                out.append(normalized)
    return out


def clean_html_fragment(value: str) -> str:
    return extract_text(value) or ""


def render_job_description(html_payload: str) -> str | None:
    try:
        document = lxml_html.fromstring(html_payload)
    except Exception:
        return None

    title = _first_text(document.xpath("//h1[contains(@class, 'title')]"))
    content_root = _first_node(document.xpath("//div[@id='job-detail-body']//div[contains(@class, 'content')]"))
    if content_root is None:
        return None

    rendered_sections: list[str] = []
    if title:
        rendered_sections.append(title)

    for section in content_root.xpath("./div[contains(@class, 'section')]"):
        heading = _first_text(section.xpath("./h2"))
        body_text = _render_section_body(section)
        if not heading and not body_text:
            continue
        if heading and body_text:
            rendered_sections.append(f"{heading}\n{body_text}")
        elif heading:
            rendered_sections.append(heading)
        elif body_text:
            rendered_sections.append(body_text)

    rendered = "\n\n".join(section for section in rendered_sections if section).strip()
    return rendered or None


def _first_node(nodes: list[object]) -> object | None:
    return nodes[0] if nodes else None


def _first_text(nodes: list[object]) -> str | None:
    if not nodes:
        return None
    node = nodes[0]
    if not hasattr(node, "itertext"):
        return None
    text = " ".join(part.strip() for part in node.itertext() if part.strip())
    return text or None


def _render_section_body(section: object) -> str | None:
    if not hasattr(section, "xpath"):
        return None

    chunks: list[str] = []
    for child in section:
        if not isinstance(getattr(child, "tag", None), str):
            continue
        if child.tag.lower() == "h2":
            continue
        text = _render_child_block(child)
        if text:
            chunks.append(text)

    rendered = "\n\n".join(chunk for chunk in chunks if chunk).strip()
    return rendered or None


def _render_child_block(node: object) -> str | None:
    if not hasattr(node, "tag"):
        return None
    tag = node.tag.lower() if isinstance(node.tag, str) else ""
    cloned = copy.deepcopy(node)
    for br in cloned.xpath(".//br"):
        br.tail = f"\n{br.tail}" if br.tail else "\n"

    text = cloned.text_content().replace("\r\n", "\n").replace("\r", "\n")
    lines = [line.strip() for line in text.splitlines()]
    if tag == "ul":
        body = "\n".join(line.lstrip("-•* ").strip() for line in lines if line)
        return body or None
    compact_lines: list[str] = []
    previous_blank = False
    for line in lines:
        if not line:
            if compact_lines and not previous_blank:
                compact_lines.append("")
            previous_blank = True
            continue
        compact_lines.append(line.lstrip("-•* ").strip())
        previous_blank = False
    body = "\n".join(compact_lines).strip()
    return body or None
