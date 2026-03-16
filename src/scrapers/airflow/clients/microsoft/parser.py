"""Microsoft payload parsing helpers."""

from __future__ import annotations

import copy
from collections.abc import Mapping
from typing import Any

from lxml import html as lxml_html

from scrapers.airflow.clients.common.html_text import extract_text
from common.job_taxonomy import infer_job_category_from_title
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


def parse_job_metadata(
    *,
    payload: Mapping[str, Any],
    base_url: str,
    locations: list[Location] | None = None,
) -> JobMetadata:
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

    return JobMetadata(
        id=job_id,
        name=name,
        company="microsoft",
        jobCategory=infer_job_category_from_title(title=name),
        locations=list(locations or []),
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
    title = to_optional_str(payload.get("name"))
    job_description = render_job_description(payload.get("jobDescription"))
    if job_description is None:
        job_description = build_job_description(payload=payload)
    job_description = prepend_title(title=title, body=job_description)

    return JobDetailsSchema(
        jobDescription=job_description,
    )


def build_job_description(*, payload: Mapping[str, Any]) -> str | None:
    sections = [
        _normalize_text(payload.get("jobDescription")),
        _format_section("Responsibilities", payload.get("responsibilities")),
        _format_section("Required Qualifications", payload.get("requiredQualifications")),
        _format_section("Minimum Qualifications", payload.get("minimumQualifications")),
        _format_section("Other Requirements", payload.get("otherRequirements")),
        _format_section("Preferred Qualifications", payload.get("preferredQualifications")),
        _normalize_text(payload.get("jobQualifications")),
        _normalize_text(payload.get("qualification")),
    ]
    parts = [section for section in sections if section]
    return "\n\n".join(parts) if parts else None


def prepend_title(*, title: str | None, body: str | None) -> str | None:
    if title and body:
        if body.startswith(title):
            return body
        return f"{title}\n\n{body}"
    return title or body


def _format_section(title: str, value: Any) -> str | None:
    body = _normalize_text(value)
    if not body:
        return None
    return f"{title}\n{body}"


def _normalize_text(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    raw = value.strip()
    if not raw:
        return None
    extracted = extract_text(raw)
    normalized = extracted if isinstance(extracted, str) and extracted.strip() else raw
    return normalized.strip() or None


def _normalize_inline_text(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = value.replace("\xa0", " ").strip()
    return normalized or None


def render_job_description(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    raw = value.strip()
    if not raw:
        return None

    try:
        fragments = lxml_html.fragments_fromstring(raw)
    except Exception:
        return None

    sections: list[str] = []
    current_heading: str | None = None
    current_blocks: list[str] = []

    for child in fragments:
        if isinstance(child, str):
            rendered = _normalize_inline_text(child)
            if rendered:
                current_blocks.append(rendered)
            continue
        rendered = _render_child_block(child)
        if not rendered:
            tail = _normalize_inline_text(getattr(child, "tail", None))
            if tail:
                current_blocks.append(tail)
            continue
        if _is_section_heading(child):
            if current_heading or current_blocks:
                sections.append(_join_section(current_heading, current_blocks))
            current_heading = rendered
            current_blocks = []
            tail = _normalize_inline_text(getattr(child, "tail", None))
            if tail:
                current_blocks.append(tail)
            continue
        current_blocks.append(rendered)
        tail = _normalize_inline_text(getattr(child, "tail", None))
        if tail:
            current_blocks.append(tail)

    if current_heading or current_blocks:
        sections.append(_join_section(current_heading, current_blocks))

    body = "\n\n".join(section for section in sections if section).strip()
    return body or None


def _is_section_heading(node: object) -> bool:
    if not hasattr(node, "tag"):
        return False
    tag = node.tag.lower() if isinstance(node.tag, str) else ""
    if tag not in {"b", "strong"}:
        return False
    return len(node) == 0


def _join_section(heading: str | None, blocks: list[str]) -> str:
    body = "\n\n".join(block for block in blocks if block).strip()
    body = body.replace(":\n\n", ":\n")
    if heading and body:
        return f"{heading}\n{body}"
    if heading:
        return heading
    return body


def _render_child_block(node: object) -> str | None:
    if not hasattr(node, "tag"):
        return None
    tag = node.tag.lower() if isinstance(node.tag, str) else ""
    if tag in {"div"}:
        blocks = [_render_child_block(child) for child in node if isinstance(getattr(child, "tag", None), str)]
        body = "\n\n".join(block for block in blocks if block).strip()
        return body or None
    if tag == "ul":
        items = [_render_child_block(child) for child in node if isinstance(getattr(child, "tag", None), str)]
        body = "\n".join(item for item in items if item).strip()
        return body or None

    cloned = copy.deepcopy(node)
    if hasattr(cloned, "xpath"):
        for br in cloned.xpath(".//br"):
            br.tail = f"\n{br.tail}" if br.tail else "\n"

    text = cloned.text_content().replace("\r\n", "\n").replace("\r", "\n")
    lines = [line.strip() for line in text.splitlines()]
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


def clean_html_fragment(value: str) -> str:
    return extract_text(value) or ""
