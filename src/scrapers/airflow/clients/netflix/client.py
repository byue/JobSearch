"""Netflix jobs client using public Apply API plus job-page structured data."""

from __future__ import annotations

import html
import json
import logging
import re
import urllib.parse
from collections.abc import Iterable, Mapping
from typing import TYPE_CHECKING, Any

import requests

from scrapers.airflow.clients.common.base import JobsClient
from scrapers.airflow.clients.common.pay import extract_pay_details_from_description
from scrapers.airflow.clients.common.request_policy import RequestPolicy
from scrapers.airflow.clients.common.http_requests import build_get_url, request_json_with_backoff, request_text_with_backoff
from web.backend.schemas import GetJobDetailsResponse, GetJobsResponse, JobDetailsSchema, JobMetadata, Location

if TYPE_CHECKING:
    from scrapers.proxy.proxy_management_client import ProxyManagementClient

NetflixJobDetailsResponseSchema = GetJobDetailsResponse
LOGGER = logging.getLogger(__name__)


def _to_optional_str(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = value.strip()
    return normalized or None


def _to_int(value: Any) -> int | None:
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        normalized = value.strip()
        if normalized and normalized.lstrip("-").isdigit():
            return int(normalized)
    return None


def _dedupe(values: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out


def _require_mapping(value: Any, *, context: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(
            f"Unexpected Netflix API payload for {context}: expected object, got {type(value).__name__}"
        )
    return value


class NetflixJobsClient(JobsClient):
    """Client for Netflix jobs endpoints on `explore.jobs.netflix.net`."""

    BASE_URL = "https://explore.jobs.netflix.net"
    API_JOBS_PATH = "/api/apply/v2/jobs"
    CAREERS_PATH = "/careers"
    PAGE_SIZE = 10
    DOMAIN = "netflix.com"
    SEARCH_POLICY_KEY = "search"
    DETAILS_POLICY_KEY = "details"
    JOB_PAGE_POLICY_KEY = "job_page_html"

    _LD_JSON_BLOCK_PATTERN = re.compile(
        r'<script[^>]*type="application/ld\+json"[^>]*>(.*?)</script>',
        re.IGNORECASE | re.DOTALL,
    )
    _META_DESCRIPTION_PATTERN = re.compile(
        r'<meta\s+name="description"\s+content="([^"]*)"',
        re.IGNORECASE,
    )

    def __init__(
        self,
        *,
        base_url: str,
        domain: str = DOMAIN,
        default_request_policy: RequestPolicy,
        endpoint_request_policies: Mapping[str, RequestPolicy] | None = None,
        proxy_management_client: "ProxyManagementClient",
    ) -> None:
        super().__init__(
            default_request_policy=default_request_policy,
            endpoint_request_policies=endpoint_request_policies,
        )
        self.base_url = base_url.rstrip("/")
        self.domain = domain.strip() or self.DOMAIN
        self.proxy_management_client = proxy_management_client

    def get_jobs(
        self,
        *,
        page: int = 1,
    ) -> GetJobsResponse:
        """Fetch one Netflix jobs page by 1-based pagination index."""
        if page < 1:
            raise ValueError("page must be >= 1")

        resolved_start = (page - 1) * self.PAGE_SIZE
        payload = self._get_jobs_payload(
            params=[
                ("domain", self.domain),
                ("start", str(resolved_start)),
            ],
            request_policy=self.get_request_policy(self.SEARCH_POLICY_KEY),
        )

        positions_raw = payload.get("positions")
        if not isinstance(positions_raw, list):
            raise ValueError(
                "Unexpected Netflix API payload for positions: expected array, "
                f"got {type(positions_raw).__name__}"
            )

        jobs: list[JobMetadata] = []
        for index, item in enumerate(positions_raw):
            if not isinstance(item, Mapping):
                raise ValueError(
                    f"Unexpected Netflix API payload for positions[{index}]: "
                    f"expected object, got {type(item).__name__}"
                )
            jobs.append(self._parse_job_metadata(item))

        total_results = _to_int(payload.get("count"))
        if isinstance(total_results, int):
            has_next_page = resolved_start + len(jobs) < total_results
        else:
            has_next_page = len(jobs) == self.PAGE_SIZE

        return GetJobsResponse(
            status=200,
            error=payload.get("error"),
            jobs=jobs,
            pagination_index=page,
            has_next_page=has_next_page,
            positions=jobs,
            total_results=total_results,
            page_size=self.PAGE_SIZE,
        )

    def get_job_details(
        self,
        *,
        job_id: str,
    ) -> NetflixJobDetailsResponseSchema:
        """Fetch detailed data for one Netflix job id."""
        normalized_job_id = job_id.strip()
        if not normalized_job_id:
            raise ValueError("job_id must be a non-empty string")
        details_url = f"{self.base_url}{self.CAREERS_PATH}/job/{urllib.parse.quote(normalized_job_id)}"

        # Prefer the public job page first; it often contains richer structured content.
        try:
            page_job_description = self._extract_description_from_job_page(
                job_id=normalized_job_id,
                details_url=details_url,
            )
        except requests.exceptions.RequestException as exc:
            LOGGER.info(
                "netflix_job_page_fetch_failed job_id=%s url=%s error=%s",
                normalized_job_id,
                details_url,
                f"{type(exc).__name__}: {exc}",
            )
            page_job_description = None

        if page_job_description:
            return NetflixJobDetailsResponseSchema(
                status=200,
                error=None,
                job=self._build_job_details_from_description(page_job_description),
            )

        detail_query_url = build_get_url(
            base_url=self.base_url,
            path=self.API_JOBS_PATH,
            params=[
                ("domain", self.domain),
                ("pid", normalized_job_id),
            ],
        )

        try:
            payload = self._get_jobs_payload(
                params=[
                    ("domain", self.domain),
                    ("pid", normalized_job_id),
                ],
                request_policy=self.get_request_policy(self.DETAILS_POLICY_KEY),
            )
        except requests.exceptions.HTTPError as exc:
            status_code = exc.response.status_code if exc.response is not None else None
            if status_code == 404:
                return NetflixJobDetailsResponseSchema(
                    status=404,
                    error=f"Job '{normalized_job_id}' not found for company 'netflix' url={detail_query_url}",
                    job=None,
                )
            raise

        positions_raw = payload.get("positions")
        if not isinstance(positions_raw, list):
            raise ValueError(
                "Unexpected Netflix API payload for detail.positions: expected array, "
                f"got {type(positions_raw).__name__}"
            )

        target_payload: Mapping[str, Any] | None = None
        for item in positions_raw:
            if not isinstance(item, Mapping):
                continue
            candidate_id = self._to_job_id(item.get("id"))
            if candidate_id == normalized_job_id:
                target_payload = item
                break

        if target_payload is None:
            return NetflixJobDetailsResponseSchema(
                status=404,
                error=f"Job '{normalized_job_id}' not found for company 'netflix' url={detail_query_url}",
                job=None,
            )

        details_url = self._build_details_url(target_payload)

        api_job_description = self._extract_job_description(target_payload.get("job_description"))
        if not api_job_description:
            return NetflixJobDetailsResponseSchema(
                status=404,
                error=f"Job '{normalized_job_id}' not found for company 'netflix' url={details_url}",
                job=None,
            )

        return NetflixJobDetailsResponseSchema(status=200, error=None, job=self._build_job_details_from_description(api_job_description))

    def _build_job_details_from_description(self, job_description: str) -> JobDetailsSchema:
        minimum_html = self._extract_section(
            job_description,
            headings=(
                "required experience and skills",
                "required qualifications",
                "minimum qualifications",
                "qualifications",
                "what you will need",
                "you will be successful in this role if you have",
                "requirements",
            ),
        )
        preferred_html = self._extract_section(
            job_description,
            headings=(
                "nice to haves",
                "nice to have",
                "preferred experience and skills",
                "preferred qualifications",
                "bonus",
                "what would set you apart",
            ),
        )
        responsibilities_html = self._extract_section(
            job_description,
            headings=(
                "the role",
                "responsibilities",
                "what you'll do",
                "what you will do",
                "in this role",
            ),
        )

        minimum_qualifications = _dedupe(self._extract_list_items(minimum_html))
        preferred_qualifications = _dedupe(self._extract_list_items(preferred_html))
        responsibilities = _dedupe(self._extract_list_items(responsibilities_html))

        list_blocks = self._extract_html_list_blocks(job_description)
        if not minimum_qualifications and list_blocks:
            minimum_qualifications = _dedupe(self._extract_list_items(list_blocks[0]))
        if not preferred_qualifications and len(list_blocks) > 1:
            preferred_qualifications = _dedupe(self._extract_list_items(list_blocks[-1]))

        return JobDetailsSchema(
            jobDescription=job_description,
            minimumQualifications=minimum_qualifications,
            preferredQualifications=preferred_qualifications,
            responsibilities=responsibilities,
            payDetails=extract_pay_details_from_description(job_description),
        )

    def _get_jobs_payload(
        self,
        *,
        params: Iterable[tuple[str, str]],
        request_policy: RequestPolicy,
    ) -> dict[str, Any]:
        url = build_get_url(
            base_url=self.base_url,
            path=self.API_JOBS_PATH,
            params=params,
        )
        payload = request_json_with_backoff(
            url=url,
            headers={
                "Accept": "application/json",
                "Referer": f"{self.base_url}{self.CAREERS_PATH}",
            },
            request_policy=request_policy,
            proxy_management_client=self.proxy_management_client,
        )
        parsed_payload = _require_mapping(payload, context=self.API_JOBS_PATH)
        return dict(parsed_payload)

    def _parse_job_metadata(self, payload: Mapping[str, Any]) -> JobMetadata:
        job_id = self._to_job_id(payload.get("id"))
        if not job_id:
            raise ValueError("Unexpected Netflix API payload for job metadata: missing required field 'id'")

        name = _to_optional_str(payload.get("posting_name")) or _to_optional_str(payload.get("name"))
        details_url = self._build_details_url(payload)

        posted_ts = _to_int(payload.get("t_create")) or _to_int(payload.get("t_update"))

        return JobMetadata(
            id=job_id,
            name=name,
            company="netflix",
            locations=self._extract_locations(payload),
            postedTs=posted_ts,
            detailsUrl=details_url,
            applyUrl=f"{details_url}#apply" if details_url else None,
        )

    @staticmethod
    def _to_job_id(value: Any) -> str | None:
        if isinstance(value, int):
            return str(value)
        if isinstance(value, str):
            normalized = value.strip()
            return normalized or None
        return None

    def _build_details_url(self, payload: Mapping[str, Any]) -> str:
        canonical = _to_optional_str(payload.get("canonicalPositionUrl"))
        if canonical:
            return canonical

        job_id = self._to_job_id(payload.get("id"))
        if job_id:
            return f"{self.base_url}{self.CAREERS_PATH}/job/{urllib.parse.quote(job_id)}"

        return f"{self.base_url}{self.CAREERS_PATH}"

    @classmethod
    def _extract_locations(cls, payload: Mapping[str, Any]) -> list[Location]:
        raw_locations = payload.get("locations")
        standardized_locations: list[str] = []

        if isinstance(raw_locations, list):
            standardized_locations.extend(
                item.strip() for item in raw_locations if isinstance(item, str) and item.strip()
            )

        if not standardized_locations:
            fallback = _to_optional_str(payload.get("location"))
            if fallback:
                standardized_locations.append(fallback)

        out: list[Location] = []
        for location in standardized_locations:
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

    @staticmethod
    def _extract_job_description(value: Any) -> str | None:
        if isinstance(value, str):
            normalized = value.strip()
            return normalized or None

        if isinstance(value, list):
            parts = [item for item in value if isinstance(item, str) and item.strip()]
            if not parts:
                return None
            return "\n\n".join(parts)

        return None

    def _extract_description_from_job_page(self, *, job_id: str, details_url: str) -> str | None:
        html_payload = self._get_job_page_html(job_id=job_id, details_url=details_url)

        job_posting = self._extract_job_posting_ld_json(html_payload)
        if isinstance(job_posting, Mapping):
            description = _to_optional_str(job_posting.get("description"))
            if description:
                return html.unescape(description)

        meta_description = self._extract_meta_description(html_payload)
        if meta_description:
            return html.unescape(meta_description)
        return None

    def _get_job_page_html(self, *, job_id: str, details_url: str) -> str:
        if details_url.startswith("http://") or details_url.startswith("https://"):
            page_url = details_url
        else:
            page_url = urllib.parse.urljoin(
                f"{self.base_url}/",
                details_url,
            )
        if not page_url.strip():
            page_url = f"{self.base_url}{self.CAREERS_PATH}/job/{urllib.parse.quote(job_id)}"

        return request_text_with_backoff(
            url=page_url,
            headers={
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Referer": f"{self.base_url}{self.CAREERS_PATH}",
            },
            request_policy=self.get_request_policy(self.JOB_PAGE_POLICY_KEY),
            proxy_management_client=self.proxy_management_client,
        )

    @classmethod
    def _extract_job_posting_ld_json(cls, html_payload: str) -> Mapping[str, Any] | None:
        for match in cls._LD_JSON_BLOCK_PATTERN.finditer(html_payload):
            raw_block = match.group(1).strip()
            if not raw_block:
                continue

            try:
                parsed = json.loads(raw_block)
            except json.JSONDecodeError:
                continue

            found = cls._find_job_posting_in_json_ld(parsed)
            if found is not None:
                return found
        return None

    @classmethod
    def _find_job_posting_in_json_ld(cls, value: Any) -> Mapping[str, Any] | None:
        if isinstance(value, Mapping):
            item_type = _to_optional_str(value.get("@type"))
            if item_type and item_type.lower() == "jobposting":
                return value

            graph = value.get("@graph")
            if isinstance(graph, list):
                for item in graph:
                    found = cls._find_job_posting_in_json_ld(item)
                    if found is not None:
                        return found
            return None

        if isinstance(value, list):
            for item in value:
                found = cls._find_job_posting_in_json_ld(item)
                if found is not None:
                    return found
            return None

        return None

    @classmethod
    def _extract_meta_description(cls, html_payload: str) -> str | None:
        match = cls._META_DESCRIPTION_PATTERN.search(html_payload)
        if not match:
            return None
        return _to_optional_str(match.group(1))

    @classmethod
    def _extract_section(cls, description: str | None, *, headings: tuple[str, ...]) -> str | None:
        if not isinstance(description, str) or not description.strip():
            return None

        for heading in headings:
            heading_pattern = re.escape(heading.strip())
            html_heading_match = re.search(
                rf"(?is)<h[1-6][^>]*>\s*{heading_pattern}:?\s*</h[1-6]>\s*(.*?)(?=<h[1-6][^>]*>|$)",
                description,
            )
            if html_heading_match:
                section = html_heading_match.group(1).strip()
                if section:
                    return section

            strong_heading_match = re.search(
                rf"(?is)<strong[^>]*>\s*{heading_pattern}:?\s*</strong>\s*(.*?)(?=<strong[^>]*>|<h[1-6][^>]*>|$)",
                description,
            )
            if strong_heading_match:
                section = strong_heading_match.group(1).strip()
                if section:
                    return section

            bold_heading_match = re.search(
                rf"(?is)<b[^>]*>\s*{heading_pattern}:?\s*</b>\s*(.*?)(?=<b[^>]*>|<strong[^>]*>|<h[1-6][^>]*>|$)",
                description,
            )
            if bold_heading_match:
                section = bold_heading_match.group(1).strip()
                if section:
                    return section

        return None

    @classmethod
    def _extract_list_items(cls, value: Any) -> list[str]:
        raw_html = value if isinstance(value, str) else None
        if not raw_html:
            return []

        cleaned = raw_html.strip()
        if not cleaned:
            return []

        li_matches = re.findall(r"(?is)<li[^>]*>(.*?)</li>", cleaned)
        candidates = li_matches if li_matches else [cleaned]

        values: list[str] = []
        seen: set[str] = set()
        for candidate in candidates:
            text = cls._clean_html_fragment(candidate)
            if not text:
                continue
            pieces = re.split(r"(?:\n|•)+", text)
            for piece in pieces:
                normalized = piece.strip()
                if not normalized or normalized in seen:
                    continue
                seen.add(normalized)
                values.append(normalized)

        return values

    @staticmethod
    def _extract_html_list_blocks(value: str | None) -> list[str]:
        if not isinstance(value, str) or not value.strip():
            return []
        return re.findall(r"(?is)<ul[^>]*>.*?</ul>", value)

    @staticmethod
    def _clean_html_fragment(value: str) -> str:
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
