"""Meta jobs client backed by public Meta Careers GraphQL endpoints."""

from __future__ import annotations

import html
import json
import logging
import re
import urllib.parse
from datetime import datetime, timezone
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

import requests

from features.client import FeaturesClient
from scrapers.airflow.clients.common.base import JobsClient
from scrapers.airflow.clients.common.errors import RetryableUpstreamError
from scrapers.airflow.clients.common.html_text import extract_text
from common.job_taxonomy import infer_job_category_from_title
from common.request_policy import RequestPolicy
from scrapers.airflow.clients.common.http_requests import (
    build_get_url,
    request_text_with_backoff,
    request_text_with_managed_proxy_backoff,
)
from web.backend.schemas import (
    GetJobDetailsResponse,
    GetJobsResponse,
    JobDetailsSchema,
    JobMetadata,
    Location,
    PayDetails,
    PayRange,
)

if TYPE_CHECKING:
    from scrapers.proxy.proxy_management_client import ProxyManagementClient

MetaJobDetailsResponseSchema = GetJobDetailsResponse
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
            f"Unexpected Meta payload for {context}: expected object, got {type(value).__name__}"
        )
    return value


class MetaJobsClient(JobsClient):
    """Client for Meta Careers search/details GraphQL endpoints."""

    BASE_URL = "https://www.metacareers.com"
    SEARCH_PATH = "/jobsearch"
    JOBS_PATH = "/jobs"
    GRAPHQL_PATH = "/api/graphql/"
    PAGE_SIZE = 10
    ASBD_ID = "359341"

    SEARCH_POLICY_KEY = "search"
    DETAILS_POLICY_KEY = "details"
    GRAPHQL_BOOTSTRAP_POLICY_KEY = "graphql_bootstrap"

    RESULTS_QUERY_DOC_ID = "24330890369943030"
    RESULTS_QUERY_NAME = "CareersJobSearchResultsV3DataQuery"

    DETAILS_QUERY_DOC_ID = "25818426654480074"
    DETAILS_QUERY_NAME = "CandidatePortalJobDetailsViewQuery"

    _LSD_PATTERN = re.compile(r'"LSD",\[\],\{"token":"([^"]+)"\}')
    _FOR_LOOP_PREFIX = "for (;;);"
    _JSON_LD_PATTERN = re.compile(
        r'(?is)<script[^>]*type=["\']application/ld\+json["\'][^>]*>(.*?)</script>'
    )
    _COMPENSATION_AMOUNT_PATTERN = re.compile(
        r"(?i)(?P<currency>[$£€])?\s*(?P<amount>\d[\d,]*)\s*(?:/\s*(?P<interval>[a-z]+))?"
    )

    _COUNTRY_CODES = frozenset(
        {
            "AE",
            "AU",
            "BR",
            "CA",
            "DE",
            "FR",
            "GB",
            "IN",
            "IE",
            "IT",
            "JP",
            "KR",
            "MX",
            "NL",
            "SG",
            "UK",
            "US",
        }
    )

    def __init__(
        self,
        *,
        base_url: str,
        default_request_policy: RequestPolicy,
        endpoint_request_policies: Mapping[str, RequestPolicy] | None = None,
        proxy_management_client: "ProxyManagementClient",
        features_client: FeaturesClient | None = None,
    ) -> None:
        super().__init__(
            default_request_policy=default_request_policy,
            endpoint_request_policies=endpoint_request_policies,
        )
        self.base_url = base_url.rstrip("/")
        self.proxy_management_client = proxy_management_client
        self.features_client = features_client

    def get_jobs(self, *, page: int = 1) -> GetJobsResponse:
        """Fetch one Meta jobs page by 1-based index."""
        if page < 1:
            raise ValueError("page must be >= 1")

        search_results = self._fetch_search_results()
        all_jobs_raw = search_results.get("all_jobs")
        if not isinstance(all_jobs_raw, list):
            raise ValueError(
                "Unexpected Meta payload for results.data.job_search_with_featured_jobs.all_jobs: "
                f"expected array, got {type(all_jobs_raw).__name__}"
            )

        all_jobs: list[JobMetadata] = []
        raw_location_batches: list[list[str]] = []
        for index, item in enumerate(all_jobs_raw):
            if not isinstance(item, Mapping):
                raise ValueError(
                    "Unexpected Meta payload for results.data.job_search_with_featured_jobs.all_jobs"
                    f"[{index}]: expected object, got {type(item).__name__}"
                )
            raw_locations = item.get("locations")
            raw_location_batches.append(
                [value.strip() for value in raw_locations if isinstance(value, str) and value.strip()]
                if isinstance(raw_locations, list)
                else []
            )

        normalized_locations_by_job = self._normalize_locations(raw_location_batches)

        for item, locations in zip(all_jobs_raw, normalized_locations_by_job):
            all_jobs.append(self._parse_job_metadata(item, locations=locations))

        total_results = len(all_jobs)
        start_index = (page - 1) * self.PAGE_SIZE
        page_jobs = all_jobs[start_index : start_index + self.PAGE_SIZE] if start_index < total_results else []
        has_next_page = start_index + len(page_jobs) < total_results

        return GetJobsResponse(
            status=200,
            error=None,
            jobs=page_jobs,
            pagination_index=page,
            has_next_page=has_next_page,
            positions=page_jobs,
            total_results=total_results,
            page_size=self.PAGE_SIZE,
        )

    def _fetch_search_results(self) -> Mapping[str, Any]:
        payload = self._run_graphql_query(
            doc_id=self.RESULTS_QUERY_DOC_ID,
            query_name=self.RESULTS_QUERY_NAME,
            variables={
                "search_input": self._build_search_input(results_per_page=None),
            },
            referer_path=self.SEARCH_PATH,
            endpoint_policy_key=self.SEARCH_POLICY_KEY,
        )
        return self._extract_search_results_from_payload(payload)

    @staticmethod
    def _build_search_input(**overrides: Any) -> dict[str, Any]:
        search_input = {
            "q": None,
            "divisions": [],
            "offices": [],
            "roles": [],
            "leadership_levels": [],
            "saved_jobs": [],
            "saved_searches": [],
            "sub_teams": [],
            "teams": [],
            "is_leadership": False,
            "is_remote_only": False,
            "sort_by_new": False,
            "results_per_page": None,
        }
        search_input.update(overrides)
        return search_input

    @staticmethod
    def _extract_search_results_from_payload(payload: Mapping[str, Any]) -> Mapping[str, Any]:
        data = _require_mapping(payload.get("data"), context="results.data")
        return _require_mapping(
            data.get("job_search_with_featured_jobs"),
            context="results.data.job_search_with_featured_jobs",
        )

    def _extract_total_results(self, search_results: Mapping[str, Any]) -> int | None:
        for key in ("total_count", "count", "total_results", "all_jobs_count", "allJobsCount"):
            parsed = _to_int(search_results.get(key))
            if isinstance(parsed, int) and parsed >= 0:
                return parsed
        return None

    def _resolve_has_next_page(self, *, page: int, jobs_count: int, total_results: int | None) -> bool:
        if isinstance(total_results, int):
            return page * self.PAGE_SIZE < total_results
        return jobs_count == self.PAGE_SIZE

    def get_job_details(self, *, job_id: str) -> MetaJobDetailsResponseSchema:
        """Fetch detailed data for one Meta job id."""
        normalized_job_id = job_id.strip()
        if not normalized_job_id:
            raise ValueError("job_id must be a non-empty string")
        details_url = f"{self.base_url}{self.JOBS_PATH}/{urllib.parse.quote(normalized_job_id)}/"

        try:
            payload = self._run_graphql_query(
                doc_id=self.DETAILS_QUERY_DOC_ID,
                query_name=self.DETAILS_QUERY_NAME,
                variables={
                    "renderLoggedInView": False,
                    "requisitionID": normalized_job_id,
                    "viewasUserID": None,
                },
                referer_path=f"{self.JOBS_PATH}/{urllib.parse.quote(normalized_job_id)}/",
                endpoint_policy_key=self.DETAILS_POLICY_KEY,
            )
        except requests.exceptions.HTTPError as exc:
            status_code = exc.response.status_code if exc.response is not None else None
            if status_code == 404:
                return MetaJobDetailsResponseSchema(
                    status=404,
                    error=f"Job '{normalized_job_id}' not found for company 'meta' url={details_url}",
                    jobDescription=None,
                    postedTs=None,
                    detailsUrl=details_url,
                )
            raise
        except RetryableUpstreamError:
            return MetaJobDetailsResponseSchema(
                status=404,
                error=f"Job '{normalized_job_id}' not found for company 'meta' url={details_url}",
                jobDescription=None,
                postedTs=None,
                detailsUrl=details_url,
            )

        data = _require_mapping(payload.get("data"), context="details.data")
        details_raw = data.get("xcp_requisition_job_description")
        if details_raw is None:
            return MetaJobDetailsResponseSchema(
                status=404,
                error=f"Job '{normalized_job_id}' not found for company 'meta' url={details_url}",
                jobDescription=None,
                postedTs=None,
                detailsUrl=details_url,
            )
        if not isinstance(details_raw, Mapping):
            raise ValueError(
                "Unexpected Meta payload for details.data.xcp_requisition_job_description: "
                f"expected object, got {type(details_raw).__name__}"
            )

        parsed_job = self._parse_job_details(payload=details_raw)
        page_details = self._extract_job_page_details(details_url=details_url)
        page_posted_ts = page_details["postedTs"]
        if page_posted_ts is None:
            LOGGER.info(
                "meta_posted_ts_missing job_id=%s details_top_level_keys=%s",
                normalized_job_id,
                ",".join(sorted(details_raw.keys())),
            )

        return MetaJobDetailsResponseSchema(
            status=200,
            error=None,
            jobDescription=parsed_job.jobDescription,
            postedTs=page_posted_ts,
            detailsUrl=details_url,
        )

    def _parse_job_metadata(self, payload: Mapping[str, Any], locations: list[Location] | None = None) -> JobMetadata:
        raw_job_id = payload.get("id")
        job_id = str(raw_job_id).strip()
        if not job_id:
            raise ValueError("Unexpected Meta payload for job metadata: missing required field 'id'")

        name = _to_optional_str(payload.get("title"))
        details_url = f"{self.base_url}{self.JOBS_PATH}/{urllib.parse.quote(job_id)}/"
        apply_url = f"{self.base_url}/profile/create_application/{urllib.parse.quote(job_id)}"
        return JobMetadata(
            id=job_id,
            name=name,
            company="meta",
            jobCategory=infer_job_category_from_title(title=name),
            locations=list(locations or []),
            postedTs=None,
            detailsUrl=details_url,
            applyUrl=apply_url,
        )

    def _normalize_locations(self, raw_location_batches: list[list[str]]) -> list[list[Location]]:
        if not raw_location_batches:
            return []
        if self.features_client is None:
            return [[] for _ in raw_location_batches]

        flattened = [value for batch in raw_location_batches for value in batch]
        if not flattened:
            return [[] for _ in raw_location_batches]

        payload = self.features_client.normalize_locations(locations=flattened)
        raw_locations = payload.get("locations")
        if not isinstance(raw_locations, list):
            raise ValueError("Invalid normalize_locations payload")

        normalized_flat: list[Location] = []
        for item in raw_locations:
            if not isinstance(item, Mapping):
                raise ValueError("Invalid normalized location item")
            normalized_flat.append(
                Location(
                    city=str(item.get("city", "") or "").strip(),
                    state=str(item.get("region", "") or "").strip(),
                    country=str(item.get("country", "") or "").strip(),
                )
            )

        if len(normalized_flat) != len(flattened):
            raise ValueError("Normalized location count mismatch")

        out: list[list[Location]] = []
        offset = 0
        for batch in raw_location_batches:
            count = len(batch)
            out.append(normalized_flat[offset : offset + count])
            offset += count
        return out

    def _parse_job_details(self, *, payload: Mapping[str, Any]) -> JobDetailsSchema:
        description_html = self._extract_html_fragment(payload.get("description"))
        description = extract_text(description_html)
        responsibilities = self._extract_detail_items(payload.get("responsibilities"))
        minimum_qualifications = self._extract_detail_items(payload.get("minimum_qualifications"))
        preferred_qualifications = self._extract_detail_items(payload.get("preferred_qualifications"))

        parts = [
            self._normalize_plain_text(payload.get("title")),
            self._format_section("Description", description),
            self._format_section("Responsibilities", self._join_items(responsibilities)),
            self._format_section("Minimum Qualifications", self._join_items(minimum_qualifications)),
            self._format_section("Preferred Qualifications", self._join_items(preferred_qualifications)),
            self._build_about_meta_section(payload),
        ]
        job_description = "\n\n".join(part for part in parts if part) or None

        return JobDetailsSchema(
            jobDescription=job_description,
            postedTs=None,
        )

    @classmethod
    def _parse_timestamp_any(cls, value: Any) -> int | None:
        # Numeric unix timestamp (seconds or milliseconds).
        numeric = _to_int(value)
        if isinstance(numeric, int):
            return numeric // 1000 if numeric > 10_000_000_000 else numeric

        # Common object shapes from GraphQL payloads.
        if isinstance(value, Mapping):
            for key in ("timestamp", "time", "value", "seconds"):
                parsed = cls._parse_timestamp_any(value.get(key))
                if parsed is not None:
                    return parsed

        if not isinstance(value, str):
            return None
        normalized = value.strip()
        if not normalized:
            return None

        # ISO-like strings.
        iso_candidate = normalized.replace("Z", "+00:00")
        try:
            parsed_dt = datetime.fromisoformat(iso_candidate)
            if parsed_dt.tzinfo is None:
                parsed_dt = parsed_dt.replace(tzinfo=timezone.utc)
            return int(parsed_dt.timestamp())
        except ValueError:
            pass

        # Human-readable date strings often used in job payloads.
        for fmt in ("%b %d, %Y", "%B %d, %Y", "%Y-%m-%d"):
            try:
                parsed_dt = datetime.strptime(normalized, fmt).replace(tzinfo=timezone.utc)
                return int(parsed_dt.timestamp())
            except ValueError:
                continue
        return None


    def _extract_posted_ts_from_job_page(self, *, details_url: str) -> int | None:
        return self._extract_job_page_details(details_url=details_url)["postedTs"]

    def _extract_job_page_details(self, *, details_url: str) -> dict[str, Any]:
        try:
            html_payload = request_text_with_backoff(
                url=details_url,
                headers={"Accept": "text/html"},
                request_policy=self.get_request_policy(self.DETAILS_POLICY_KEY),
                proxy_management_client=self.proxy_management_client,
            )
            return self._extract_job_page_details_from_html(html_payload)
        except Exception as exc:
            status_code = None
            if isinstance(exc, requests.exceptions.HTTPError) and exc.response is not None:
                status_code = int(exc.response.status_code)
            LOGGER.warning(
                "meta_job_page_date_posted_fetch_failed error=%s status=%s",
                type(exc).__name__,
                status_code,
            )
            return {"postedTs": None, "jobDescription": None}

    @classmethod
    def _extract_job_page_details_from_html(cls, html_payload: str) -> dict[str, Any]:
        job_posting = cls._extract_job_posting_json_ld(html_payload)
        posted_ts = cls._extract_posted_ts_from_html(html_payload)
        if not job_posting:
            return {"postedTs": posted_ts, "jobDescription": None}

        parts = [
            cls._normalize_plain_text(job_posting.get("title")),
            cls._format_section("Description", cls._normalize_plain_text(job_posting.get("description"))),
            cls._format_section("Responsibilities", cls._normalize_htmlish_text(job_posting.get("responsibilities"))),
            cls._format_section("Qualifications", cls._normalize_htmlish_text(job_posting.get("qualifications"))),
        ]
        job_description = "\n\n".join(part for part in parts if part) or None
        return {"postedTs": posted_ts, "jobDescription": job_description}

    @classmethod
    def _extract_posted_ts_from_html(cls, html_payload: str) -> int | None:
        for match in cls._JSON_LD_PATTERN.finditer(html_payload):
            raw_script = match.group(1).strip()
            if not raw_script:
                continue
            try:
                parsed_script = json.loads(raw_script)
            except json.JSONDecodeError:
                continue
            posted_value = cls._find_key_recursive(parsed_script, target_key="datePosted")
            parsed_ts = cls._parse_timestamp_any(posted_value)
            if parsed_ts is not None:
                return parsed_ts

        # Fallback for cases where JSON-LD parsing fails but datePosted is still present.
        fallback_match = re.search(r'"datePosted"\s*:\s*"([^"]+)"', html_payload, flags=re.IGNORECASE)
        if fallback_match:
            return cls._parse_timestamp_any(fallback_match.group(1))
        return None

    @classmethod
    def _extract_job_posting_json_ld(cls, html_payload: str) -> Mapping[str, Any] | None:
        for match in cls._JSON_LD_PATTERN.finditer(html_payload):
            raw_script = match.group(1).strip()
            if not raw_script:
                continue
            try:
                parsed_script = json.loads(raw_script)
            except json.JSONDecodeError:
                continue
            job_posting = cls._find_job_posting_object(parsed_script)
            if job_posting is not None:
                return job_posting
        return None

    @classmethod
    def _find_job_posting_object(cls, value: Any) -> Mapping[str, Any] | None:
        if isinstance(value, Mapping):
            object_type = _to_optional_str(value.get("@type"))
            if object_type and object_type.casefold() == "jobposting":
                return value
            for child in value.values():
                nested = cls._find_job_posting_object(child)
                if nested is not None:
                    return nested
            return None
        if isinstance(value, list):
            for item in value:
                nested = cls._find_job_posting_object(item)
                if nested is not None:
                    return nested
        return None

    @staticmethod
    def _normalize_plain_text(value: Any) -> str | None:
        raw_value = _to_optional_str(value)
        if not raw_value:
            return None
        normalized = html.unescape(raw_value).replace("\r\n", "\n").replace("\r", "\n")
        normalized = re.sub(r"\n{3,}", "\n\n", normalized).strip()
        return normalized or None

    @classmethod
    def _normalize_htmlish_text(cls, value: Any) -> str | None:
        raw_value = cls._normalize_plain_text(value)
        if not raw_value:
            return None
        text = raw_value.replace("\xa0", "\n\n").replace("&nbsp;", "\n\n")
        text = re.sub(r"\n{3,}", "\n\n", text)
        lines = [line.strip() for line in text.splitlines()]
        compact: list[str] = []
        previous_blank = False
        for line in lines:
            if not line:
                if compact and not previous_blank:
                    compact.append("")
                previous_blank = True
                continue
            compact.append(line)
            previous_blank = False
        rendered = "\n".join(compact).strip()
        return rendered or None

    @staticmethod
    def _format_section(title: str, content: str | None) -> str | None:
        if not content:
            return None
        return f"{title}\n{content}"

    @staticmethod
    def _join_items(items: list[str]) -> str | None:
        if not items:
            return None
        return "\n".join(item for item in items if item) or None

    @classmethod
    def _build_about_meta_section(cls, payload: Mapping[str, Any]) -> str | None:
        intro = extract_text(cls._extract_html_fragment(payload.get("boiler_plate_intro")))
        california = cls._collapse_inline_text(
            extract_text(cls._extract_html_fragment(payload.get("california_disclaimer")))
        )
        compensation_line = cls._build_compensation_line(payload.get("public_compensation"))
        compensation_note = (
            "Individual compensation is determined by skills, qualifications, experience, and location. "
            "Compensation details listed in this posting reflect the base hourly rate, monthly rate, or annual salary only, "
            "and do not include bonus, equity or sales incentives, if applicable. In addition to base compensation, Meta offers benefits. "
            "Learn more about benefits at Meta."
            if compensation_line
            else None
        )
        content = "\n\n".join(
            part
            for part in [intro, california, compensation_line, compensation_note]
            if part
        )
        return cls._format_section("About Meta", content or None)

    @classmethod
    def _build_compensation_line(cls, value: Any) -> str | None:
        if not isinstance(value, list) or not value:
            return None
        item = value[0]
        if not isinstance(item, Mapping):
            return None
        minimum = _to_optional_str(item.get("compensation_amount_minimum"))
        maximum = _to_optional_str(item.get("compensation_amount_maximum"))
        if not minimum and not maximum:
            return None
        range_text = " to ".join(part for part in [minimum, maximum] if part)
        suffixes: list[str] = []
        if item.get("has_bonus") is True:
            suffixes.append("bonus")
        if item.get("has_equity") is True:
            suffixes.append("equity")
        suffixes.append("benefits")
        return f"{range_text} + {' + '.join(suffixes)}"

    @staticmethod
    def _collapse_inline_text(value: str | None) -> str | None:
        if not value:
            return None
        collapsed = " ".join(part.strip() for part in value.splitlines() if part.strip())
        return collapsed or None

    @classmethod
    def _find_key_recursive(cls, value: Any, *, target_key: str) -> Any:
        if isinstance(value, Mapping):
            for key, child in value.items():
                if str(key).lower() == target_key.lower():
                    return child
                nested = cls._find_key_recursive(child, target_key=target_key)
                if nested is not None:
                    return nested
            return None
        if isinstance(value, list):
            for item in value:
                nested = cls._find_key_recursive(item, target_key=target_key)
                if nested is not None:
                    return nested
            return None
        return None

    def _run_graphql_query(
        self,
        *,
        doc_id: str,
        query_name: str,
        variables: Mapping[str, Any],
        referer_path: str,
        endpoint_policy_key: str,
    ) -> Mapping[str, Any]:
        try:
            lsd_token = self._bootstrap_lsd_token()

            response_text = request_text_with_managed_proxy_backoff(
                method="POST",
                url=f"{self.base_url}{self.GRAPHQL_PATH}",
                headers={
                    "Accept": "*/*",
                    "Content-Type": "application/x-www-form-urlencoded",
                    "Origin": self.base_url,
                    "Referer": f"{self.base_url}{referer_path}",
                    "X-ASBD-ID": self.ASBD_ID,
                    "X-FB-Friendly-Name": query_name,
                    "X-FB-LSD": lsd_token,
                },
                data={
                    "av": "0",
                    "__user": "0",
                    "__a": "1",
                    "lsd": lsd_token,
                    "jazoest": self._build_jazoest(lsd_token),
                    "fb_api_caller_class": "RelayModern",
                    "fb_api_req_friendly_name": query_name,
                    "variables": json.dumps(variables, separators=(",", ":")),
                    "server_timestamps": "true",
                    "doc_id": doc_id,
                },
                request_policy=self.get_request_policy(endpoint_policy_key),
                proxy_management_client=self.proxy_management_client,
            )
            response_text = self._strip_for_loop_prefix(response_text)

            parsed_payload = json.loads(response_text)
            parsed_mapping = _require_mapping(parsed_payload, context="graphql.response")

            errors = parsed_mapping.get("errors")
            if isinstance(errors, list) and errors:
                first_error = errors[0]
                if isinstance(first_error, Mapping):
                    message = _to_optional_str(first_error.get("message"))
                    if message:
                        raise ValueError(f"Meta GraphQL error for {query_name}: {message}")
                raise ValueError(f"Meta GraphQL error for {query_name}")

            return parsed_mapping
        except Exception as exc:
            status_code = None
            if isinstance(exc, requests.exceptions.HTTPError) and exc.response is not None:
                status_code = int(exc.response.status_code)
            LOGGER.warning(
                "meta_graphql_request_failed operation=%s error=%s status=%s",
                query_name,
                type(exc).__name__,
                status_code,
            )
            raise

    def _bootstrap_lsd_token(
        self,
    ) -> str:
        bootstrap_url = build_get_url(
            base_url=self.base_url,
            path=self.SEARCH_PATH,
            params=[],
        )
        html_payload = request_text_with_backoff(
            url=bootstrap_url,
            headers={
                "Accept": "text/html",
            },
            request_policy=self.get_request_policy(self.GRAPHQL_BOOTSTRAP_POLICY_KEY),
            proxy_management_client=self.proxy_management_client,
        )
        match = self._LSD_PATTERN.search(html_payload)
        if not match:
            raise RetryableUpstreamError("Unable to extract Meta request token from careers page")
        return match.group(1)

    @staticmethod
    def _build_jazoest(lsd_token: str) -> str:
        return f"2{sum(ord(char) for char in lsd_token)}"

    @classmethod
    def _strip_for_loop_prefix(cls, payload: str) -> str:
        normalized = payload.lstrip()
        if normalized.startswith(cls._FOR_LOOP_PREFIX):
            return normalized[len(cls._FOR_LOOP_PREFIX) :]
        return normalized

    @classmethod
    def _extract_html_fragment(cls, value: Any) -> str | None:
        raw_value = _to_optional_str(value)
        if not raw_value:
            return None

        if raw_value.startswith("{") and "__html" in raw_value:
            try:
                parsed = json.loads(raw_value)
            except json.JSONDecodeError:
                return raw_value
            if isinstance(parsed, Mapping):
                html_value = _to_optional_str(parsed.get("__html"))
                if html_value:
                    return html_value
        return raw_value

    @classmethod
    def _extract_detail_items(cls, value: Any) -> list[str]:
        if not isinstance(value, list):
            return []

        out: list[str] = []
        for item in value:
            raw_text: str | None = None
            if isinstance(item, Mapping):
                raw_text = _to_optional_str(item.get("item"))
            elif isinstance(item, str):
                raw_text = _to_optional_str(item)
            if not raw_text:
                continue
            normalized = re.sub(r"\s+", " ", html.unescape(raw_text)).strip()
            if normalized:
                out.append(normalized)
        return _dedupe(out)

    @classmethod
    def _extract_public_compensation(cls, value: Any) -> PayDetails | None:
        if not isinstance(value, list):
            return None

        ranges: list[PayRange] = []
        notes: list[str] = []

        for item in value:
            if not isinstance(item, Mapping):
                continue

            minimum_amount, minimum_currency, minimum_interval = cls._parse_compensation_amount(
                item.get("compensation_amount_minimum")
            )
            maximum_amount, maximum_currency, maximum_interval = cls._parse_compensation_amount(
                item.get("compensation_amount_maximum")
            )
            currency = minimum_currency or maximum_currency
            interval = minimum_interval or maximum_interval

            if (
                minimum_amount is not None
                or maximum_amount is not None
                or currency is not None
                or interval is not None
            ):
                ranges.append(
                    PayRange(
                        minAmount=minimum_amount,
                        maxAmount=maximum_amount,
                        currency=currency,
                        interval=interval,
                        context="public_compensation",
                    )
                )

            if item.get("has_bonus") is True:
                notes.append("Bonus eligible")
            if item.get("has_equity") is True:
                notes.append("Equity eligible")

            error_note = _to_optional_str(item.get("error_apology_note"))
            if error_note:
                notes.append(error_note)

        if not ranges and not notes:
            return None
        return PayDetails(ranges=ranges, notes=_dedupe(notes))

    @classmethod
    def _merge_pay_details(
        cls,
        primary: PayDetails | None,
        fallback: PayDetails | None,
    ) -> PayDetails | None:
        if primary is None:
            return fallback
        if fallback is None:
            return primary

        merged_ranges: list[PayRange] = []
        seen_ranges: set[tuple[int | None, int | None, str | None, str | None, str | None]] = set()
        for source in (primary, fallback):
            for item in source.ranges:
                key = (item.minAmount, item.maxAmount, item.currency, item.interval, item.context)
                if key in seen_ranges:
                    continue
                seen_ranges.add(key)
                merged_ranges.append(item)

        return PayDetails(
            ranges=merged_ranges,
            notes=_dedupe([*primary.notes, *fallback.notes]),
        )

    @classmethod
    def _parse_compensation_amount(cls, value: Any) -> tuple[int | None, str | None, str | None]:
        raw_value = _to_optional_str(value)
        if not raw_value:
            return None, None, None

        match = cls._COMPENSATION_AMOUNT_PATTERN.search(raw_value)
        if not match:
            return None, None, None

        amount_raw = match.group("amount")
        amount = int(amount_raw.replace(",", "")) if amount_raw else None
        currency = cls._normalize_currency(match.group("currency"))
        interval = cls._normalize_interval(match.group("interval"))
        return amount, currency, interval

    @staticmethod
    def _normalize_currency(value: str | None) -> str | None:
        if value == "$":
            return "USD"
        if value == "£":
            return "GBP"
        if value == "€":
            return "EUR"
        return None

    @staticmethod
    def _normalize_interval(value: str | None) -> str | None:
        if not value:
            return None
        normalized = value.strip().lower()
        if normalized in {"year", "yr", "annual", "annually"}:
            return "year"
        if normalized in {"month", "mo"}:
            return "month"
        if normalized in {"week", "wk"}:
            return "week"
        if normalized in {"day"}:
            return "day"
        if normalized in {"hour", "hr"}:
            return "hour"
        return normalized

    @classmethod
    def _to_locations(cls, locations: list[str]) -> list[Location]:
        out: list[Location] = []
        for location in locations:
            parts = [part.strip() for part in location.split(",") if part.strip()]
            city = ""
            state = ""
            country = ""
            if len(parts) == 1:
                country = parts[0]
            elif len(parts) == 2:
                city = parts[0]
                second = parts[1]
                if second.isupper() and len(second) <= 3 and second not in cls._COUNTRY_CODES:
                    state = second
                elif second in {"CA", "NY", "WA", "TX", "MA", "IL"}:
                    state = second
                elif second in {"UK", "US", "UAE"}:
                    country = second
                else:
                    country = second
            elif len(parts) >= 3:
                city = parts[0]
                state = parts[1]
                country = ", ".join(parts[2:])
            out.append(Location(city=city, state=state, country=country))
        return out
