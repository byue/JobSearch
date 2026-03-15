"""FastAPI backend exposing unified job APIs backed by the published DB snapshot."""

from __future__ import annotations

import logging
import math
import os
from datetime import datetime, timezone
from typing import Any

import psycopg
import requests
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from psycopg.rows import dict_row

from common.request_policy import RequestPolicy
from features.client import FeaturesClient
from scrapers.common.elasticsearch import ElasticsearchClient
from scrapers.common.env import require_env
from scrapers.common.minio import get_job_description
from web.backend.schemas import (
    GetCompaniesResponse,
    GetJobDetailsRequest,
    GetJobDetailsResponse,
    GetJobsRequest,
    GetJobsResponse,
    JobMetadata,
    Location,
)

_LOGGER = logging.getLogger(__name__)


app = FastAPI(
    title="JobSearch API",
    version="1.0.0",
    description="Unified backend API for Amazon, Apple, Microsoft, Google, Meta, and Netflix jobs.",
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://127.0.0.1:5173",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def _env_int(name: str, default: int, *, minimum: int = 0) -> int:
    import os

    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        parsed = int(raw.strip())
    except ValueError:
        return default
    return max(parsed, minimum)


def _normalize_db_url(raw_url: str) -> str:
    normalized = raw_url.strip()
    if normalized.startswith("postgresql+psycopg2://"):
        return normalized.replace("postgresql+psycopg2://", "postgresql://", 1)
    if normalized.startswith("postgresql+psycopg://"):
        return normalized.replace("postgresql+psycopg://", "postgresql://", 1)
    return normalized


_DB_URL = _normalize_db_url(require_env("JOBSEARCH_DB_URL"))
_PAGE_SIZE = _env_int("JOBSEARCH_API_PAGE_SIZE", default=25, minimum=1)
_FEATURES_API_URL = os.getenv("JOBSEARCH_FEATURES_API_URL", "http://localhost:8010").strip() or "http://localhost:8010"
_ES_URL = os.getenv("JOBSEARCH_ES_URL", "http://localhost:9200").strip() or "http://localhost:9200"
_ES_ALIAS = os.getenv("JOBSEARCH_ES_ALIAS", "jobs_catalog").strip() or "jobs_catalog"
_ES_QUERY_CANDIDATES = _env_int("JOBSEARCH_ES_QUERY_CANDIDATES", default=100, minimum=1)
_ES_KNN_CANDIDATES = _env_int("JOBSEARCH_ES_KNN_CANDIDATES", default=100, minimum=1)
_ES_KNN_NUM_CANDIDATES = _env_int("JOBSEARCH_ES_KNN_NUM_CANDIDATES", default=200, minimum=1)
_RRF_K = _env_int("JOBSEARCH_ES_RRF_K", default=60, minimum=1)


def _db_conn() -> psycopg.Connection[Any]:
    return psycopg.connect(_DB_URL, row_factory=dict_row)


def _default_request_policy() -> RequestPolicy:
    return RequestPolicy(
        timeout_seconds=10.0,
        connect_timeout_seconds=2.0,
        max_retries=2,
        backoff_factor=0.5,
        max_backoff_seconds=4.0,
        jitter=True,
    )


def _features_client() -> FeaturesClient:
    return FeaturesClient(base_url=_FEATURES_API_URL, request_policy=_default_request_policy())


def _es_client() -> ElasticsearchClient:
    return ElasticsearchClient(base_url=_ES_URL, request_policy=_default_request_policy())


def _active_run_id() -> str:
    with _db_conn() as conn:
        row = conn.execute(
            """
            SELECT p.run_id
            FROM publication_pointers p
            JOIN publish_runs r ON r.run_id = p.run_id
            WHERE p.namespace = 'jobs_catalog'
              AND r.db_ready = TRUE
            LIMIT 1
            """
        ).fetchone()
    if row is None:
        raise HTTPException(status_code=503, detail="No published DB snapshot available")
    return str(row["run_id"])


def _epoch_seconds(value: datetime | None) -> int | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return int(value.timestamp())


def _load_job_description(path: str | None) -> str | None:
    if path is None:
        return None
    normalized = path.strip()
    if not normalized:
        return None
    return get_job_description(key=normalized)


def _normalize_skills(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    out: list[str] = []
    for item in value:
        if not isinstance(item, str):
            continue
        normalized = item.strip()
        if normalized:
            out.append(normalized)
    return out


def _normalize_company_filter(value: str | None) -> str | None:
    normalized = str(value or "").strip().lower()
    if not normalized or normalized == "__all__":
        return None
    return normalized


def _normalize_posted_within(value: str | None) -> str | None:
    normalized = str(value or "").strip().lower()
    if normalized in {"24h", "7d", "30d"}:
        return normalized
    return None


def _validate_company_in_run(run_id: str, company: str) -> str:
    normalized = company.strip().lower()
    if not normalized:
        raise ValueError("company is required")
    with _db_conn() as conn:
        row = conn.execute(
            """
            SELECT 1
            FROM companies
            WHERE run_id = %s
              AND company = %s
            LIMIT 1
            """,
            (run_id, normalized),
        ).fetchone()
    if row is None:
        raise ValueError(f"Unsupported company '{company}' for active run '{run_id}'")
    return normalized


def _resolve_company_filter(run_id: str, company: str | None) -> str | None:
    normalized = _normalize_company_filter(company)
    if normalized is None:
        return None
    return _validate_company_in_run(run_id, normalized)


def _epoch_millis(value: datetime | None) -> int | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return int(value.timestamp() * 1000)


def _format_es_posted_ts(value: int | float | str | None) -> int | None:
    if isinstance(value, (int, float)):
        raw = float(value)
        if raw > 1_000_000_000_000:
            return int(raw // 1000)
        return int(raw)
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        try:
            if stripped.endswith("Z"):
                stripped = stripped[:-1] + "+00:00"
            dt = datetime.fromisoformat(stripped)
        except ValueError:
            return None
        return _epoch_seconds(dt)
    return None


def _build_location_from_source(source: dict[str, Any]) -> list[Location]:
    city = str(source.get("city") or "")
    state = str(source.get("state") or "")
    country = str(source.get("country") or "")
    if not any([city, state, country]):
        return []
    return [Location(city=city, state=state, country=country)]


def _hits_from_response(payload: dict[str, Any]) -> list[dict[str, Any]]:
    hits = payload.get("hits")
    if not isinstance(hits, dict):
        return []
    raw_hits = hits.get("hits")
    return raw_hits if isinstance(raw_hits, list) else []


def _total_hits_from_response(payload: dict[str, Any]) -> int:
    hits = payload.get("hits")
    if not isinstance(hits, dict):
        return 0
    total = hits.get("total")
    if isinstance(total, dict):
        value = total.get("value")
        return int(value) if isinstance(value, int) else 0
    return int(total) if isinstance(total, int) else 0


def _job_metadata_from_hit(hit: dict[str, Any]) -> JobMetadata:
    source = hit.get("_source")
    if not isinstance(source, dict):
        source = {}
    return JobMetadata(
        id=str(source.get("external_job_id") or ""),
        runId=str(source.get("run_id") or "") or None,
        name=str(source.get("title") or "") or None,
        company=str(source.get("company") or "") or None,
        locations=_build_location_from_source(source),
        postedTs=_format_es_posted_ts(source.get("posted_ts")),
        applyUrl=str(source.get("apply_url") or "") or None,
        detailsUrl=str(source.get("details_url") or "") or None,
    )


def _search_filters(company: str | None, posted_within: str | None) -> list[dict[str, Any]]:
    filters: list[dict[str, Any]] = []
    if company:
        filters.append({"term": {"company": company}})
    if posted_within:
        filters.append({"range": {"posted_ts": {"gte": f"now-{posted_within}"}}})
    return filters


def _browse_jobs(company: str | None, *, posted_within: str | None, page: int) -> tuple[list[JobMetadata], int, bool]:
    offset = (page - 1) * _PAGE_SIZE
    body = {
        "track_total_hits": True,
        "from": offset,
        "size": _PAGE_SIZE,
        "sort": [
            {"posted_ts": {"order": "desc", "missing": "_last"}},
            {"external_job_id": {"order": "asc"}},
        ],
        "query": {
            "bool": {
                "filter": _search_filters(company, posted_within),
            }
        },
    }
    payload = _es_client().search(index_name=_ES_ALIAS, body=body)
    hits = _hits_from_response(payload)
    jobs = [_job_metadata_from_hit(hit) for hit in hits]
    total_results = _total_hits_from_response(payload)
    return jobs, total_results, offset + len(jobs) < total_results


def _query_embedding(query: str) -> list[float]:
    payload = _features_client().get_query_embedding(text=query)
    raw_embedding = payload.get("embedding")
    if not isinstance(raw_embedding, list):
        return []
    return [float(value) for value in raw_embedding]


def _rrf_fuse_hits(bm25_hits: list[dict[str, Any]], knn_hits: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_id: dict[str, dict[str, Any]] = {}
    scores: dict[str, float] = {}
    for hits in (bm25_hits, knn_hits):
        for rank, hit in enumerate(hits, start=1):
            doc_id = str(hit.get("_id") or "")
            if not doc_id:
                continue
            by_id[doc_id] = hit
            scores[doc_id] = scores.get(doc_id, 0.0) + (1.0 / (_RRF_K + rank))
    return [
        by_id[doc_id]
        for doc_id, _score in sorted(
            scores.items(),
            key=lambda item: (
                -item[1],
                -(_format_es_posted_ts(by_id[item[0]].get("_source", {}).get("posted_ts")) or -1),
                item[0],
            ),
        )
    ]


def _search_jobs(
    company: str | None,
    *,
    query: str,
    page: int,
    posted_within: str | None,
) -> tuple[list[JobMetadata], int, bool]:
    filters = _search_filters(company, posted_within)

    query_embedding = _query_embedding(query)
    bm25_body = {
        "size": _ES_QUERY_CANDIDATES,
        "query": {
            "bool": {
                "must": [
                    {
                        "multi_match": {
                            "query": query,
                            "fields": ["title^4", "skills^3", "job_description"],
                        }
                    }
                ],
                "filter": filters,
            }
        },
    }
    bm25_payload = _es_client().search(index_name=_ES_ALIAS, body=bm25_body)
    bm25_hits = _hits_from_response(bm25_payload)

    knn_body = {
        "size": _ES_KNN_CANDIDATES,
        "knn": {
            "field": "job_description_embedding",
            "query_vector": query_embedding,
            "k": _ES_KNN_CANDIDATES,
            "num_candidates": _ES_KNN_NUM_CANDIDATES,
            "filter": filters,
        },
    }
    knn_payload = _es_client().search(index_name=_ES_ALIAS, body=knn_body)
    knn_hits = _hits_from_response(knn_payload)

    fused_hits = _rrf_fuse_hits(bm25_hits, knn_hits)
    offset = (page - 1) * _PAGE_SIZE
    paged_hits = fused_hits[offset : offset + _PAGE_SIZE]
    jobs = [_job_metadata_from_hit(hit) for hit in paged_hits]
    total_results = len(fused_hits)
    return jobs, total_results, offset + len(jobs) < total_results


def _log_company_request(*, endpoint: str, company: str, status: int | None) -> None:
    _LOGGER.info(
        "[api-request] endpoint=%s company=%s status=%s",
        endpoint,
        company,
        status if status is not None else "-",
    )


@app.middleware("http")
async def translate_client_errors(request: Request, call_next: Any) -> JSONResponse | Any:
    try:
        return await call_next(request)
    except ValueError as exc:
        return JSONResponse(status_code=400, content={"detail": str(exc)})
    except requests.exceptions.HTTPError as exc:
        status_code = exc.response.status_code if exc.response is not None else None
        if status_code == 429:
            return JSONResponse(
                status_code=429,
                content={
                    "detail": "Upstream API rate-limited the request. Please retry in a few seconds."
                },
            )
        if isinstance(status_code, int):
            return JSONResponse(
                status_code=502,
                content={"detail": f"Upstream API HTTP error: {status_code}"},
            )
        return JSONResponse(
            status_code=502,
            content={"detail": "Upstream API HTTP error"},
        )
    except requests.exceptions.RequestException as exc:
        return JSONResponse(
            status_code=502,
            content={"detail": f"Upstream API network error: {exc}"},
        )


@app.on_event("startup")
async def startup_event() -> None:
    _LOGGER.info(
        "[api] startup db_url_configured=%s page_size=%s es_url_configured=%s features_url_configured=%s",
        bool(_DB_URL),
        _PAGE_SIZE,
        bool(_ES_URL),
        bool(_FEATURES_API_URL),
    )


@app.on_event("shutdown")
async def shutdown_event() -> None:
    return None


@app.post("/get_jobs", response_model=GetJobsResponse)
def get_jobs(payload: GetJobsRequest, request: Request) -> GetJobsResponse:
    run_id = _active_run_id()
    company = _resolve_company_filter(run_id, payload.company)
    page = max(1, int(payload.pagination_index))
    query = str(payload.query or "").strip()
    posted_within = _normalize_posted_within(payload.posted_within)

    try:
        if query:
            jobs, total_results, has_next_page = _search_jobs(
                company,
                query=query,
                page=page,
                posted_within=posted_within,
            )
        else:
            jobs, total_results, has_next_page = _browse_jobs(company, posted_within=posted_within, page=page)
    except requests.exceptions.RequestException as exc:
        raise HTTPException(status_code=502, detail=f"Search index unavailable: {exc}") from exc

    _log_company_request(endpoint="/get_jobs", company=company or "all", status=200)
    total_pages = max(1, math.ceil(total_results / _PAGE_SIZE)) if total_results > 0 else 1
    return GetJobsResponse(
        status=200,
        error=None,
        jobs=jobs,
        total_results=total_results,
        page_size=_PAGE_SIZE,
        total_pages=total_pages,
        pagination_index=page,
        has_next_page=has_next_page,
    )


@app.get("/get_companies", response_model=GetCompaniesResponse)
def get_companies() -> GetCompaniesResponse:
    run_id = _active_run_id()
    with _db_conn() as conn:
        rows = conn.execute(
            """
            SELECT company
            FROM companies
            WHERE run_id = %s
            ORDER BY company
            """,
            (run_id,),
        ).fetchall()
    companies = [str(row["company"]) for row in rows]
    return GetCompaniesResponse(status=200, error=None, companies=companies)


@app.post("/get_job_details", response_model=GetJobDetailsResponse)
def get_job_details(payload: GetJobDetailsRequest, request: Request) -> GetJobDetailsResponse:
    run_id = str(payload.runId).strip() if payload.runId else _active_run_id()
    company = _validate_company_in_run(run_id, payload.company)

    with _db_conn() as conn:
        row = conn.execute(
            """
            SELECT
              j.is_missing_details,
              j.external_job_id,
              j.title,
              j.details_url,
              j.apply_url,
              j.skills,
              j.posted_ts,
              d.job_description_path
            FROM jobs j
            LEFT JOIN job_details d
              ON d.run_id = j.run_id
             AND d.company = j.company
             AND d.external_job_id = j.external_job_id
            WHERE j.run_id = %s
              AND j.company = %s
              AND j.external_job_id = %s
            LIMIT 1
            """,
            (run_id, company, payload.job_id),
        ).fetchone()

    if row is None:
        raise HTTPException(
            status_code=404,
            detail=f"Job '{payload.job_id}' not found for company '{company}' in active run",
        )
    if bool(row["is_missing_details"]):
        raise HTTPException(
            status_code=404,
            detail=f"Job '{payload.job_id}' details are unavailable for company '{company}'",
        )

    _log_company_request(endpoint="/get_job_details", company=company, status=200)
    return GetJobDetailsResponse(
        status=200,
        error=None,
        jobDescription=_load_job_description(row["job_description_path"]),
        skills=_normalize_skills(row["skills"]),
        postedTs=_epoch_seconds(row["posted_ts"]),
        detailsUrl=row["details_url"],
    )
