"""FastAPI backend exposing unified job APIs backed by the published DB snapshot."""

from __future__ import annotations

import logging
import math
from datetime import datetime, timezone
from typing import Any

import psycopg
import requests
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from psycopg.rows import dict_row

from scrapers.common.env import require_env
from web.backend.schemas import (
    GetCompaniesResponse,
    GetJobDetailsRequest,
    GetJobDetailsResponse,
    GetJobsRequest,
    GetJobsResponse,
    JobDetailsSchema,
    JobMetadata,
    Location,
    PayDetails,
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


def _db_conn() -> psycopg.Connection[Any]:
    return psycopg.connect(_DB_URL, row_factory=dict_row)


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
    _LOGGER.info("[db] startup db_url_configured=%s page_size=%s", bool(_DB_URL), _PAGE_SIZE)


@app.on_event("shutdown")
async def shutdown_event() -> None:
    return None


@app.post("/get_jobs", response_model=GetJobsResponse)
def get_jobs(payload: GetJobsRequest, request: Request) -> GetJobsResponse:
    run_id = _active_run_id()
    company = _validate_company_in_run(run_id, payload.company)

    page = max(1, int(payload.pagination_index))
    offset = (page - 1) * _PAGE_SIZE

    with _db_conn() as conn:
        total_row = conn.execute(
            """
            SELECT COUNT(*) AS total
            FROM jobs
            WHERE run_id = %s
              AND company = %s
              AND is_missing_details = FALSE
            """,
            (run_id, company),
        ).fetchone()
        total_results = int(total_row["total"]) if total_row is not None else 0

        rows = conn.execute(
            """
            SELECT
              external_job_id,
              title,
              details_url,
              apply_url,
              city,
              state,
              country,
              posted_ts
            FROM jobs
            WHERE run_id = %s
              AND company = %s
              AND is_missing_details = FALSE
            ORDER BY posted_ts DESC NULLS LAST, external_job_id
            LIMIT %s OFFSET %s
            """,
            (run_id, company, _PAGE_SIZE, offset),
        ).fetchall()

    jobs: list[JobMetadata] = []
    for row in rows:
        jobs.append(
            JobMetadata(
                id=str(row["external_job_id"]),
                name=row["title"],
                company=company,
                locations=[
                    Location(
                        country=str(row["country"] or ""),
                        state=str(row["state"] or ""),
                        city=str(row["city"] or ""),
                    )
                ],
                postedTs=_epoch_seconds(row["posted_ts"]),
                applyUrl=row["apply_url"],
                detailsUrl=row["details_url"],
            )
        )

    _log_company_request(endpoint="/get_jobs", company=company, status=200)
    total_pages = max(1, math.ceil(total_results / _PAGE_SIZE)) if total_results > 0 else 1
    return GetJobsResponse(
        status=200,
        error=None,
        jobs=jobs,
        total_results=total_results,
        page_size=_PAGE_SIZE,
        total_pages=total_pages,
        pagination_index=page,
        has_next_page=offset + len(jobs) < total_results,
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
    run_id = _active_run_id()
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
              j.posted_ts,
              d.job_description,
              d.minimum_qualifications,
              d.preferred_qualifications,
              d.responsibilities,
              d.pay_details
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

    job = JobDetailsSchema(
        id=str(row["external_job_id"]),
        name=row["title"],
        company=company,
        jobDescription=row["job_description"],
        postedTs=_epoch_seconds(row["posted_ts"]),
        minimumQualifications=list(row["minimum_qualifications"] or []),
        preferredQualifications=list(row["preferred_qualifications"] or []),
        responsibilities=list(row["responsibilities"] or []),
        payDetails=PayDetails.model_validate(row["pay_details"]) if row["pay_details"] else None,
        applyUrl=row["apply_url"],
        detailsUrl=row["details_url"],
    )

    _log_company_request(endpoint="/get_job_details", company=company, status=200)
    return GetJobDetailsResponse(status=200, error=None, job=job)
