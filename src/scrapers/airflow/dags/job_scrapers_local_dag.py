"""Local Airflow DAG for scraping jobs and staging/publishing DB snapshots."""

from __future__ import annotations

import json
import logging
import math
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

import requests
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.operators.python import get_current_context
from airflow.sensors.python import PythonSensor
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.pool import NullPool

from scrapers.airflow.clients.client_factory import build_client
from scrapers.airflow.clients.common.errors import RetryableUpstreamError
from scrapers.airflow.clients.common.request_policy import RequestPolicy
from scrapers.common.company_scopes import resolve_companies as resolve_companies_from_env
from scrapers.common.company_scopes import resolve_scopes as resolve_proxy_scopes_for_companies
from scrapers.common.env import env_bool, env_float, env_int, require_env
from scrapers.proxy.proxy_management_client import ProxyManagementClient

LOGGER = logging.getLogger(__name__)


def _resolve_companies() -> list[str]:
    return resolve_companies_from_env(require_env("JOBSEARCH_AIRFLOW_COMPANIES"))


def _resolve_proxy_scopes(companies: list[str]) -> list[str]:
    return resolve_proxy_scopes_for_companies(companies)


def _resolve_max_pages() -> int | None:
    raw = os.getenv("JOBSEARCH_AIRFLOW_MAX_PAGES")
    if raw is None:
        return 1
    try:
        parsed = int(raw.strip())
    except ValueError:
        return 1
    if parsed == 0:
        return None
    if parsed < 0:
        return 1
    return parsed


def _resolve_total_pages(response: Any, max_pages: int | None) -> int:
    total_results = getattr(response, "total_results", None)
    page_size = getattr(response, "page_size", None)

    if isinstance(total_results, int) and total_results >= 0 and isinstance(page_size, int) and page_size > 0:
        calculated = max(1, math.ceil(total_results / page_size))
        if max_pages is None:
            return calculated
        return min(calculated, max_pages)

    has_next_page = bool(getattr(response, "has_next_page", False))
    if has_next_page:
        if max_pages is None:
            raise ValueError(
                "Cannot resolve total pages from response metadata with JOBSEARCH_AIRFLOW_MAX_PAGES=0; "
                "set a positive page cap."
            )
        return max_pages
    return 1


def _call_with_proxy_retry(
    *,
    fn: Callable[[], Any],
    attempts: int,
    backoff_seconds: float,
    operation_context: str,
) -> Any:
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        LOGGER.info(
            "proxy_attempt_start context=%s attempt=%s/%s",
            operation_context,
            attempt,
            attempts,
        )
        try:
            result = fn()
            return result
        except Exception as exc:
            last_error = exc
            LOGGER.warning(
                "proxy_attempt_failed context=%s attempt=%s/%s error=%s",
                operation_context,
                attempt,
                attempts,
                f"{type(exc).__name__}: {exc}",
            )
            if attempt >= attempts:
                break
            if isinstance(
                exc,
                (
                    requests.exceptions.RequestException,
                    requests.exceptions.ProxyError,
                    RetryableUpstreamError,
                ),
            ):
                time.sleep(backoff_seconds * attempt)
                continue
            break
    if last_error is not None:
        raise last_error
    raise RuntimeError("Proxy retry exhausted without captured error")


def _normalize_db_url(raw_url: str) -> str:
    normalized = raw_url.strip()
    if not normalized:
        raise ValueError("DB URL must be non-empty")
    # Airflow commonly uses postgresql+psycopg2://; keep it SQLAlchemy-compatible.
    return normalized


def _db_engine(db_url: str) -> Engine:
    return create_engine(_normalize_db_url(db_url), poolclass=NullPool, future=True)


def _to_json(value: Any) -> str | None:
    if value is None:
        return None
    return json.dumps(value, separators=(",", ":"))


@dag(
    dag_id="job_scrapers_local",
    description="Run mapped job scraping pipeline and publish DB snapshot pointer.",
    schedule="0 */4 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={
        "owner": "jobsearch",
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["jobsearch", "scrapers", "local"],
)
def job_scrapers_local_dag() -> None:
    companies = _resolve_companies()
    proxy_scopes = _resolve_proxy_scopes(companies)
    max_pages = _resolve_max_pages()
    proxy_retry_attempts = env_int("JOBSEARCH_AIRFLOW_PROXY_RETRY_ATTEMPTS", default=6, minimum=1)
    proxy_retry_backoff_seconds = env_float(
        "JOBSEARCH_AIRFLOW_PROXY_RETRY_BACKOFF_SECONDS", default=2.0, minimum=0.2
    )
    client_timeout_seconds = env_int(
        "JOBSEARCH_AIRFLOW_CLIENT_TIMEOUT_SECONDS",
        default=env_int("JOBSEARCH_AIRFLOW_META_TIMEOUT_SECONDS", default=20, minimum=1),
        minimum=1,
    )
    client_max_retries = env_int("JOBSEARCH_AIRFLOW_CLIENT_MAX_RETRIES", default=5, minimum=1)
    client_backoff_factor = env_float("JOBSEARCH_AIRFLOW_CLIENT_BACKOFF_FACTOR", default=0.5, minimum=0.0)
    client_max_backoff_seconds = env_float(
        "JOBSEARCH_AIRFLOW_CLIENT_MAX_BACKOFF_SECONDS",
        default=6.0,
        minimum=0.0,
    )
    client_backoff_jitter = env_bool("JOBSEARCH_AIRFLOW_CLIENT_BACKOFF_JITTER", default=False)
    fail_on_company_error = env_bool("JOBSEARCH_AIRFLOW_FAIL_ON_COMPANY_ERROR", default=False)

    proxy_api_url = require_env("JOBSEARCH_PROXY_API_URL")
    proxy_api_timeout_seconds = env_float("JOBSEARCH_PROXY_API_TIMEOUT_SECONDS", default=10.0, minimum=0.2)
    proxy_sensor_poke_seconds = env_int("JOBSEARCH_AIRFLOW_PROXY_SENSOR_POKE_SECONDS", default=20, minimum=1)
    proxy_sensor_timeout_seconds = env_int("JOBSEARCH_AIRFLOW_PROXY_SENSOR_TIMEOUT_SECONDS", default=1800, minimum=5)
    proxy_min_available_total = env_int("JOBSEARCH_AIRFLOW_PROXY_MIN_AVAILABLE_TOTAL", default=24, minimum=1)
    proxy_min_available_per_scope = env_int("JOBSEARCH_AIRFLOW_PROXY_MIN_AVAILABLE_PER_SCOPE", default=3, minimum=1)
    proxy_sensor_soft_fail = env_bool("JOBSEARCH_AIRFLOW_PROXY_SENSOR_SOFT_FAIL", default=False)

    db_url = require_env("JOBSEARCH_DB_URL")

    def _build_proxy_management_client() -> ProxyManagementClient:
        return ProxyManagementClient(
            base_url=proxy_api_url,
            timeout_seconds=proxy_api_timeout_seconds,
        )

    def _build_default_request_policy() -> RequestPolicy:
        return RequestPolicy(
            timeout_seconds=float(client_timeout_seconds),
            max_retries=client_max_retries,
            backoff_factor=client_backoff_factor,
            max_backoff_seconds=client_max_backoff_seconds,
            jitter=client_backoff_jitter,
        )

    def _proxy_capacity_ready() -> bool:
        if not proxy_scopes:
            LOGGER.warning("proxy_capacity_check no_scopes_configured")
            return False

        proxy_management_client = _build_proxy_management_client()

        total_available = 0
        total_inuse = 0
        total_blocked = 0
        scope_available: dict[str, int] = {}
        scope_shortages: list[str] = []

        for scope in proxy_scopes:
            try:
                snapshot = proxy_management_client.sizes(scope=scope)
            except Exception as exc:
                LOGGER.warning(
                    "proxy_capacity_check_failed scope=%s error=%s",
                    scope,
                    f"{type(exc).__name__}: {exc}",
                )
                return False

            available = int(snapshot.get("available", 0))
            inuse = int(snapshot.get("inuse", 0))
            blocked = int(snapshot.get("blocked", 0))

            scope_available[scope] = available
            total_available += available
            total_inuse += inuse
            total_blocked += blocked

            if available < proxy_min_available_per_scope:
                scope_shortages.append(scope)

        total_ready = total_available >= proxy_min_available_total
        scopes_ready = not scope_shortages
        ready = total_ready and scopes_ready

        LOGGER.info(
            "proxy_capacity_check ready=%s total_available=%s total_inuse=%s total_blocked=%s "
            "min_total=%s min_per_scope=%s shortages=%s scope_available=%s",
            ready,
            total_available,
            total_inuse,
            total_blocked,
            proxy_min_available_total,
            proxy_min_available_per_scope,
            ",".join(scope_shortages) if scope_shortages else "-",
            json.dumps(scope_available, sort_keys=True),
        )
        return ready

    @task(task_id="create_publish_run")
    def create_publish_run() -> dict[str, str]:
        context = get_current_context()
        dag_run = context["dag_run"]
        run_id = str(dag_run.run_id)
        logical_date = context.get("logical_date")
        if isinstance(logical_date, datetime):
            version_ts = logical_date.astimezone(timezone.utc)
        else:
            version_ts = datetime.now(timezone.utc)

        engine = _db_engine(db_url)
        try:
            with engine.begin() as conn:
                conn.execute(
                    text(
                        """
                        INSERT INTO publish_runs (
                            run_id,
                            version_ts,
                            status,
                            db_ready,
                            db_published_at,
                            db_error_message,
                            es_ready,
                            es_published_at,
                            es_error_message,
                            created_at,
                            updated_at
                        )
                        VALUES (
                            :run_id,
                            :version_ts,
                            'in_progress',
                            FALSE,
                            NULL,
                            NULL,
                            FALSE,
                            NULL,
                            NULL,
                            now(),
                            now()
                        )
                        ON CONFLICT (run_id) DO UPDATE
                        SET version_ts = EXCLUDED.version_ts,
                            status = 'in_progress',
                            db_ready = FALSE,
                            db_published_at = NULL,
                            db_error_message = NULL,
                            es_ready = FALSE,
                            es_published_at = NULL,
                            es_error_message = NULL,
                            updated_at = now()
                        """
                    ),
                    {
                        "run_id": run_id,
                        "version_ts": version_ts,
                    },
                )
        finally:
            engine.dispose()

        return {"run_id": run_id, "version_ts": version_ts.isoformat()}

    @task(task_id="stage_companies")
    def stage_companies(run_info: dict[str, str]) -> None:
        run_id = str(run_info["run_id"])
        version_ts = datetime.fromisoformat(str(run_info["version_ts"]))

        rows = [
            {
                "run_id": run_id,
                "version_ts": version_ts,
                "company": company,
                "display_name": company.capitalize(),
            }
            for company in companies
        ]

        engine = _db_engine(db_url)
        try:
            with engine.begin() as conn:
                conn.execute(
                    text(
                        """
                        INSERT INTO companies (
                            run_id,
                            version_ts,
                            company,
                            display_name,
                            updated_at
                        )
                        VALUES (
                            :run_id,
                            :version_ts,
                            :company,
                            :display_name,
                            now()
                        )
                        ON CONFLICT (run_id, company) DO UPDATE
                        SET version_ts = EXCLUDED.version_ts,
                            display_name = EXCLUDED.display_name,
                            updated_at = now()
                        """
                    ),
                    rows,
                )
        finally:
            engine.dispose()

    @task(
        task_id="jobs_get_first_page",
        map_index_template="{{ task.op_kwargs.get('company', ti.map_index) }}",
    )
    def get_first_page(company: str) -> dict[str, Any]:
        proxy_management_client = _build_proxy_management_client()
        scraper_client = build_client(
            company=company,
            proxy_management_client=proxy_management_client,
            default_request_policy=_build_default_request_policy(),
        )

        try:
            response = _call_with_proxy_retry(
                fn=lambda: scraper_client.get_jobs(page=1),
                attempts=proxy_retry_attempts,
                backoff_seconds=proxy_retry_backoff_seconds,
                operation_context=f"{company}:get_jobs:page=1",
            )
            pages_to_fetch = 1 if company == "meta" else _resolve_total_pages(response, max_pages=max_pages)
            return {
                "company": company,
                "pages_to_fetch": int(pages_to_fetch),
                "success": True,
                "error": None,
            }
        except Exception as exc:
            if fail_on_company_error:
                raise
            return {
                "company": company,
                "pages_to_fetch": 0,
                "success": False,
                "error": f"{type(exc).__name__}: {exc}",
            }

    @task(task_id="jobs_build_page_requests")
    def build_page_requests(first_pages: list[dict[str, Any]]) -> list[dict[str, Any]]:
        pages_by_company: dict[str, int] = {}
        company_order: list[str] = []
        for payload in first_pages:
            company = str(payload.get("company", "")).strip()
            pages_to_fetch = int(payload.get("pages_to_fetch", 0) or 0)
            if not company:
                continue
            if company not in pages_by_company:
                company_order.append(company)
            pages_by_company[company] = max(0, pages_to_fetch)

        requests_out: list[dict[str, Any]] = []
        max_company_pages = max(pages_by_company.values(), default=0)
        for page in range(1, max_company_pages + 1):
            for company in company_order:
                if pages_by_company.get(company, 0) >= page:
                    requests_out.append({"company": company, "page": page})
        return requests_out

    @task(
        task_id="jobs_get_page",
        map_index_template="{{ ti.xcom_pull(task_ids='jobs_build_page_requests')[ti.map_index]['company'] }}-p{{ ti.xcom_pull(task_ids='jobs_build_page_requests')[ti.map_index]['page'] }}",
    )
    def get_jobs_page(run_info: dict[str, str], company: str, page: int) -> dict[str, Any]:
        run_id = str(run_info["run_id"])
        version_ts = datetime.fromisoformat(str(run_info["version_ts"]))
        proxy_management_client = _build_proxy_management_client()
        scraper_client = build_client(
            company=company,
            proxy_management_client=proxy_management_client,
            default_request_policy=_build_default_request_policy(),
        )

        try:
            response = _call_with_proxy_retry(
                fn=lambda: scraper_client.get_jobs(page=page),
                attempts=proxy_retry_attempts,
                backoff_seconds=proxy_retry_backoff_seconds,
                operation_context=f"{company}:get_jobs:page={page}",
            )

            jobs_payload: list[dict[str, Any]] = []
            for job in (response.jobs or []):
                raw_id = getattr(job, "id", None)
                if not raw_id:
                    continue
                job_id = str(raw_id).strip()
                if not job_id:
                    continue
                locations = list(getattr(job, "locations", []) or [])
                city = state = country = None
                if locations:
                    loc = locations[0]
                    city = str(getattr(loc, "city", "") or "").strip() or None
                    state = str(getattr(loc, "state", "") or "").strip() or None
                    country = str(getattr(loc, "country", "") or "").strip() or None
                posted_ts = getattr(job, "postedTs", None)
                jobs_payload.append(
                    {
                        "job_id": job_id,
                        "title": str(getattr(job, "name", "") or "").strip() or None,
                        "details_url": str(getattr(job, "detailsUrl", "") or "").strip() or None,
                        "apply_url": str(getattr(job, "applyUrl", "") or "").strip() or None,
                        "city": city,
                        "state": state,
                        "country": country,
                        "posted_ts": int(posted_ts) if isinstance(posted_ts, int) else None,
                    }
                )

            rows: list[dict[str, Any]] = []
            for job in jobs_payload:
                posted_ts_raw = job.get("posted_ts")
                rows.append(
                    {
                        "run_id": run_id,
                        "version_ts": version_ts,
                        "company": company,
                        "external_job_id": str(job.get("job_id", "")).strip(),
                        "title": job.get("title"),
                        "details_url": job.get("details_url"),
                        "apply_url": job.get("apply_url"),
                        "city": job.get("city"),
                        "state": job.get("state"),
                        "country": job.get("country"),
                        "posted_ts": (
                            datetime.fromtimestamp(int(posted_ts_raw), tz=timezone.utc)
                            if isinstance(posted_ts_raw, int)
                            else None
                        ),
                    }
                )
            rows = [row for row in rows if row["external_job_id"]]

            if rows:
                engine = _db_engine(db_url)
                try:
                    with engine.begin() as conn:
                        conn.execute(
                            text(
                                """
                                INSERT INTO jobs (
                                    run_id,
                                    version_ts,
                                    company,
                                    external_job_id,
                                    title,
                                    details_url,
                                    apply_url,
                                    city,
                                    state,
                                    country,
                                    posted_ts,
                                    is_missing_details,
                                    updated_at
                                )
                                VALUES (
                                    :run_id,
                                    :version_ts,
                                    :company,
                                    :external_job_id,
                                    :title,
                                    :details_url,
                                    :apply_url,
                                    :city,
                                    :state,
                                    :country,
                                    :posted_ts,
                                    FALSE,
                                    now()
                                )
                                ON CONFLICT (run_id, company, external_job_id) DO UPDATE
                                SET version_ts = EXCLUDED.version_ts,
                                    title = EXCLUDED.title,
                                    details_url = EXCLUDED.details_url,
                                    apply_url = EXCLUDED.apply_url,
                                    city = EXCLUDED.city,
                                    state = EXCLUDED.state,
                                    country = EXCLUDED.country,
                                    posted_ts = EXCLUDED.posted_ts,
                                    is_missing_details = FALSE,
                                    updated_at = now()
                                """
                            ),
                            rows,
                        )
                finally:
                    engine.dispose()

            return {
                "company": company,
                "page": page,
                "jobs_seen": len(jobs_payload),
                "job_ids": [item["job_id"] for item in jobs_payload],
                "jobs_written": len(rows),
                "error": response.error,
                "success": True,
            }
        except Exception as exc:
            if fail_on_company_error:
                raise
            return {
                "company": company,
                "page": page,
                "jobs_seen": 0,
                "job_ids": [],
                "jobs_written": 0,
                "error": f"{type(exc).__name__}: {exc}",
                "success": False,
            }

    @task(task_id="jobs_build_detail_requests")
    def build_detail_requests(page_results: list[dict[str, Any]]) -> list[dict[str, str]]:
        job_ids_by_company: dict[str, list[str]] = {}
        company_order: list[str] = []

        for page in page_results:
            if not bool(page.get("success", False)):
                continue
            company = str(page.get("company", "")).strip()
            if not company:
                continue
            if company not in job_ids_by_company:
                company_order.append(company)
            company_ids = job_ids_by_company.setdefault(company, [])
            for job_id in page.get("job_ids", []):
                if isinstance(job_id, str) and job_id and job_id not in company_ids:
                    company_ids.append(job_id)

        detail_requests: list[dict[str, str]] = []
        max_company_jobs = max((len(ids) for ids in job_ids_by_company.values()), default=0)
        for idx in range(max_company_jobs):
            for company in company_order:
                ids = job_ids_by_company.get(company, [])
                if idx < len(ids):
                    detail_requests.append({"company": company, "job_id": ids[idx]})
        return detail_requests

    @task(
        task_id="jobs_get_details",
        map_index_template="{{ ti.xcom_pull(task_ids='jobs_build_detail_requests')[ti.map_index]['company'] }}-{{ ti.xcom_pull(task_ids='jobs_build_detail_requests')[ti.map_index]['job_id'] }}",
    )
    def get_job_details(run_info: dict[str, str], company: str, job_id: str) -> dict[str, Any]:
        run_id = str(run_info["run_id"])
        version_ts = datetime.fromisoformat(str(run_info["version_ts"]))
        proxy_management_client = _build_proxy_management_client()
        scraper_client = build_client(
            company=company,
            proxy_management_client=proxy_management_client,
            default_request_policy=_build_default_request_policy(),
        )

        try:
            response = _call_with_proxy_retry(
                fn=lambda: scraper_client.get_job_details(job_id=job_id),
                attempts=proxy_retry_attempts,
                backoff_seconds=proxy_retry_backoff_seconds,
                operation_context=f"{company}:get_job_details:job_id={job_id}",
            )
            status = int(response.status) if response.status is not None else 500
            if status == 404:
                engine = _db_engine(db_url)
                try:
                    with engine.begin() as conn:
                        conn.execute(
                            text(
                                """
                                UPDATE jobs
                                SET is_missing_details = TRUE,
                                    updated_at = now()
                                WHERE run_id = :run_id
                                  AND company = :company
                                  AND external_job_id = :external_job_id
                                """
                            ),
                            {
                                "run_id": run_id,
                                "company": company,
                                "external_job_id": job_id,
                            },
                        )
                finally:
                    engine.dispose()
                return {
                    "company": company,
                    "job_id": job_id,
                    "success": True,
                    "error": response.error,
                    "job_detail_written": False,
                }
            success = 200 <= status < 300 and response.job is not None
            job_payload: dict[str, Any] | None = None
            if success and response.job is not None:
                pay_details = response.job.payDetails.model_dump(mode="json") if response.job.payDetails else None
                posted_ts = getattr(response.job, "postedTs", None)
                job_payload = {
                    "job_description": response.job.jobDescription,
                    "minimum_qualifications": list(response.job.minimumQualifications or []),
                    "preferred_qualifications": list(response.job.preferredQualifications or []),
                    "responsibilities": list(response.job.responsibilities or []),
                    "pay_details": pay_details,
                    "posted_ts_from_details": int(posted_ts) if isinstance(posted_ts, int) else None,
                }

                detail_row = {
                    "run_id": run_id,
                    "version_ts": version_ts,
                    "company": company,
                    "external_job_id": job_id,
                    "job_description": job_payload.get("job_description"),
                    "minimum_qualifications": _to_json(job_payload.get("minimum_qualifications")),
                    "preferred_qualifications": _to_json(job_payload.get("preferred_qualifications")),
                    "responsibilities": _to_json(job_payload.get("responsibilities")),
                    "pay_details": _to_json(job_payload.get("pay_details")),
                }
                posted_ts_raw = job_payload.get("posted_ts_from_details")

                engine = _db_engine(db_url)
                try:
                    with engine.begin() as conn:
                        conn.execute(
                            text(
                                """
                                INSERT INTO job_details (
                                    run_id,
                                    version_ts,
                                    company,
                                    external_job_id,
                                    job_description,
                                    minimum_qualifications,
                                    preferred_qualifications,
                                    responsibilities,
                                    pay_details,
                                    updated_at
                                )
                                VALUES (
                                    :run_id,
                                    :version_ts,
                                    :company,
                                    :external_job_id,
                                    :job_description,
                                    CAST(:minimum_qualifications AS JSONB),
                                    CAST(:preferred_qualifications AS JSONB),
                                    CAST(:responsibilities AS JSONB),
                                    CAST(:pay_details AS JSONB),
                                    now()
                                )
                                ON CONFLICT (run_id, company, external_job_id) DO UPDATE
                                SET version_ts = EXCLUDED.version_ts,
                                    job_description = EXCLUDED.job_description,
                                    minimum_qualifications = EXCLUDED.minimum_qualifications,
                                    preferred_qualifications = EXCLUDED.preferred_qualifications,
                                    responsibilities = EXCLUDED.responsibilities,
                                    pay_details = EXCLUDED.pay_details,
                                    updated_at = now()
                                """
                            ),
                            detail_row,
                        )

                        if isinstance(posted_ts_raw, int):
                            conn.execute(
                                text(
                                    """
                                    UPDATE jobs
                                    SET posted_ts = COALESCE(jobs.posted_ts, :posted_ts),
                                        updated_at = now()
                                    WHERE run_id = :run_id
                                      AND company = :company
                                      AND external_job_id = :external_job_id
                                    """
                                ),
                                {
                                    "run_id": run_id,
                                    "company": company,
                                    "external_job_id": job_id,
                                    "posted_ts": datetime.fromtimestamp(posted_ts_raw, tz=timezone.utc),
                                },
                            )
                finally:
                    engine.dispose()
            return {
                "company": company,
                "job_id": job_id,
                "success": success,
                "error": response.error,
                "job_detail_written": bool(success and job_payload is not None),
            }
        except Exception as exc:
            if fail_on_company_error:
                raise
            return {
                "company": company,
                "job_id": job_id,
                "success": False,
                "error": f"{type(exc).__name__}: {exc}",
                "job_detail_written": False,
            }

    @task(task_id="verify_db_consistency")
    def verify_db_consistency(
        run_info: dict[str, str],
        page_results: list[dict[str, Any]],
        detail_requests: list[dict[str, str]],
    ) -> dict[str, Any]:
        run_id = str(run_info["run_id"])

        expected_jobs_ids_by_company: dict[str, set[str]] = {company: set() for company in companies}
        for page in page_results:
            if not bool(page.get("success", False)):
                continue
            company = str(page.get("company", "")).strip()
            if company not in expected_jobs_ids_by_company:
                continue
            for job_id in page.get("job_ids", []):
                if isinstance(job_id, str) and job_id:
                    expected_jobs_ids_by_company[company].add(job_id)
        expected_jobs_by_company: dict[str, int] = {
            company: len(job_ids) for company, job_ids in expected_jobs_ids_by_company.items()
        }

        expected_detail_jobs_by_company: dict[str, int] = {company: 0 for company in companies}
        for item in detail_requests:
            company = str(item.get("company", "")).strip()
            if company in expected_detail_jobs_by_company:
                expected_detail_jobs_by_company[company] += 1

        jobs_count_by_company: dict[str, int] = {company: 0 for company in companies}
        missing_details_by_company: dict[str, int] = {company: 0 for company in companies}
        details_count_by_company: dict[str, int] = {company: 0 for company in companies}
        missing_description_by_company: dict[str, int] = {company: 0 for company in companies}

        engine = _db_engine(db_url)
        try:
            with engine.begin() as conn:
                jobs_rows = conn.execute(
                    text(
                        """
                        SELECT
                          company,
                          COUNT(*) FILTER (WHERE NOT is_missing_details) AS cnt,
                          COUNT(*) FILTER (WHERE is_missing_details) AS missing_cnt
                        FROM jobs
                        WHERE run_id = :run_id
                        GROUP BY company
                        """
                    ),
                    {"run_id": run_id},
                ).mappings()
                for row in jobs_rows:
                    company = str(row["company"])
                    if company in jobs_count_by_company:
                        jobs_count_by_company[company] = int(row["cnt"])
                        missing_details_by_company[company] = int(row["missing_cnt"])

                details_rows = conn.execute(
                    text(
                        """
                        SELECT d.company, COUNT(*) AS cnt
                        FROM job_details d
                        JOIN jobs j
                          ON j.run_id = d.run_id
                         AND j.company = d.company
                         AND j.external_job_id = d.external_job_id
                        WHERE d.run_id = :run_id
                          AND j.is_missing_details = FALSE
                        GROUP BY d.company
                        """
                    ),
                    {"run_id": run_id},
                ).mappings()
                for row in details_rows:
                    company = str(row["company"])
                    if company in details_count_by_company:
                        details_count_by_company[company] = int(row["cnt"])

                missing_desc_rows = conn.execute(
                    text(
                        """
                        SELECT d.company, COUNT(*) AS cnt
                        FROM job_details d
                        JOIN jobs j
                          ON j.run_id = d.run_id
                         AND j.company = d.company
                         AND j.external_job_id = d.external_job_id
                        WHERE d.run_id = :run_id
                          AND j.is_missing_details = FALSE
                          AND (d.job_description IS NULL OR btrim(d.job_description) = '')
                        GROUP BY d.company
                        """
                    ),
                    {"run_id": run_id},
                ).mappings()
                for row in missing_desc_rows:
                    company = str(row["company"])
                    if company in missing_description_by_company:
                        missing_description_by_company[company] = int(row["cnt"])
        finally:
            engine.dispose()

        violations: list[str] = []
        for company in companies:
            expected_jobs = expected_jobs_by_company.get(company, 0)
            expected_detail_jobs = expected_detail_jobs_by_company.get(company, 0)
            jobs_count = jobs_count_by_company.get(company, 0)
            missing_details = missing_details_by_company.get(company, 0)
            details_count = details_count_by_company.get(company, 0)
            missing_desc = missing_description_by_company.get(company, 0)
            expected_jobs_excluding_missing = max(0, expected_jobs - missing_details)
            expected_details_excluding_missing = max(0, expected_detail_jobs - missing_details)

            # Rule 1: jobs count per company must match expected count from scraped IDs.
            if jobs_count != expected_jobs_excluding_missing:
                violations.append(
                    "company="
                    f"{company} rule=jobs_expected_mismatch expected={expected_jobs_excluding_missing} "
                    f"actual={jobs_count} missing_details={missing_details}"
                )

            # Rule 2: job_details count must match expected detail attempts, excluding true missing-details rows.
            if details_count != expected_details_excluding_missing:
                violations.append(
                    "company="
                    f"{company} rule=details_count_mismatch expected={expected_details_excluding_missing} "
                    f"actual={details_count} expected_detail_jobs={expected_detail_jobs} "
                    f"missing_details={missing_details}"
                )

            # Rule 3: all job_details must have non-empty job_description.
            if missing_desc > 0:
                violations.append(
                    f"company={company} rule=missing_job_description count={missing_desc}"
                )

        if violations:
            raise AirflowFailException(
                f"Run {run_id} DB consistency verification failed: {' | '.join(violations[:10])}"
            )

        return {
            "run_id": run_id,
            "verified": True,
            "jobs_count_by_company": jobs_count_by_company,
            "details_count_by_company": details_count_by_company,
        }

    @task(task_id="update_publish_run")
    def update_publish_run(
        run_info: dict[str, str],
        first_pages: list[dict[str, Any]],
        page_results: list[dict[str, Any]],
        detail_results: list[dict[str, Any]],
    ) -> dict[str, Any]:
        run_id = str(run_info["run_id"])

        errors: list[str] = []
        errors.extend(
            str(item.get("error"))
            for item in first_pages
            if not bool(item.get("success", False)) and item.get("error")
        )
        errors.extend(
            str(item.get("error"))
            for item in page_results
            if not bool(item.get("success", False)) and item.get("error")
        )
        errors.extend(
            str(item.get("error"))
            for item in detail_results
            if not bool(item.get("success", False)) and item.get("error")
        )

        failed = len(errors) > 0
        status = "failed" if failed else "succeeded"
        db_ready = not failed
        db_error_message = " | ".join(errors[:5]) if failed else None

        engine = _db_engine(db_url)
        try:
            with engine.begin() as conn:
                conn.execute(
                    text(
                        """
                        UPDATE publish_runs
                        SET status = :status,
                            db_ready = :db_ready,
                            db_error_message = :db_error_message,
                            updated_at = now()
                        WHERE run_id = :run_id
                        """
                    ),
                    {
                        "run_id": run_id,
                        "status": status,
                        "db_ready": db_ready,
                        "db_error_message": db_error_message,
                    },
                )
        finally:
            engine.dispose()

        if failed:
            raise AirflowFailException(
                f"Run {run_id} failed due to scrape errors; first_error={errors[0] if errors else 'unknown'}"
            )

        return {
            "run_id": run_id,
            "status": status,
            "db_ready": db_ready,
            "error_count": len(errors),
        }

    @task(task_id="publish_db_pointer")
    def publish_db_pointer(run_info: dict[str, str], publish_state: dict[str, Any]) -> dict[str, Any]:
        run_id = str(run_info["run_id"])
        if str(publish_state.get("status")) != "succeeded":
            LOGGER.warning(
                "publish_db_pointer skipped run_id=%s status=%s",
                run_id,
                publish_state.get("status"),
            )
            return {"published": False, "run_id": run_id}

        engine = _db_engine(db_url)
        try:
            with engine.begin() as conn:
                conn.execute(
                    text(
                        """
                        INSERT INTO publication_pointers (namespace, run_id, updated_at)
                        VALUES ('jobs_catalog', :run_id, now())
                        ON CONFLICT (namespace) DO UPDATE
                        SET run_id = EXCLUDED.run_id,
                            updated_at = now()
                        """
                    ),
                    {"run_id": run_id},
                )
                conn.execute(
                    text(
                        """
                        UPDATE publish_runs
                        SET db_published_at = now(),
                            updated_at = now()
                        WHERE run_id = :run_id
                        """
                    ),
                    {"run_id": run_id},
                )
        finally:
            engine.dispose()

        return {"published": True, "run_id": run_id}

    wait_for_proxy_capacity = PythonSensor(
        task_id="wait_for_proxy_capacity",
        python_callable=_proxy_capacity_ready,
        mode="reschedule",
        poke_interval=proxy_sensor_poke_seconds,
        timeout=proxy_sensor_timeout_seconds,
        soft_fail=proxy_sensor_soft_fail,
    )

    run_info = create_publish_run()
    wait_for_proxy_capacity >> run_info
    companies_staged = stage_companies(run_info)

    first_pages = get_first_page.expand(company=companies)
    companies_staged >> first_pages
    page_requests = build_page_requests(first_pages)
    page_results = get_jobs_page.partial(run_info=run_info).expand_kwargs(page_requests)

    detail_requests = build_detail_requests(page_results)
    detail_results = get_job_details.partial(run_info=run_info).expand_kwargs(detail_requests)

    verification_state = verify_db_consistency(run_info, page_results, detail_requests)
    detail_results >> verification_state
    publish_state = update_publish_run(run_info, first_pages, page_results, detail_results)
    verification_state >> publish_state
    publish_db_pointer(run_info, publish_state)


job_scrapers_local_dag()
