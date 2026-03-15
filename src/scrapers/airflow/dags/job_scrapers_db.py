"""Database helpers for the local job scrapers DAG."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.pool import NullPool


def _normalize_db_url(raw_url: str) -> str:
    normalized = raw_url.strip()
    if not normalized:
        raise ValueError("DB URL must be non-empty")
    return normalized


def _db_engine(db_url: str) -> Engine:
    return create_engine(_normalize_db_url(db_url), poolclass=NullPool, future=True)


def upsert_publish_run_in_progress(db_url: str, *, run_id: str, version_ts: datetime) -> None:
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


def upsert_companies(db_url: str, *, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
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


def upsert_jobs(db_url: str, *, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
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


def mark_missing_details(
    db_url: str,
    *,
    run_id: str,
    company: str,
    external_job_id: str,
) -> None:
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
                    "external_job_id": external_job_id,
                },
            )
    finally:
        engine.dispose()


def upsert_job_details(
    db_url: str,
    *,
    detail_row: dict[str, Any],
    posted_ts: datetime | None,
) -> None:
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
                        job_description_path,
                        updated_at
                    )
                    VALUES (
                        :run_id,
                        :version_ts,
                        :company,
                        :external_job_id,
                        :job_description_path,
                        now()
                    )
                    ON CONFLICT (run_id, company, external_job_id) DO UPDATE
                    SET version_ts = EXCLUDED.version_ts,
                        job_description_path = EXCLUDED.job_description_path,
                        updated_at = now()
                    """
                ),
                detail_row,
            )

            if posted_ts is not None:
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
                        "run_id": detail_row["run_id"],
                        "company": detail_row["company"],
                        "external_job_id": detail_row["external_job_id"],
                        "posted_ts": posted_ts,
                    },
                )
    finally:
        engine.dispose()


def fetch_latest_published_run_id(
    db_url: str,
    *,
    exclude_run_id: str | None = None,
) -> str | None:
    engine = _db_engine(db_url)
    try:
        with engine.begin() as conn:
            row = conn.execute(
                text(
                    """
                    SELECT run_id
                    FROM publication_pointers
                    WHERE namespace = 'jobs_catalog'
                      AND (:exclude_run_id IS NULL OR run_id <> :exclude_run_id)
                    LIMIT 1
                    """
                ),
                {"exclude_run_id": exclude_run_id},
            ).mappings()
            first_row = next(iter(row), None)
            if first_row is None:
                return None
            run_id = first_row.get("run_id")
            if run_id is None:
                return None
            return str(run_id)
    finally:
        engine.dispose()


def copy_job_details_from_run(
    db_url: str,
    *,
    source_run_id: str,
    target_run_id: str,
    target_version_ts: datetime,
) -> int:
    engine = _db_engine(db_url)
    try:
        with engine.begin() as conn:
            inserted = conn.execute(
                text(
                    """
                    INSERT INTO job_details (
                        run_id,
                        version_ts,
                        company,
                        external_job_id,
                        job_description_path,
                        updated_at
                    )
                    SELECT
                        :target_run_id,
                        :target_version_ts,
                        source.company,
                        source.external_job_id,
                        source.job_description_path,
                        now()
                    FROM job_details source
                    JOIN jobs target_jobs
                      ON target_jobs.run_id = :target_run_id
                     AND target_jobs.company = source.company
                     AND target_jobs.external_job_id = source.external_job_id
                    LEFT JOIN job_details existing
                      ON existing.run_id = :target_run_id
                     AND existing.company = source.company
                     AND existing.external_job_id = source.external_job_id
                    WHERE source.run_id = :source_run_id
                      AND existing.external_job_id IS NULL
                    """
                ),
                {
                    "source_run_id": source_run_id,
                    "target_run_id": target_run_id,
                    "target_version_ts": target_version_ts,
                },
            )

            conn.execute(
                text(
                    """
                    UPDATE jobs target_jobs
                    SET posted_ts = COALESCE(target_jobs.posted_ts, source_jobs.posted_ts),
                        updated_at = now()
                    FROM jobs source_jobs
                    JOIN job_details source_details
                      ON source_details.run_id = source_jobs.run_id
                     AND source_details.company = source_jobs.company
                     AND source_details.external_job_id = source_jobs.external_job_id
                    WHERE target_jobs.run_id = :target_run_id
                      AND source_jobs.run_id = :source_run_id
                      AND target_jobs.company = source_jobs.company
                      AND target_jobs.external_job_id = source_jobs.external_job_id
                    """
                ),
                {
                    "source_run_id": source_run_id,
                    "target_run_id": target_run_id,
                },
            )
            return int(inserted.rowcount or 0)
    finally:
        engine.dispose()


def fetch_existing_job_detail_ids(
    db_url: str,
    *,
    run_id: str,
    companies: list[str],
) -> dict[str, set[str]]:
    detail_ids_by_company: dict[str, set[str]] = {company: set() for company in companies}
    if not companies:
        return detail_ids_by_company

    engine = _db_engine(db_url)
    try:
        with engine.begin() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT company, external_job_id
                    FROM job_details
                    WHERE run_id = :run_id
                    """
                ),
                {"run_id": run_id},
            ).mappings()
            for row in rows:
                company = str(row["company"])
                if company in detail_ids_by_company:
                    detail_ids_by_company[company].add(str(row["external_job_id"]))
    finally:
        engine.dispose()

    return detail_ids_by_company


def fetch_consistency_counts(
    db_url: str,
    *,
    run_id: str,
    companies: list[str],
) -> tuple[dict[str, int], dict[str, int], dict[str, int], dict[str, int]]:
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
                      AND (d.job_description_path IS NULL OR btrim(d.job_description_path) = '')
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

    return (
        jobs_count_by_company,
        missing_details_by_company,
        details_count_by_company,
        missing_description_by_company,
    )


def update_publish_run_status(
    db_url: str,
    *,
    run_id: str,
    status: str,
    db_ready: bool,
    db_error_message: str | None,
) -> None:
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


def publish_jobs_catalog_pointer(db_url: str, *, run_id: str) -> None:
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
