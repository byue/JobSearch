"""Database helpers for the local job scrapers DAG."""

from __future__ import annotations

import json
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
    prepared_rows: list[dict[str, Any]] = []
    for row in rows:
        prepared = dict(row)
        raw_skills = prepared.get("skills")
        skills = raw_skills if isinstance(raw_skills, list) else []
        prepared["skills"] = json.dumps([str(skill).strip() for skill in skills if str(skill).strip()])
        raw_locations = prepared.get("locations")
        locations = raw_locations if isinstance(raw_locations, list) else []
        prepared["locations"] = json.dumps(
            [
                {
                    "city": str(location.get("city") or "").strip() or None,
                    "region": str(location.get("region") or "").strip() or None,
                    "country": str(location.get("country") or "").strip() or None,
                }
                for location in locations
                if isinstance(location, dict)
            ]
        )
        prepared_rows.append(prepared)
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
                        job_type,
                        job_level,
                        details_url,
                        apply_url,
                        locations,
                        skills,
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
                        :job_type,
                        :job_level,
                        :details_url,
                        :apply_url,
                        CAST(:locations AS JSONB),
                        CAST(:skills AS JSONB),
                        :posted_ts,
                        FALSE,
                        now()
                    )
                    ON CONFLICT (run_id, company, external_job_id) DO UPDATE
                    SET version_ts = EXCLUDED.version_ts,
                        title = EXCLUDED.title,
                        job_type = EXCLUDED.job_type,
                        job_level = EXCLUDED.job_level,
                        details_url = EXCLUDED.details_url,
                        apply_url = EXCLUDED.apply_url,
                        locations = EXCLUDED.locations,
                        skills = CASE
                            WHEN EXCLUDED.skills = '[]'::jsonb THEN jobs.skills
                            ELSE EXCLUDED.skills
                        END,
                        posted_ts = EXCLUDED.posted_ts,
                        is_missing_details = FALSE,
                        updated_at = now()
                    """
                ),
                prepared_rows,
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


def fetch_job_skill_requests(
    db_url: str,
    *,
    run_id: str,
    companies: list[str],
) -> list[dict[str, str]]:
    requests_out: list[dict[str, str]] = []
    if not companies:
        return requests_out

    engine = _db_engine(db_url)
    try:
        with engine.begin() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT
                      j.company,
                      j.external_job_id,
                      d.job_description_path
                    FROM jobs j
                    JOIN job_details d
                      ON d.run_id = j.run_id
                     AND d.company = j.company
                     AND d.external_job_id = j.external_job_id
                    WHERE j.run_id = :run_id
                      AND j.is_missing_details = FALSE
                      AND d.job_description_path IS NOT NULL
                      AND btrim(d.job_description_path) <> ''
                      AND COALESCE(jsonb_array_length(j.skills), 0) = 0
                    ORDER BY j.company, j.external_job_id
                    """
                ),
                {"run_id": run_id},
            ).mappings()
            for row in rows:
                company = str(row["company"])
                if company not in companies:
                    continue
                requests_out.append(
                    {
                        "company": company,
                        "job_id": str(row["external_job_id"]),
                        "job_description_path": str(row["job_description_path"]),
                    }
                )
    finally:
        engine.dispose()

    return requests_out


def update_job_skills(
    db_url: str,
    *,
    run_id: str,
    company: str,
    external_job_id: str,
    skills: list[str],
    job_description_embedding: list[float],
) -> None:
    normalized_skills = [str(skill).strip() for skill in skills if str(skill).strip()]
    normalized_embedding = [float(value) for value in job_description_embedding]
    engine = _db_engine(db_url)
    try:
        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                    UPDATE jobs
                    SET skills = CAST(:skills AS JSONB),
                        job_description_embedding = CAST(:job_description_embedding AS JSONB),
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
                    "skills": json.dumps(normalized_skills),
                    "job_description_embedding": json.dumps(normalized_embedding),
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
) -> tuple[dict[str, int], dict[str, int], dict[str, int], dict[str, int], dict[str, int]]:
    jobs_count_by_company: dict[str, int] = {company: 0 for company in companies}
    missing_details_by_company: dict[str, int] = {company: 0 for company in companies}
    details_count_by_company: dict[str, int] = {company: 0 for company in companies}
    missing_description_by_company: dict[str, int] = {company: 0 for company in companies}
    missing_embedding_by_company: dict[str, int] = {company: 0 for company in companies}

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

            missing_embedding_rows = conn.execute(
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
                      AND d.job_description_path IS NOT NULL
                      AND btrim(d.job_description_path) <> ''
                      AND COALESCE(jsonb_array_length(j.job_description_embedding), 0) = 0
                    GROUP BY d.company
                    """
                ),
                {"run_id": run_id},
            ).mappings()
            for row in missing_embedding_rows:
                company = str(row["company"])
                if company in missing_embedding_by_company:
                    missing_embedding_by_company[company] = int(row["cnt"])
    finally:
        engine.dispose()

    return (
        jobs_count_by_company,
        missing_details_by_company,
        details_count_by_company,
        missing_description_by_company,
        missing_embedding_by_company,
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


def update_publish_run_es_status(
    db_url: str,
    *,
    run_id: str,
    es_ready: bool,
    es_error_message: str | None,
    status: str | None = None,
) -> None:
    engine = _db_engine(db_url)
    try:
        with engine.begin() as conn:
            if status is None:
                conn.execute(
                    text(
                        """
                        UPDATE publish_runs
                        SET es_ready = :es_ready,
                            es_error_message = :es_error_message,
                            updated_at = now()
                        WHERE run_id = :run_id
                        """
                    ),
                    {
                        "run_id": run_id,
                        "es_ready": es_ready,
                        "es_error_message": es_error_message,
                    },
                )
            else:
                conn.execute(
                    text(
                        """
                        UPDATE publish_runs
                        SET status = :status,
                            es_ready = :es_ready,
                            es_error_message = :es_error_message,
                            updated_at = now()
                        WHERE run_id = :run_id
                        """
                    ),
                    {
                        "run_id": run_id,
                        "status": status,
                        "es_ready": es_ready,
                        "es_error_message": es_error_message,
                    },
                )
    finally:
        engine.dispose()


def fetch_publish_run_readiness(
    db_url: str,
    *,
    run_id: str,
) -> dict[str, Any]:
    engine = _db_engine(db_url)
    try:
        with engine.begin() as conn:
            row = conn.execute(
                text(
                    """
                    SELECT status, db_ready, es_ready
                    FROM publish_runs
                    WHERE run_id = :run_id
                    LIMIT 1
                    """
                ),
                {"run_id": run_id},
            ).mappings()
            first_row = next(iter(row), None)
            if first_row is None:
                raise ValueError(f"Publish run '{run_id}' not found")
            return {
                "status": str(first_row.get("status") or ""),
                "db_ready": bool(first_row.get("db_ready")),
                "es_ready": bool(first_row.get("es_ready")),
            }
    finally:
        engine.dispose()


def fetch_search_index_requests(
    db_url: str,
    *,
    run_id: str,
) -> list[dict[str, Any]]:
    requests_out: list[dict[str, Any]] = []
    engine = _db_engine(db_url)
    try:
        with engine.begin() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT
                      j.run_id,
                      j.company,
                      j.external_job_id,
                      j.title,
                      j.job_type,
                      j.job_level,
                      j.details_url,
                      j.apply_url,
                      j.locations,
                      j.skills,
                      j.job_description_embedding,
                      j.posted_ts,
                      d.job_description_path
                    FROM jobs j
                    JOIN job_details d
                      ON d.run_id = j.run_id
                     AND d.company = j.company
                     AND d.external_job_id = j.external_job_id
                    WHERE j.run_id = :run_id
                      AND j.is_missing_details = FALSE
                      AND d.job_description_path IS NOT NULL
                      AND btrim(d.job_description_path) <> ''
                    ORDER BY j.company, j.external_job_id
                    """
                ),
                {"run_id": run_id},
            ).mappings()
            for row in rows:
                requests_out.append(dict(row))
    finally:
        engine.dispose()
    return requests_out


def mark_publish_run_succeeded(
    db_url: str,
    *,
    run_id: str,
) -> None:
    engine = _db_engine(db_url)
    try:
        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                    UPDATE publish_runs
                    SET status = 'succeeded',
                        updated_at = now()
                    WHERE run_id = :run_id
                    """
                ),
                {"run_id": run_id},
            )
    finally:
        engine.dispose()


def mark_publish_run_es_published(
    db_url: str,
    *,
    run_id: str,
) -> None:
    engine = _db_engine(db_url)
    try:
        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                    UPDATE publish_runs
                    SET es_published_at = now(),
                        updated_at = now()
                    WHERE run_id = :run_id
                    """
                ),
                {"run_id": run_id},
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
