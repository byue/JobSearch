"""Shared MinIO helpers for storing and loading job detail text."""

from __future__ import annotations

from io import BytesIO

from minio import Minio

from scrapers.common.env import env_bool, require_env


def _config() -> dict[str, str | bool]:
    endpoint = require_env("MINIO_ENDPOINT")
    access_key = require_env("MINIO_ACCESS_KEY")
    secret_key = require_env("MINIO_SECRET_KEY")
    bucket = require_env("MINIO_BUCKET")
    require_env("MINIO_SECURE")
    secure = env_bool("MINIO_SECURE", default=False)
    return {
        "endpoint": endpoint,
        "access_key": access_key,
        "secret_key": secret_key,
        "bucket": bucket,
        "secure": secure,
    }


def _client() -> Minio:
    cfg = _config()
    return Minio(
        str(cfg["endpoint"]),
        str(cfg["access_key"]),
        str(cfg["secret_key"]),
        secure=bool(cfg["secure"]),
    )


def build_job_description_key(*, run_id: str, company: str, external_job_id: str) -> str:
    return f"job-details/{run_id}/{company}/{external_job_id}.txt"


def put_job_description(*, key: str, body: str) -> str:
    cfg = _config()
    client = _client()
    payload = body.encode("utf-8")
    client.put_object(
        str(cfg["bucket"]),
        key,
        BytesIO(payload),
        length=len(payload),
        content_type="text/plain; charset=utf-8",
    )
    return key


def get_job_description(*, key: str) -> str:
    cfg = _config()
    client = _client()
    response = client.get_object(str(cfg["bucket"]), key)
    try:
        return response.read().decode("utf-8")
    finally:
        response.close()
        response.release_conn()
