"""MinIO-backed custom XCom backend.

Stores XCom payload bytes in MinIO and keeps only lightweight metadata in Airflow DB.
"""

from __future__ import annotations

import pickle
import re
from io import BytesIO
from typing import Any

from airflow.models.xcom import BaseXCom
from minio import Minio
from minio.error import S3Error

_MARKER = "jobsearch_minio_xcom_v1"


def _sanitize(value: str | None, fallback: str) -> str:
    if value is None:
        return fallback
    stripped = str(value).strip()
    if not stripped:
        return fallback
    return re.sub(r"[^a-zA-Z0-9._-]", "_", stripped)


def _as_bool(raw: str | None, default: bool = False) -> bool:
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


class MinioXComBackend(BaseXCom):
    """Persist XCom values in MinIO and only metadata in metadata DB."""

    @classmethod
    def _config(cls) -> dict[str, Any]:
        from airflow.configuration import conf

        endpoint = conf.get("core", "xcom_minio_endpoint", fallback=None) or "minio:9000"
        access_key = conf.get("core", "xcom_minio_access_key", fallback=None) or "minioadmin"
        secret_key = conf.get("core", "xcom_minio_secret_key", fallback=None) or "minioadmin"
        bucket = conf.get("core", "xcom_minio_bucket", fallback=None) or "jobsearch-xcom"
        prefix = conf.get("core", "xcom_minio_prefix", fallback=None) or "xcom"
        secure = _as_bool(conf.get("core", "xcom_minio_secure", fallback=None), default=False)
        return {
            "endpoint": endpoint,
            "access_key": access_key,
            "secret_key": secret_key,
            "bucket": bucket,
            "prefix": prefix.strip("/"),
            "secure": secure,
        }

    @classmethod
    def _client(cls) -> Minio:
        cfg = cls._config()
        return Minio(
            endpoint=cfg["endpoint"],
            access_key=cfg["access_key"],
            secret_key=cfg["secret_key"],
            secure=cfg["secure"],
        )

    @classmethod
    def _ensure_bucket(cls, client: Minio, bucket: str) -> None:
        try:
            if not client.bucket_exists(bucket):
                client.make_bucket(bucket)
        except S3Error:
            # Accept race where another worker created the bucket first.
            if not client.bucket_exists(bucket):
                raise

    @classmethod
    def _object_key(
        cls,
        *,
        dag_id: str | None,
        run_id: str | None,
        task_id: str | None,
        key: str | None,
        map_index: int | None,
    ) -> str:
        cfg = cls._config()
        parts = [
            cfg["prefix"] or "xcom",
            _sanitize(dag_id, "unknown_dag"),
            _sanitize(run_id, "unknown_run"),
            _sanitize(task_id, "unknown_task"),
            str(map_index if isinstance(map_index, int) else -1),
            f"{_sanitize(key, 'return_value')}.pkl",
        ]
        return "/".join(parts)

    @classmethod
    def serialize_value(
        cls,
        value: Any,
        *,
        key: str | None = None,
        task_id: str | None = None,
        dag_id: str | None = None,
        run_id: str | None = None,
        map_index: int | None = None,
        **kwargs: Any,
    ) -> Any:
        cfg = cls._config()
        client = cls._client()
        cls._ensure_bucket(client, cfg["bucket"])

        object_key = cls._object_key(
            dag_id=dag_id,
            run_id=run_id,
            task_id=task_id,
            key=key,
            map_index=map_index,
        )
        payload = pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL)
        client.put_object(
            bucket_name=cfg["bucket"],
            object_name=object_key,
            data=BytesIO(payload),
            length=len(payload),
            content_type="application/octet-stream",
        )

        marker = {
            "__type": _MARKER,
            "bucket": cfg["bucket"],
            "object_key": object_key,
            "size": len(payload),
        }
        return pickle.dumps(marker, protocol=pickle.HIGHEST_PROTOCOL)

    @classmethod
    def deserialize_value(cls, result: Any) -> Any:
        marker: Any = getattr(result, "value", result)
        if isinstance(result, (bytes, bytearray, memoryview)):
            try:
                marker = pickle.loads(bytes(result))
            except Exception:
                return result
        elif isinstance(marker, (bytes, bytearray, memoryview)):
            try:
                marker = pickle.loads(bytes(marker))
            except Exception:
                return marker

        if not isinstance(marker, dict) or marker.get("__type") != _MARKER:
            return marker
        bucket = marker.get("bucket")
        object_key = marker.get("object_key")
        if not isinstance(bucket, str) or not isinstance(object_key, str):
            return marker

        client = cls._client()
        response = client.get_object(bucket_name=bucket, object_name=object_key)
        try:
            payload = response.read()
        finally:
            response.close()
            response.release_conn()
        return pickle.loads(payload)

    def orm_deserialize_value(self) -> Any:
        marker: Any = getattr(self, "value", self)
        if isinstance(self, (bytes, bytearray, memoryview)):
            try:
                marker = pickle.loads(bytes(self))
            except Exception:
                return self
        elif isinstance(marker, (bytes, bytearray, memoryview)):
            try:
                marker = pickle.loads(bytes(marker))
            except Exception:
                return marker

        if isinstance(marker, dict) and marker.get("__type") == _MARKER:
            bucket = marker.get("bucket", "unknown")
            object_key = marker.get("object_key", "unknown")
            size = marker.get("size", "?")
            return f"<MinIOXCom bucket={bucket} object={object_key} size={size}>"
        return marker
