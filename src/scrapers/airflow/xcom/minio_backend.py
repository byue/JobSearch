import json
import uuid
from io import BytesIO
from typing import Any

from airflow.models.xcom import BaseXCom
from minio import Minio
from scrapers.common.env import env_bool, require_env


class MinioXComBackend(BaseXCom):
    @classmethod
    def _config(cls) -> dict[str, Any]:
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

    @classmethod
    def _client(cls) -> Minio:
        cfg = cls._config()
        return Minio(
            cfg["endpoint"],
            cfg["access_key"],
            cfg["secret_key"],
            secure=cfg["secure"],
        )

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
    ) -> Any:
        cfg = cls._config()
        client = cls._client()
        filename = f"{uuid.uuid4()}.json"
        minio_key = f"{dag_id}/{run_id}/{task_id}/{map_index}/{key}/{filename}"
        payload = json.dumps(value).encode("utf-8")
        client.put_object(
            cfg["bucket"],
            minio_key,
            BytesIO(payload),
            length=len(payload),
            content_type="application/json",
        )
        return BaseXCom.serialize_value(value=minio_key)

    @classmethod
    def deserialize_value(cls, result: Any) -> Any:
        key = BaseXCom.deserialize_value(result=result)
        cfg = cls._config()
        client = cls._client()
        response = client.get_object(cfg["bucket"], key)
        try:
            return json.loads(response.read().decode("utf-8"))
        finally:
            response.close()
            response.release_conn()
