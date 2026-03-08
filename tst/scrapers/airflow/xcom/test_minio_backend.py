"""Unit tests for MinIO-backed custom XCom backend."""

from __future__ import annotations

import importlib
import json
import os
import sys
import types
import unittest
import uuid as py_uuid
from typing import Any


class _FakeResponse:
    def __init__(self, payload: bytes) -> None:
        self._payload = payload
        self.closed = False
        self.released = False

    def read(self) -> bytes:
        return self._payload

    def close(self) -> None:
        self.closed = True

    def release_conn(self) -> None:
        self.released = True


class _FakeMinio:
    init_calls: list[dict[str, Any]] = []
    objects: dict[tuple[str, str], bytes] = {}
    last_response: _FakeResponse | None = None

    @classmethod
    def reset(cls) -> None:
        cls.init_calls = []
        cls.objects = {}
        cls.last_response = None

    def __init__(self, endpoint: str, access_key: str, secret_key: str, secure: bool) -> None:
        type(self).init_calls.append(
            {
                "endpoint": endpoint,
                "access_key": access_key,
                "secret_key": secret_key,
                "secure": secure,
            }
        )

    def put_object(
        self,
        bucket_name: str,
        object_name: str,
        data: Any,
        length: int,
        content_type: str,
    ) -> None:
        _ = content_type
        type(self).objects[(bucket_name, object_name)] = data.read(length)

    def get_object(self, bucket_name: str, object_name: str) -> _FakeResponse:
        payload = type(self).objects[(bucket_name, object_name)]
        response = _FakeResponse(payload)
        type(self).last_response = response
        return response


class TestMinioBackend(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls._module_names = [
            "airflow",
            "airflow.models",
            "airflow.models.xcom",
            "minio",
            "scrapers.airflow.xcom.minio_backend",
        ]
        cls._saved_modules = {name: sys.modules.get(name) for name in cls._module_names}

        airflow_mod = types.ModuleType("airflow")
        airflow_models_mod = types.ModuleType("airflow.models")
        airflow_models_xcom_mod = types.ModuleType("airflow.models.xcom")

        class _BaseXCom:
            @staticmethod
            def serialize_value(value: Any) -> Any:
                return value

            @staticmethod
            def deserialize_value(result: Any) -> Any:
                return result

        airflow_models_xcom_mod.BaseXCom = _BaseXCom

        minio_mod = types.ModuleType("minio")
        minio_mod.Minio = _FakeMinio

        sys.modules["airflow"] = airflow_mod
        sys.modules["airflow.models"] = airflow_models_mod
        sys.modules["airflow.models.xcom"] = airflow_models_xcom_mod
        sys.modules["minio"] = minio_mod
        sys.modules.pop("scrapers.airflow.xcom.minio_backend", None)

        cls.mod = importlib.import_module("scrapers.airflow.xcom.minio_backend")

    @classmethod
    def tearDownClass(cls) -> None:
        for name in cls._module_names:
            if cls._saved_modules[name] is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = cls._saved_modules[name]

    def setUp(self) -> None:
        _FakeMinio.reset()
        os.environ["MINIO_ENDPOINT"] = "minio:9000"
        os.environ["MINIO_ACCESS_KEY"] = "minioadmin"
        os.environ["MINIO_SECRET_KEY"] = "minioadmin"
        os.environ["MINIO_BUCKET"] = "jobsearch-xcom"
        os.environ["MINIO_SECURE"] = "false"

    def test_config_reads_required_env(self) -> None:
        cfg = self.mod.MinioXComBackend._config()
        self.assertEqual(cfg["endpoint"], "minio:9000")
        self.assertEqual(cfg["access_key"], "minioadmin")
        self.assertEqual(cfg["secret_key"], "minioadmin")
        self.assertEqual(cfg["bucket"], "jobsearch-xcom")
        self.assertFalse(cfg["secure"])

    def test_config_raises_when_missing_required_env(self) -> None:
        del os.environ["MINIO_ENDPOINT"]
        with self.assertRaises(RuntimeError):
            _ = self.mod.MinioXComBackend._config()

    def test_client_uses_config(self) -> None:
        os.environ["MINIO_ENDPOINT"] = "endpoint-a"
        os.environ["MINIO_ACCESS_KEY"] = "access-a"
        os.environ["MINIO_SECRET_KEY"] = "secret-a"
        os.environ["MINIO_SECURE"] = "true"
        _ = self.mod.MinioXComBackend._client()

        self.assertEqual(len(_FakeMinio.init_calls), 1)
        self.assertEqual(_FakeMinio.init_calls[0]["endpoint"], "endpoint-a")
        self.assertEqual(_FakeMinio.init_calls[0]["access_key"], "access-a")
        self.assertEqual(_FakeMinio.init_calls[0]["secret_key"], "secret-a")
        self.assertTrue(_FakeMinio.init_calls[0]["secure"])

    def test_serialize_builds_expected_key_and_payload(self) -> None:
        fixed_uuid = py_uuid.UUID("12345678-1234-5678-1234-567812345678")
        original_uuid4 = self.mod.uuid.uuid4
        self.mod.uuid.uuid4 = lambda: fixed_uuid
        try:
            serialized_key = self.mod.MinioXComBackend.serialize_value(
                {"x": 1},
                key="result",
                task_id="task-1",
                dag_id="dag-1",
                run_id="run-1",
                map_index=7,
            )
        finally:
            self.mod.uuid.uuid4 = original_uuid4

        expected_key = "dag-1/run-1/task-1/7/result/12345678-1234-5678-1234-567812345678.json"
        self.assertEqual(serialized_key, expected_key)
        payload = _FakeMinio.objects[("jobsearch-xcom", expected_key)]
        self.assertEqual(json.loads(payload.decode("utf-8")), {"x": 1})

    def test_deserialize_fetches_json_and_releases_response(self) -> None:
        key = "dag-1/run-1/task-1/0/result/file.json"
        _FakeMinio.objects[("jobsearch-xcom", key)] = b'{"ok":true}'

        data = self.mod.MinioXComBackend.deserialize_value(key)
        self.assertEqual(data, {"ok": True})
        self.assertIsNotNone(_FakeMinio.last_response)
        self.assertTrue(_FakeMinio.last_response.closed)
        self.assertTrue(_FakeMinio.last_response.released)

    def test_serialize_is_keyword_only(self) -> None:
        with self.assertRaises(TypeError):
            self.mod.MinioXComBackend.serialize_value({"x": 1}, "k")  # type: ignore[misc]


if __name__ == "__main__":
    unittest.main()
