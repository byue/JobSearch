"""Unit tests for MinIO-backed custom XCom backend."""

from __future__ import annotations

import importlib
import pickle
import sys
import types
import unittest
from typing import Any


class _FakeConf:
    def __init__(self) -> None:
        self.values: dict[tuple[str, str], str | None] = {}

    def get(self, section: str, key: str, fallback: str | None = None) -> str | None:
        return self.values.get((section, key), fallback)


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


class _FakeStat:
    def __init__(self) -> None:
        self.etag = "fake-etag"
        self.version_id = "v1"


class _FakeMinio:
    init_calls: list[dict[str, Any]] = []
    buckets: set[str] = set()
    objects: dict[tuple[str, str], bytes] = {}
    last_response: _FakeResponse | None = None

    @classmethod
    def reset(cls) -> None:
        cls.init_calls = []
        cls.buckets = set()
        cls.objects = {}
        cls.last_response = None

    def __init__(self, *, endpoint: str, access_key: str, secret_key: str, secure: bool) -> None:
        self.init_calls.append(
            {
                "endpoint": endpoint,
                "access_key": access_key,
                "secret_key": secret_key,
                "secure": secure,
            }
        )

    def bucket_exists(self, bucket: str) -> bool:
        return bucket in self.buckets

    def make_bucket(self, bucket: str) -> None:
        self.buckets.add(bucket)

    def put_object(
        self,
        *,
        bucket_name: str,
        object_name: str,
        data: Any,
        length: int,
        content_type: str,
    ) -> None:
        _ = content_type
        self.buckets.add(bucket_name)
        self.objects[(bucket_name, object_name)] = data.read(length)

    def stat_object(self, bucket_name: str, object_name: str) -> _FakeStat:
        if (bucket_name, object_name) not in self.objects:
            raise KeyError("object not found")
        return _FakeStat()

    def get_object(self, *, bucket_name: str, object_name: str, version_id: str | None = None) -> _FakeResponse:
        _ = version_id
        payload = self.objects[(bucket_name, object_name)]
        response = _FakeResponse(payload)
        type(self).last_response = response
        return response


class _FakeXComRow:
    def __init__(self, value: Any) -> None:
        self.value = value


class TestMinioBackend(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls._module_names = [
            "airflow",
            "airflow.configuration",
            "airflow.models",
            "airflow.models.xcom",
            "minio",
            "minio.error",
            "scrapers.airflow.xcom.minio_backend",
        ]
        cls._saved_modules = {name: sys.modules.get(name) for name in cls._module_names}

        cls.fake_conf = _FakeConf()

        airflow_mod = types.ModuleType("airflow")
        airflow_configuration_mod = types.ModuleType("airflow.configuration")
        airflow_configuration_mod.conf = cls.fake_conf
        airflow_models_mod = types.ModuleType("airflow.models")
        airflow_models_xcom_mod = types.ModuleType("airflow.models.xcom")

        class _BaseXCom:
            pass

        airflow_models_xcom_mod.BaseXCom = _BaseXCom

        minio_mod = types.ModuleType("minio")
        minio_error_mod = types.ModuleType("minio.error")

        class _S3Error(Exception):
            pass

        cls.S3Error = _S3Error
        minio_mod.Minio = _FakeMinio
        minio_error_mod.S3Error = _S3Error

        sys.modules["airflow"] = airflow_mod
        sys.modules["airflow.configuration"] = airflow_configuration_mod
        sys.modules["airflow.models"] = airflow_models_mod
        sys.modules["airflow.models.xcom"] = airflow_models_xcom_mod
        sys.modules["minio"] = minio_mod
        sys.modules["minio.error"] = minio_error_mod
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
        self.fake_conf.values = {}
        _FakeMinio.reset()

    def test_sanitize(self) -> None:
        self.assertEqual(self.mod._sanitize(None, "fallback"), "fallback")
        self.assertEqual(self.mod._sanitize("   ", "fallback"), "fallback")
        self.assertEqual(self.mod._sanitize("abc-_.123", "fallback"), "abc-_.123")
        self.assertEqual(self.mod._sanitize("a b:c", "fallback"), "a_b_c")

    def test_as_bool(self) -> None:
        self.assertTrue(self.mod._as_bool(None, default=True))
        self.assertFalse(self.mod._as_bool(None, default=False))
        self.assertTrue(self.mod._as_bool("TRUE"))
        self.assertTrue(self.mod._as_bool(" on "))
        self.assertFalse(self.mod._as_bool("no"))

    def test_config_defaults_and_custom(self) -> None:
        cfg = self.mod.MinioXComBackend._config()
        self.assertEqual(cfg["endpoint"], "minio:9000")
        self.assertEqual(cfg["access_key"], "minioadmin")
        self.assertEqual(cfg["secret_key"], "minioadmin")
        self.assertEqual(cfg["bucket"], "jobsearch-xcom")
        self.assertEqual(cfg["prefix"], "xcom")
        self.assertFalse(cfg["secure"])

        self.fake_conf.values = {
            ("core", "xcom_minio_endpoint"): "custom-minio:9000",
            ("core", "xcom_minio_access_key"): "ak",
            ("core", "xcom_minio_secret_key"): "sk",
            ("core", "xcom_minio_bucket"): "custom-bucket",
            ("core", "xcom_minio_prefix"): "/a/b/",
            ("core", "xcom_minio_secure"): "yes",
        }
        cfg_custom = self.mod.MinioXComBackend._config()
        self.assertEqual(cfg_custom["endpoint"], "custom-minio:9000")
        self.assertEqual(cfg_custom["access_key"], "ak")
        self.assertEqual(cfg_custom["secret_key"], "sk")
        self.assertEqual(cfg_custom["bucket"], "custom-bucket")
        self.assertEqual(cfg_custom["prefix"], "a/b")
        self.assertTrue(cfg_custom["secure"])

    def test_client_uses_config(self) -> None:
        self.fake_conf.values = {
            ("core", "xcom_minio_endpoint"): "endpoint-a",
            ("core", "xcom_minio_access_key"): "access-a",
            ("core", "xcom_minio_secret_key"): "secret-a",
            ("core", "xcom_minio_secure"): "true",
        }
        _ = self.mod.MinioXComBackend._client()
        self.assertEqual(len(_FakeMinio.init_calls), 1)
        self.assertEqual(_FakeMinio.init_calls[0]["endpoint"], "endpoint-a")
        self.assertEqual(_FakeMinio.init_calls[0]["access_key"], "access-a")
        self.assertEqual(_FakeMinio.init_calls[0]["secret_key"], "secret-a")
        self.assertTrue(_FakeMinio.init_calls[0]["secure"])

    def test_ensure_bucket_success_and_race(self) -> None:
        client = _FakeMinio(endpoint="e", access_key="a", secret_key="s", secure=False)
        self.mod.MinioXComBackend._ensure_bucket(client, "bucket-a")
        self.assertIn("bucket-a", _FakeMinio.buckets)

        class RaceClient:
            def __init__(self, exc_type: type[Exception]) -> None:
                self.calls = 0
                self.exc_type = exc_type

            def bucket_exists(self, bucket: str) -> bool:
                _ = bucket
                self.calls += 1
                if self.calls == 1:
                    return False
                return True

            def make_bucket(self, bucket: str) -> None:
                _ = bucket
                raise self.exc_type("race")

        race_client = RaceClient(self.S3Error)
        self.mod.MinioXComBackend._ensure_bucket(race_client, "bucket-race")

    def test_ensure_bucket_raises_when_still_missing(self) -> None:
        s3_error = self.S3Error

        class BadClient:
            def bucket_exists(self, bucket: str) -> bool:
                _ = bucket
                return False

            def make_bucket(self, bucket: str) -> None:
                _ = bucket
                raise s3_error("boom")

        with self.assertRaises(self.S3Error):
            self.mod.MinioXComBackend._ensure_bucket(BadClient(), "bucket-bad")

    def test_object_key_formats_and_fallbacks(self) -> None:
        self.fake_conf.values = {("core", "xcom_minio_prefix"): ""}
        key = self.mod.MinioXComBackend._object_key(
            dag_id="dag/id",
            run_id="run:id",
            task_id="task id",
            key="return value",
            map_index=None,
        )
        self.assertEqual(key, "xcom/dag_id/run_id/task_id/-1/return_value.pkl")

    def test_serialize_deserialize_and_orm_deserialize(self) -> None:
        value = {"a": [1, 2], "b": {"nested": True}}
        serialized = self.mod.MinioXComBackend.serialize_value(
            value,
            key="result",
            task_id="task-1",
            dag_id="dag-1",
            run_id="run-1",
            map_index=7,
        )
        self.assertIsInstance(serialized, (bytes, bytearray))
        metadata = pickle.loads(serialized)
        self.assertEqual(metadata["__type"], "jobsearch_minio_xcom_v1")
        self.assertEqual(metadata["bucket"], "jobsearch-xcom")
        self.assertTrue(metadata["object_key"].startswith("xcom/dag-1/run-1/task-1/7/result.pkl"))
        self.assertGreater(metadata["size"], 0)

        roundtrip = self.mod.MinioXComBackend.deserialize_value(serialized)
        self.assertEqual(roundtrip, value)
        roundtrip_from_row = self.mod.MinioXComBackend.deserialize_value(_FakeXComRow(serialized))
        self.assertEqual(roundtrip_from_row, value)
        self.assertIsNotNone(_FakeMinio.last_response)
        self.assertTrue(_FakeMinio.last_response.closed)
        self.assertTrue(_FakeMinio.last_response.released)

        orm_value = self.mod.MinioXComBackend.orm_deserialize_value(serialized)
        self.assertIn("MinIOXCom", orm_value)
        self.assertIn("jobsearch-xcom", orm_value)
        orm_value_from_row = self.mod.MinioXComBackend.orm_deserialize_value(_FakeXComRow(serialized))
        self.assertIn("MinIOXCom", orm_value_from_row)

    def test_deserialize_passthrough_cases(self) -> None:
        self.assertEqual(self.mod.MinioXComBackend.deserialize_value("abc"), "abc")
        self.assertEqual(self.mod.MinioXComBackend.deserialize_value({"__type": "other"}), {"__type": "other"})
        bad_meta = {"__type": "jobsearch_minio_xcom_v1", "bucket": 123, "object_key": "k"}
        self.assertEqual(self.mod.MinioXComBackend.deserialize_value(bad_meta), bad_meta)
        self.assertEqual(self.mod.MinioXComBackend.deserialize_value(b"not-pickle"), b"not-pickle")
        self.assertEqual(
            self.mod.MinioXComBackend.deserialize_value(_FakeXComRow(b"not-pickle-via-value")),
            b"not-pickle-via-value",
        )

        self.assertEqual(self.mod.MinioXComBackend.orm_deserialize_value(123), 123)
        marker_missing = pickle.dumps({"__type": "jobsearch_minio_xcom_v1"})
        preview = self.mod.MinioXComBackend.orm_deserialize_value(marker_missing)
        self.assertIn("bucket=unknown", preview)
        self.assertIn("object=unknown", preview)
        self.assertIn("size=?", preview)
        self.assertEqual(self.mod.MinioXComBackend.orm_deserialize_value(b"not-pickle"), b"not-pickle")
        self.assertEqual(
            self.mod.MinioXComBackend.orm_deserialize_value(_FakeXComRow(b"not-pickle-via-value")),
            b"not-pickle-via-value",
        )

    def test_serialize_ignores_extra_kwargs(self) -> None:
        serialized = self.mod.MinioXComBackend.serialize_value(
            {"x": 1},
            key="k",
            task_id="t",
            dag_id="d",
            run_id="r",
            map_index=0,
            unknown_kwarg="ignored",
        )
        meta = pickle.loads(serialized)
        self.assertEqual(meta["__type"], "jobsearch_minio_xcom_v1")
        stored = _FakeMinio.objects[(meta["bucket"], meta["object_key"])]
        self.assertEqual(pickle.loads(stored), {"x": 1})


if __name__ == "__main__":
    unittest.main()
