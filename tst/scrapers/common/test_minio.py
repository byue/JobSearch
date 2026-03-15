"""Unit tests for shared MinIO text storage helpers."""

from __future__ import annotations

import importlib
import os
import sys
import types
import unittest
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
        response = _FakeResponse(type(self).objects[(bucket_name, object_name)])
        type(self).last_response = response
        return response


class TestMinioStorage(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls._saved_modules = {name: sys.modules.get(name) for name in ("minio", "scrapers.common.minio")}
        minio_mod = types.ModuleType("minio")
        minio_mod.Minio = _FakeMinio
        sys.modules["minio"] = minio_mod
        sys.modules.pop("scrapers.common.minio", None)
        cls.mod = importlib.import_module("scrapers.common.minio")

    @classmethod
    def tearDownClass(cls) -> None:
        for name, value in cls._saved_modules.items():
            if value is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = value

    def setUp(self) -> None:
        _FakeMinio.reset()
        os.environ["MINIO_ENDPOINT"] = "minio:9000"
        os.environ["MINIO_ACCESS_KEY"] = "minioadmin"
        os.environ["MINIO_SECRET_KEY"] = "minioadmin"
        os.environ["MINIO_BUCKET"] = "jobsearch-xcom"
        os.environ["MINIO_SECURE"] = "false"

    def test_build_job_description_key(self) -> None:
        key = self.mod.build_job_description_key(run_id="r1", company="amazon", external_job_id="j1")
        self.assertEqual(key, "job-details/r1/amazon/j1.txt")

    def test_put_and_get_job_description(self) -> None:
        key = "job-details/r1/amazon/j1.txt"
        out = self.mod.put_job_description(key=key, body="hello")
        self.assertEqual(out, key)
        self.assertEqual(_FakeMinio.objects[("jobsearch-xcom", key)], b"hello")

        value = self.mod.get_job_description(key=key)
        self.assertEqual(value, "hello")
        self.assertIsNotNone(_FakeMinio.last_response)
        self.assertTrue(_FakeMinio.last_response.closed)
        self.assertTrue(_FakeMinio.last_response.released)


if __name__ == "__main__":
    unittest.main()
