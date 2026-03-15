from __future__ import annotations

import importlib
import sys
import types
import unittest
from datetime import datetime, timezone
from unittest.mock import patch

class _FakeMappingsResult:
    def __init__(self, rows: list[dict[str, object]]) -> None:
        self._rows = rows

    def mappings(self) -> list[dict[str, object]]:
        return self._rows


class _FakeExecuteResult:
    def __init__(self, rowcount: int = 0) -> None:
        self.rowcount = rowcount


class _FakeConnection:
    def __init__(self, returns: list[object] | None = None, raise_on_execute: bool = False) -> None:
        self.calls: list[tuple[object, object]] = []
        self._returns = list(returns or [])
        self._raise_on_execute = raise_on_execute

    def execute(self, query: object, params: object) -> object:
        self.calls.append((query, params))
        if self._raise_on_execute:
            raise RuntimeError("execute failed")
        if self._returns:
            return self._returns.pop(0)
        return None


class _FakeBeginCtx:
    def __init__(self, conn: _FakeConnection) -> None:
        self._conn = conn

    def __enter__(self) -> _FakeConnection:
        return self._conn

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False


class _FakeEngine:
    def __init__(self, conn: _FakeConnection) -> None:
        self.conn = conn
        self.disposed = False

    def begin(self) -> _FakeBeginCtx:
        return _FakeBeginCtx(self.conn)

    def dispose(self) -> None:
        self.disposed = True


class JobScrapersDbTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls._module_names = [
            "sqlalchemy",
            "sqlalchemy.engine",
            "sqlalchemy.pool",
            "scrapers.airflow.dags.job_scrapers_db",
        ]
        cls._saved_modules = {name: sys.modules.get(name) for name in cls._module_names}

        sqlalchemy_mod = types.ModuleType("sqlalchemy")
        sqlalchemy_engine_mod = types.ModuleType("sqlalchemy.engine")
        sqlalchemy_pool_mod = types.ModuleType("sqlalchemy.pool")

        class _Engine:
            pass

        class _NullPool:
            pass

        def _text(query: object) -> object:
            return query

        def _create_engine(*args, **kwargs) -> object:
            _ = (args, kwargs)
            return object()

        sqlalchemy_mod.text = _text
        sqlalchemy_mod.create_engine = _create_engine
        sqlalchemy_engine_mod.Engine = _Engine
        sqlalchemy_pool_mod.NullPool = _NullPool

        sys.modules["sqlalchemy"] = sqlalchemy_mod
        sys.modules["sqlalchemy.engine"] = sqlalchemy_engine_mod
        sys.modules["sqlalchemy.pool"] = sqlalchemy_pool_mod
        sys.modules.pop("scrapers.airflow.dags.job_scrapers_db", None)

        cls.mod = importlib.import_module("scrapers.airflow.dags.job_scrapers_db")

    @classmethod
    def tearDownClass(cls) -> None:
        for name in cls._module_names:
            if cls._saved_modules[name] is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = cls._saved_modules[name]

    def test_normalize_db_url(self) -> None:
        self.assertEqual(self.mod._normalize_db_url("  postgresql://x  "), "postgresql://x")
        with self.assertRaises(ValueError):
            self.mod._normalize_db_url("   ")

    def test_db_engine_uses_sqlalchemy_create_engine(self) -> None:
        with patch.object(self.mod, "create_engine", return_value="engine") as create_engine_mock:
            out = self.mod._db_engine("  postgresql://db ")
        self.assertEqual(out, "engine")
        kwargs = create_engine_mock.call_args.kwargs
        self.assertEqual(create_engine_mock.call_args.args[0], "postgresql://db")
        self.assertIs(kwargs["poolclass"], self.mod.NullPool)
        self.assertTrue(kwargs["future"])

    def test_upsert_publish_run_in_progress(self) -> None:
        conn = _FakeConnection()
        engine = _FakeEngine(conn)
        ts = datetime(2026, 1, 2, tzinfo=timezone.utc)
        with patch.object(self.mod, "_db_engine", return_value=engine):
            self.mod.upsert_publish_run_in_progress("db", run_id="r1", version_ts=ts)
        self.assertTrue(engine.disposed)
        self.assertEqual(len(conn.calls), 1)
        params = conn.calls[0][1]
        self.assertEqual(params["run_id"], "r1")
        self.assertEqual(params["version_ts"], ts)

    def test_upsert_companies_noop_when_empty(self) -> None:
        with patch.object(self.mod, "_db_engine") as engine_mock:
            self.mod.upsert_companies("db", rows=[])
        engine_mock.assert_not_called()

    def test_upsert_companies(self) -> None:
        conn = _FakeConnection()
        engine = _FakeEngine(conn)
        rows = [{"run_id": "r", "company": "google", "display_name": "Google", "version_ts": datetime.now(timezone.utc)}]
        with patch.object(self.mod, "_db_engine", return_value=engine):
            self.mod.upsert_companies("db", rows=rows)
        self.assertTrue(engine.disposed)
        self.assertEqual(conn.calls[0][1], rows)

    def test_upsert_jobs_noop_when_empty(self) -> None:
        with patch.object(self.mod, "_db_engine") as engine_mock:
            self.mod.upsert_jobs("db", rows=[])
        engine_mock.assert_not_called()

    def test_upsert_jobs(self) -> None:
        conn = _FakeConnection()
        engine = _FakeEngine(conn)
        rows = [{"run_id": "r", "version_ts": datetime.now(timezone.utc), "company": "google", "external_job_id": "1"}]
        with patch.object(self.mod, "_db_engine", return_value=engine):
            self.mod.upsert_jobs("db", rows=rows)
        self.assertTrue(engine.disposed)
        self.assertEqual(len(conn.calls[0][1]), 1)
        self.assertEqual(conn.calls[0][1][0]["external_job_id"], "1")
        self.assertEqual(conn.calls[0][1][0]["skills"], "[]")

    def test_mark_missing_details(self) -> None:
        conn = _FakeConnection()
        engine = _FakeEngine(conn)
        with patch.object(self.mod, "_db_engine", return_value=engine):
            self.mod.mark_missing_details("db", run_id="r1", company="google", external_job_id="j1")
        self.assertTrue(engine.disposed)
        self.assertEqual(conn.calls[0][1]["run_id"], "r1")
        self.assertEqual(conn.calls[0][1]["company"], "google")
        self.assertEqual(conn.calls[0][1]["external_job_id"], "j1")

    def test_upsert_job_details_without_posted_ts(self) -> None:
        conn = _FakeConnection()
        engine = _FakeEngine(conn)
        detail_row = {
            "run_id": "r1",
            "version_ts": datetime.now(timezone.utc),
            "company": "google",
            "external_job_id": "j1",
            "job_description_path": "job-details/r1/google/j1.txt",
        }
        with patch.object(self.mod, "_db_engine", return_value=engine):
            self.mod.upsert_job_details("db", detail_row=detail_row, posted_ts=None)
        self.assertTrue(engine.disposed)
        self.assertEqual(len(conn.calls), 1)
        self.assertEqual(conn.calls[0][1], detail_row)

    def test_upsert_job_details_with_posted_ts(self) -> None:
        conn = _FakeConnection()
        engine = _FakeEngine(conn)
        posted_ts = datetime(2026, 1, 2, tzinfo=timezone.utc)
        detail_row = {
            "run_id": "r1",
            "version_ts": datetime.now(timezone.utc),
            "company": "google",
            "external_job_id": "j1",
            "job_description_path": "job-details/r1/google/j1.txt",
        }
        with patch.object(self.mod, "_db_engine", return_value=engine):
            self.mod.upsert_job_details("db", detail_row=detail_row, posted_ts=posted_ts)
        self.assertTrue(engine.disposed)
        self.assertEqual(len(conn.calls), 2)
        second_params = conn.calls[1][1]
        self.assertEqual(second_params["run_id"], "r1")
        self.assertEqual(second_params["company"], "google")
        self.assertEqual(second_params["external_job_id"], "j1")
        self.assertEqual(second_params["posted_ts"], posted_ts)

    def test_fetch_consistency_counts(self) -> None:
        jobs_rows = _FakeMappingsResult(
            [
                {"company": "google", "cnt": 3, "missing_cnt": 1},
                {"company": "unknown", "cnt": 999, "missing_cnt": 999},
            ]
        )
        details_rows = _FakeMappingsResult(
            [
                {"company": "google", "cnt": 2},
                {"company": "unknown", "cnt": 999},
            ]
        )
        missing_desc_rows = _FakeMappingsResult(
            [
                {"company": "google", "cnt": 1},
                {"company": "unknown", "cnt": 999},
            ]
        )
        conn = _FakeConnection(returns=[jobs_rows, details_rows, missing_desc_rows])
        engine = _FakeEngine(conn)
        with patch.object(self.mod, "_db_engine", return_value=engine):
            jobs, missing, details, missing_desc = self.mod.fetch_consistency_counts(
                "db",
                run_id="r1",
                companies=["google", "meta"],
            )
        self.assertTrue(engine.disposed)
        self.assertEqual(len(conn.calls), 3)
        self.assertEqual(jobs, {"google": 3, "meta": 0})
        self.assertEqual(missing, {"google": 1, "meta": 0})
        self.assertEqual(details, {"google": 2, "meta": 0})
        self.assertEqual(missing_desc, {"google": 1, "meta": 0})

    def test_fetch_latest_published_run_id(self) -> None:
        conn = _FakeConnection(returns=[_FakeMappingsResult([{"run_id": "published-run"}])])
        engine = _FakeEngine(conn)
        with patch.object(self.mod, "_db_engine", return_value=engine):
            out = self.mod.fetch_latest_published_run_id("db", exclude_run_id="current-run")
        self.assertTrue(engine.disposed)
        self.assertEqual(out, "published-run")
        self.assertEqual(conn.calls[0][1], {"exclude_run_id": "current-run"})

    def test_fetch_latest_published_run_id_none_when_missing(self) -> None:
        conn = _FakeConnection(returns=[_FakeMappingsResult([])])
        engine = _FakeEngine(conn)
        with patch.object(self.mod, "_db_engine", return_value=engine):
            out = self.mod.fetch_latest_published_run_id("db")
        self.assertTrue(engine.disposed)
        self.assertIsNone(out)

    def test_fetch_latest_published_run_id_none_when_row_has_null_run_id(self) -> None:
        conn = _FakeConnection(returns=[_FakeMappingsResult([{"run_id": None}])])
        engine = _FakeEngine(conn)
        with patch.object(self.mod, "_db_engine", return_value=engine):
            out = self.mod.fetch_latest_published_run_id("db")
        self.assertTrue(engine.disposed)
        self.assertIsNone(out)

    def test_copy_job_details_from_run(self) -> None:
        conn = _FakeConnection(returns=[_FakeExecuteResult(rowcount=3), _FakeExecuteResult(rowcount=2)])
        engine = _FakeEngine(conn)
        version_ts = datetime(2026, 1, 2, tzinfo=timezone.utc)
        with patch.object(self.mod, "_db_engine", return_value=engine):
            out = self.mod.copy_job_details_from_run(
                "db",
                source_run_id="source-run",
                target_run_id="target-run",
                target_version_ts=version_ts,
            )
        self.assertTrue(engine.disposed)
        self.assertEqual(out, 3)
        self.assertEqual(len(conn.calls), 2)
        self.assertEqual(
            conn.calls[0][1],
            {
                "source_run_id": "source-run",
                "target_run_id": "target-run",
                "target_version_ts": version_ts,
            },
        )
        self.assertEqual(
            conn.calls[1][1],
            {
                "source_run_id": "source-run",
                "target_run_id": "target-run",
            },
        )

    def test_fetch_existing_job_detail_ids(self) -> None:
        conn = _FakeConnection(
            returns=[
                _FakeMappingsResult(
                    [
                        {"company": "google", "external_job_id": "g1"},
                        {"company": "google", "external_job_id": "g2"},
                        {"company": "unknown", "external_job_id": "u1"},
                    ]
                )
            ]
        )
        engine = _FakeEngine(conn)
        with patch.object(self.mod, "_db_engine", return_value=engine):
            out = self.mod.fetch_existing_job_detail_ids("db", run_id="r1", companies=["google", "meta"])
        self.assertTrue(engine.disposed)
        self.assertEqual(out, {"google": {"g1", "g2"}, "meta": set()})
        self.assertEqual(conn.calls[0][1], {"run_id": "r1"})

    def test_fetch_existing_job_detail_ids_empty_companies_short_circuits(self) -> None:
        with patch.object(self.mod, "_db_engine") as engine_mock:
            out = self.mod.fetch_existing_job_detail_ids("db", run_id="r1", companies=[])
        engine_mock.assert_not_called()
        self.assertEqual(out, {})

    def test_fetch_job_skill_requests(self) -> None:
        conn = _FakeConnection(
            returns=[
                _FakeMappingsResult(
                    [
                        {"company": "google", "external_job_id": "g1", "job_description_path": "job-details/r1/google/g1.txt"},
                        {"company": "unknown", "external_job_id": "u1", "job_description_path": "job-details/r1/unknown/u1.txt"},
                    ]
                )
            ]
        )
        engine = _FakeEngine(conn)
        with patch.object(self.mod, "_db_engine", return_value=engine):
            out = self.mod.fetch_job_skill_requests("db", run_id="r1", companies=["google", "meta"])
        self.assertTrue(engine.disposed)
        self.assertEqual(out, [{"company": "google", "job_id": "g1", "job_description_path": "job-details/r1/google/g1.txt"}])

    def test_fetch_job_skill_requests_empty_companies(self) -> None:
        with patch.object(self.mod, "_db_engine") as engine_mock:
            out = self.mod.fetch_job_skill_requests("db", run_id="r1", companies=[])
        engine_mock.assert_not_called()
        self.assertEqual(out, [])

    def test_update_job_skills(self) -> None:
        conn = _FakeConnection()
        engine = _FakeEngine(conn)
        with patch.object(self.mod, "_db_engine", return_value=engine):
            self.mod.update_job_skills(
                "db",
                run_id="r1",
                company="google",
                external_job_id="j1",
                skills=["Python", " SQL ", ""],
            )
        self.assertTrue(engine.disposed)
        self.assertEqual(conn.calls[0][1]["run_id"], "r1")
        self.assertEqual(conn.calls[0][1]["company"], "google")
        self.assertEqual(conn.calls[0][1]["external_job_id"], "j1")
        self.assertEqual(conn.calls[0][1]["skills"], '["Python", "SQL"]')

    def test_update_publish_run_status(self) -> None:
        conn = _FakeConnection()
        engine = _FakeEngine(conn)
        with patch.object(self.mod, "_db_engine", return_value=engine):
            self.mod.update_publish_run_status(
                "db",
                run_id="r1",
                status="failed",
                db_ready=False,
                db_error_message="boom",
            )
        self.assertTrue(engine.disposed)
        self.assertEqual(conn.calls[0][1]["status"], "failed")
        self.assertFalse(conn.calls[0][1]["db_ready"])
        self.assertEqual(conn.calls[0][1]["db_error_message"], "boom")

    def test_publish_jobs_catalog_pointer(self) -> None:
        conn = _FakeConnection()
        engine = _FakeEngine(conn)
        with patch.object(self.mod, "_db_engine", return_value=engine):
            self.mod.publish_jobs_catalog_pointer("db", run_id="r1")
        self.assertTrue(engine.disposed)
        self.assertEqual(len(conn.calls), 2)
        self.assertEqual(conn.calls[0][1], {"run_id": "r1"})
        self.assertEqual(conn.calls[1][1], {"run_id": "r1"})

    def test_dispose_happens_on_execute_error(self) -> None:
        conn = _FakeConnection(raise_on_execute=True)
        engine = _FakeEngine(conn)
        with patch.object(self.mod, "_db_engine", return_value=engine):
            with self.assertRaises(RuntimeError):
                self.mod.mark_missing_details("db", run_id="r", company="c", external_job_id="j")
        self.assertTrue(engine.disposed)


if __name__ == "__main__":
    unittest.main()
