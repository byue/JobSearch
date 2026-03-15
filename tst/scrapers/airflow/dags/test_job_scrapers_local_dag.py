from __future__ import annotations

import importlib
import os
import sys
import types
import unittest
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import patch

_TASK_REGISTRY: dict[str, "_FakeTask"] = {}
_SENSOR_CALLABLES: dict[str, object] = {}


class _FakeNode:
    def __rshift__(self, other: object) -> object:
        return other

    def __rrshift__(self, other: object) -> object:
        return self


class _FakeTask(_FakeNode):
    def __init__(self, fn):
        self.fn = fn

    def __call__(self, *args, **kwargs):
        _ = (args, kwargs)
        return _FakeNode()

    def expand(self, **kwargs):
        _ = kwargs
        return _FakeNode()

    def expand_kwargs(self, kwargs):
        _ = kwargs
        return _FakeNode()

    def partial(self, **kwargs):
        _ = kwargs
        return self


def _fake_task_decorator(*dargs, **dkwargs):
    task_id = dkwargs.get("task_id")

    def _decorate(fn):
        resolved_task_id = str(task_id or fn.__name__)
        wrapped = _FakeTask(fn)
        _TASK_REGISTRY[resolved_task_id] = wrapped
        return wrapped

    if dargs and callable(dargs[0]) and len(dargs) == 1:
        return _decorate(dargs[0])
    return _decorate


def _fake_dag_decorator(*dargs, **dkwargs):
    _ = (dargs, dkwargs)

    def _decorate(fn):
        def _wrapped(*args, **kwargs):
            return fn(*args, **kwargs)

        return _wrapped

    return _decorate


class _FakeAirflowFailException(Exception):
    pass


def _fake_get_current_context():
    return {}


class _FakePythonSensor(_FakeNode):
    def __init__(self, *args, **kwargs):
        _ = args
        task_id = str(kwargs.get("task_id", ""))
        python_callable = kwargs.get("python_callable")
        if task_id and python_callable is not None:
            _SENSOR_CALLABLES[task_id] = python_callable


class JobScrapersLocalDagTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls._module_names = [
            "airflow",
            "airflow.decorators",
            "airflow.sdk",
            "airflow.exceptions",
            "airflow.operators.python",
            "airflow.sensors.python",
            "airflow.providers",
            "airflow.providers.standard",
            "airflow.providers.standard.sensors",
            "airflow.providers.standard.sensors.python",
            "sqlalchemy",
            "sqlalchemy.engine",
            "sqlalchemy.pool",
            "scrapers.airflow.clients",
            "scrapers.airflow.clients.client_factory",
            "scrapers.airflow.clients.common",
            "common",
            "common.request_policy",
            "scrapers.proxy",
            "scrapers.proxy.proxy_management_client",
            "scrapers.airflow.dags.job_scrapers_local_dag",
        ]
        cls._saved_modules = {name: sys.modules.get(name) for name in cls._module_names}

        airflow_mod = types.ModuleType("airflow")
        airflow_decorators_mod = types.ModuleType("airflow.decorators")
        airflow_sdk_mod = types.ModuleType("airflow.sdk")
        airflow_exceptions_mod = types.ModuleType("airflow.exceptions")
        airflow_operators_python_mod = types.ModuleType("airflow.operators.python")
        airflow_sensors_python_mod = types.ModuleType("airflow.sensors.python")
        airflow_providers_mod = types.ModuleType("airflow.providers")
        airflow_providers_standard_mod = types.ModuleType("airflow.providers.standard")
        airflow_providers_standard_sensors_mod = types.ModuleType("airflow.providers.standard.sensors")
        airflow_providers_standard_sensors_python_mod = types.ModuleType("airflow.providers.standard.sensors.python")
        sqlalchemy_mod = types.ModuleType("sqlalchemy")
        sqlalchemy_engine_mod = types.ModuleType("sqlalchemy.engine")
        sqlalchemy_pool_mod = types.ModuleType("sqlalchemy.pool")
        clients_mod = types.ModuleType("scrapers.airflow.clients")
        clients_mod.__path__ = []  # package marker
        clients_client_factory_mod = types.ModuleType("scrapers.airflow.clients.client_factory")
        clients_common_mod = types.ModuleType("scrapers.airflow.clients.common")
        clients_common_mod.__path__ = []  # package marker
        common_mod = types.ModuleType("common")
        common_mod.__path__ = []  # package marker
        clients_request_policy_mod = types.ModuleType("common.request_policy")
        proxy_mod = types.ModuleType("scrapers.proxy")
        proxy_mod.__path__ = []  # package marker
        proxy_management_client_mod = types.ModuleType("scrapers.proxy.proxy_management_client")

        airflow_decorators_mod.dag = _fake_dag_decorator
        airflow_decorators_mod.task = _fake_task_decorator
        airflow_sdk_mod.dag = _fake_dag_decorator
        airflow_sdk_mod.task = _fake_task_decorator
        airflow_sdk_mod.get_current_context = _fake_get_current_context
        airflow_exceptions_mod.AirflowFailException = _FakeAirflowFailException
        airflow_operators_python_mod.get_current_context = _fake_get_current_context
        airflow_sensors_python_mod.PythonSensor = _FakePythonSensor
        airflow_providers_standard_sensors_python_mod.PythonSensor = _FakePythonSensor
        sqlalchemy_mod.text = lambda query: query
        sqlalchemy_mod.create_engine = lambda *args, **kwargs: object()
        sqlalchemy_engine_mod.Engine = object
        sqlalchemy_pool_mod.NullPool = object
        clients_client_factory_mod.build_client = lambda **kwargs: object()

        class _RequestPolicy:
            def __init__(
                self,
                *,
                timeout_seconds: float,
                connect_timeout_seconds: float | None = None,
                max_retries: int = 1,
                backoff_factor: float = 0.0,
                max_backoff_seconds: float = 0.0,
                jitter: bool = False,
            ) -> None:
                _ = (
                    timeout_seconds,
                    connect_timeout_seconds,
                    max_retries,
                    backoff_factor,
                    max_backoff_seconds,
                    jitter,
                )

        clients_request_policy_mod.RequestPolicy = _RequestPolicy

        class _ProxyManagementClient:
            def __init__(self, *args, **kwargs) -> None:
                _ = (args, kwargs)

        proxy_management_client_mod.ProxyManagementClient = _ProxyManagementClient

        sys.modules["airflow"] = airflow_mod
        sys.modules["airflow.decorators"] = airflow_decorators_mod
        sys.modules["airflow.sdk"] = airflow_sdk_mod
        sys.modules["airflow.exceptions"] = airflow_exceptions_mod
        sys.modules["airflow.operators.python"] = airflow_operators_python_mod
        sys.modules["airflow.sensors.python"] = airflow_sensors_python_mod
        sys.modules["airflow.providers"] = airflow_providers_mod
        sys.modules["airflow.providers.standard"] = airflow_providers_standard_mod
        sys.modules["airflow.providers.standard.sensors"] = airflow_providers_standard_sensors_mod
        sys.modules["airflow.providers.standard.sensors.python"] = airflow_providers_standard_sensors_python_mod
        sys.modules["sqlalchemy"] = sqlalchemy_mod
        sys.modules["sqlalchemy.engine"] = sqlalchemy_engine_mod
        sys.modules["sqlalchemy.pool"] = sqlalchemy_pool_mod
        sys.modules["scrapers.airflow.clients"] = clients_mod
        sys.modules["scrapers.airflow.clients.client_factory"] = clients_client_factory_mod
        sys.modules["scrapers.airflow.clients.common"] = clients_common_mod
        sys.modules["common"] = common_mod
        sys.modules["common.request_policy"] = clients_request_policy_mod
        sys.modules["scrapers.proxy"] = proxy_mod
        sys.modules["scrapers.proxy.proxy_management_client"] = proxy_management_client_mod
        sys.modules.pop("scrapers.airflow.dags.job_scrapers_local_dag", None)

        cls._saved_env = dict(os.environ)
        os.environ.update(
            {
                "JOBSEARCH_AIRFLOW_COMPANIES": "amazon,google",
                "JOBSEARCH_AIRFLOW_SCHEDULE_HOURS": "6",
                "JOBSEARCH_AIRFLOW_TASK_RETRIES": "0",
                "JOBSEARCH_AIRFLOW_TASK_RETRY_DELAY_SECONDS": "60",
                "JOBSEARCH_AIRFLOW_CLIENT_REQUEST_TIMEOUT_SECONDS": "5",
                "JOBSEARCH_AIRFLOW_CLIENT_CONNECT_TIMEOUT_SECONDS": "2",
                "JOBSEARCH_AIRFLOW_CLIENT_MAX_RETRIES": "1",
                "JOBSEARCH_AIRFLOW_CLIENT_BACKOFF_FACTOR": "0.1",
                "JOBSEARCH_AIRFLOW_CLIENT_MAX_BACKOFF_SECONDS": "2",
                "JOBSEARCH_AIRFLOW_CLIENT_BACKOFF_JITTER": "false",
                "JOBSEARCH_PROXY_API_URL": "http://proxy-api",
                "JOBSEARCH_PROXY_API_TIMEOUT_SECONDS": "2",
                "JOBSEARCH_PROXY_LEASE_ACQUIRE_TIMEOUT_SECONDS": "1",
                "JOBSEARCH_PROXY_LEASE_POLL_INTERVAL_SECONDS": "0.1",
                "JOBSEARCH_AIRFLOW_PROXY_SENSOR_POKE_SECONDS": "5",
                "JOBSEARCH_AIRFLOW_PROXY_SENSOR_TIMEOUT_SECONDS": "60",
                "JOBSEARCH_AIRFLOW_PROXY_MIN_AVAILABLE_PER_SCOPE": "1",
                "JOBSEARCH_AIRFLOW_PROXY_SENSOR_SOFT_FAIL": "false",
                "JOBSEARCH_DB_URL": "postgresql://db",
                "JOBSEARCH_FEATURES_API_URL": "http://features:8010",
            }
        )
        _TASK_REGISTRY.clear()
        _SENSOR_CALLABLES.clear()

        cls.mod = importlib.import_module("scrapers.airflow.dags.job_scrapers_local_dag")
        cls.tasks = dict(_TASK_REGISTRY)
        cls.sensor_callables = dict(_SENSOR_CALLABLES)

    @classmethod
    def tearDownClass(cls) -> None:
        os.environ.clear()
        os.environ.update(cls._saved_env)
        for name in cls._module_names:
            if cls._saved_modules[name] is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = cls._saved_modules[name]

    def test_resolve_total_pages_from_metadata(self) -> None:
        response = SimpleNamespace(total_results=101, page_size=10, has_next_page=True)
        self.assertEqual(self.mod._resolve_total_pages(response, None), 11)
        self.assertEqual(self.mod._resolve_total_pages(response, 3), 3)

    def test_resolve_total_pages_fallback(self) -> None:
        has_next = SimpleNamespace(total_results=None, page_size=None, has_next_page=True)
        with self.assertRaises(ValueError):
            self.mod._resolve_total_pages(has_next, None)
        self.assertEqual(self.mod._resolve_total_pages(has_next, 7), 7)

        no_next = SimpleNamespace(total_results=None, page_size=None, has_next_page=False)
        self.assertEqual(self.mod._resolve_total_pages(no_next, None), 1)

    def test_resolve_max_pages(self) -> None:
        with patch.dict(os.environ, {"JOBSEARCH_AIRFLOW_MAX_PAGES": "0"}, clear=False):
            self.assertIsNone(self.mod._resolve_max_pages())
        with patch.dict(os.environ, {"JOBSEARCH_AIRFLOW_MAX_PAGES": "4"}, clear=False):
            self.assertEqual(self.mod._resolve_max_pages(), 4)

    def test_resolve_schedule(self) -> None:
        with patch.object(self.mod, "require_env_int", return_value=12) as req:
            self.assertEqual(self.mod._resolve_schedule(), "0 */12 * * *")
            req.assert_called_once_with("JOBSEARCH_AIRFLOW_SCHEDULE_HOURS", minimum=1)

    def test_resolve_companies_and_scopes(self) -> None:
        with patch.object(self.mod, "require_env", return_value="amazon,meta"), patch.object(
            self.mod, "resolve_companies_from_env", return_value=["amazon", "meta"]
        ) as resolve_companies:
            companies = self.mod._resolve_companies()
            self.assertEqual(companies, ["amazon", "meta"])
            resolve_companies.assert_called_once_with("amazon,meta")

        with patch.object(self.mod, "resolve_proxy_scopes_for_companies", return_value=["a.com"]) as resolve_scopes:
            scopes = self.mod._resolve_proxy_scopes(["amazon"])
            self.assertEqual(scopes, ["a.com"])
            resolve_scopes.assert_called_once_with(["amazon"])

    def test_task_registry_contains_expected_tasks(self) -> None:
        for task_id in (
            "create_publish_run",
            "stage_companies",
            "jobs_get_first_page",
            "jobs_build_page_requests",
            "jobs_get_page",
            "jobs_copy_forward_details",
            "jobs_build_detail_requests",
            "jobs_get_details",
            "verify_db_consistency",
            "update_publish_run",
            "publish_db_pointer",
        ):
            self.assertIn(task_id, self.tasks)
        self.assertIn("wait_for_proxy_capacity", self.sensor_callables)

    def test_task_create_publish_run(self) -> None:
        fn = self.tasks["create_publish_run"].fn
        logical_date = self.mod.datetime(2026, 1, 2, tzinfo=self.mod.timezone.utc)
        context = {"dag_run": SimpleNamespace(run_id="run-1"), "logical_date": logical_date}
        with patch.object(self.mod, "get_current_context", return_value=context), patch.object(
            self.mod, "upsert_publish_run_in_progress"
        ) as upsert:
            out = fn()
        self.assertEqual(out["run_id"], "run-1")
        upsert.assert_called_once()

    def test_task_create_publish_run_without_logical_date_uses_now(self) -> None:
        fn = self.tasks["create_publish_run"].fn
        context = {"dag_run": SimpleNamespace(run_id="run-2")}
        with patch.object(self.mod, "get_current_context", return_value=context), patch.object(
            self.mod, "upsert_publish_run_in_progress"
        ) as upsert:
            out = fn()
        self.assertEqual(out["run_id"], "run-2")
        self.assertIsInstance(out["version_ts"], str)
        self.assertTrue(out["version_ts"])
        upsert.assert_called_once()

    def test_task_stage_companies(self) -> None:
        fn = self.tasks["stage_companies"].fn
        run_info = {"run_id": "r1", "version_ts": "2026-01-01T00:00:00+00:00"}
        with patch.object(self.mod, "upsert_companies") as upsert:
            fn(run_info)
        self.assertEqual(len(upsert.call_args.kwargs["rows"]), 2)

    def test_task_get_first_page(self) -> None:
        fn = self.tasks["jobs_get_first_page"].fn
        client = SimpleNamespace(get_jobs=lambda page: SimpleNamespace(total_results=21, page_size=10, has_next_page=True))
        with patch.object(self.mod, "build_client", return_value=client), patch.object(self.mod, "ProxyManagementClient"):
            out = fn("amazon")
        self.assertEqual(out["company"], "amazon")
        self.assertEqual(out["pages_to_fetch"], 3)
        self.assertTrue(out["success"])

    def test_task_build_page_requests(self) -> None:
        fn = self.tasks["jobs_build_page_requests"].fn
        out = fn(
            [
                {"company": "amazon", "pages_to_fetch": 2},
                {"company": "google", "pages_to_fetch": 1},
            ]
        )
        self.assertEqual(
            out,
            [
                {"company": "amazon", "page": 1},
                {"company": "google", "page": 1},
                {"company": "amazon", "page": 2},
            ],
        )

    def test_task_get_jobs_page(self) -> None:
        fn = self.tasks["jobs_get_page"].fn
        job = SimpleNamespace(
            id="j1",
            name="Engineer",
            detailsUrl="https://x",
            applyUrl="https://y",
            locations=[SimpleNamespace(city="Seattle", state="WA", country="USA")],
            postedTs=1704067200,
        )
        response = SimpleNamespace(jobs=[job], error=None)
        client = SimpleNamespace(get_jobs=lambda page: response)
        with patch.object(self.mod, "build_client", return_value=client), patch.object(self.mod, "ProxyManagementClient"), patch.object(
            self.mod, "upsert_jobs"
        ) as upsert_jobs:
            out = fn({"run_id": "r1", "version_ts": "2026-01-01T00:00:00+00:00"}, "amazon", 1)
        self.assertEqual(out["jobs_seen"], 1)
        self.assertEqual(out["jobs_written"], 1)
        self.assertEqual(out["job_ids"], ["j1"])
        upsert_jobs.assert_called_once()

    def test_task_get_jobs_page_missing_or_empty_job_id_raises(self) -> None:
        fn = self.tasks["jobs_get_page"].fn
        run_info = {"run_id": "r1", "version_ts": "2026-01-01T00:00:00+00:00"}
        response_missing = SimpleNamespace(jobs=[SimpleNamespace(id=None)], error=None)
        response_empty = SimpleNamespace(jobs=[SimpleNamespace(id="   ")], error=None)

        with patch.object(
            self.mod, "build_client", return_value=SimpleNamespace(get_jobs=lambda page: response_missing)
        ), patch.object(self.mod, "ProxyManagementClient"):
            with self.assertRaises(ValueError):
                fn(run_info, "amazon", 1)

        with patch.object(
            self.mod, "build_client", return_value=SimpleNamespace(get_jobs=lambda page: response_empty)
        ), patch.object(self.mod, "ProxyManagementClient"):
            with self.assertRaises(ValueError):
                fn(run_info, "amazon", 1)

    def test_task_copy_forward_details(self) -> None:
        fn = self.tasks["jobs_copy_forward_details"].fn
        run_info = {"run_id": "r1", "version_ts": "2026-01-01T00:00:00+00:00"}

        with patch.object(self.mod, "fetch_latest_published_run_id", return_value=None) as fetch_latest, patch.object(
            self.mod, "copy_job_details_from_run"
        ) as copy_job_details:
            out = fn(run_info)
        self.assertEqual(out["copied_count"], 0)
        self.assertIsNone(out["source_run_id"])
        fetch_latest.assert_called_once_with("postgresql://db", exclude_run_id="r1")
        copy_job_details.assert_not_called()

        with patch.object(self.mod, "fetch_latest_published_run_id", return_value="published-r0"), patch.object(
            self.mod, "copy_job_details_from_run", return_value=2
        ) as copy_job_details:
            out = fn(run_info)
        self.assertEqual(out["copied_count"], 2)
        self.assertEqual(out["source_run_id"], "published-r0")
        copy_job_details.assert_called_once()

    def test_task_build_detail_requests(self) -> None:
        fn = self.tasks["jobs_build_detail_requests"].fn
        run_info = {"run_id": "r1"}
        with patch.object(
            self.mod,
            "fetch_existing_job_detail_ids",
            return_value={"amazon": {"a2"}, "google": set()},
        ):
            out = fn(
                run_info,
                [
                    {"company": "amazon", "job_ids": ["a2", "a1"]},
                    {"company": "google", "job_ids": ["g1"]},
                    {"company": "amazon", "job_ids": ["a1"]},
                ],
            )
        self.assertEqual(
            out,
            [
                {"company": "amazon", "job_id": "a1"},
                {"company": "google", "job_id": "g1"},
            ],
        )

    def test_task_get_job_details_404_and_success(self) -> None:
        fn = self.tasks["jobs_get_details"].fn
        run_info = {"run_id": "r1", "version_ts": "2026-01-01T00:00:00+00:00"}

        response_404 = SimpleNamespace(status=404, error="missing", jobDescription=None, postedTs=None)
        client_404 = SimpleNamespace(get_job_details=lambda job_id: response_404)
        with patch.object(self.mod, "build_client", return_value=client_404), patch.object(
            self.mod, "ProxyManagementClient"
        ), patch.object(self.mod, "mark_missing_details") as mark_missing, patch.object(
            self.mod, "upsert_job_details"
        ) as upsert_job_details:
            out_404 = fn(run_info, "amazon", "j404")
        self.assertTrue(out_404["success"])
        self.assertFalse(out_404["job_detail_written"])
        mark_missing.assert_called_once()
        upsert_job_details.assert_not_called()

        response_ok = SimpleNamespace(status=302, error=None, jobDescription="desc", postedTs=1704067200)
        client_ok = SimpleNamespace(get_job_details=lambda job_id: response_ok)
        with patch.object(self.mod, "build_client", return_value=client_ok), patch.object(
            self.mod, "ProxyManagementClient"
        ), patch.object(self.mod, "put_job_description", return_value="job-details/r1/amazon/j1.txt") as put_job_description, patch.object(
            self.mod, "upsert_job_details"
        ) as upsert_job_details:
            out_ok = fn(run_info, "amazon", "j1")
        self.assertTrue(out_ok["success"])
        self.assertTrue(out_ok["job_detail_written"])
        put_job_description.assert_called_once()
        upsert_job_details.assert_called_once()
        self.assertEqual(upsert_job_details.call_args.kwargs["posted_ts"], datetime(2024, 1, 1, tzinfo=timezone.utc))

    def test_task_get_job_details_invalid_raises(self) -> None:
        fn = self.tasks["jobs_get_details"].fn
        run_info = {"run_id": "r1", "version_ts": "2026-01-01T00:00:00+00:00"}
        response = SimpleNamespace(status=200, error=None, jobDescription=None, postedTs=None)
        client = SimpleNamespace(get_job_details=lambda job_id: response)
        with patch.object(self.mod, "build_client", return_value=client), patch.object(self.mod, "ProxyManagementClient"):
            with self.assertRaises(ValueError):
                fn(run_info, "amazon", "j1")

    def test_task_build_skill_requests(self) -> None:
        fn = self.tasks["jobs_build_skill_requests"].fn
        with patch.object(
            self.mod,
            "fetch_job_skill_requests",
            return_value=[{"company": "amazon", "job_id": "j1", "job_description_path": "job-details/r1/amazon/j1.txt"}],
        ) as fetch_job_skill_requests:
            out = fn({"run_id": "r1"})
        fetch_job_skill_requests.assert_called_once_with(
            "postgresql://db",
            run_id="r1",
            companies=["amazon", "google"],
        )
        self.assertEqual(out, [{"company": "amazon", "job_id": "j1", "job_description_path": "job-details/r1/amazon/j1.txt"}])

    def test_task_extract_job_skills(self) -> None:
        fn = self.tasks["jobs_extract_skills"].fn
        fake_features_client = SimpleNamespace(get_job_skills=lambda text: {"skills": ["Python", "SQL"], "embedding": [0.1, -0.2]})
        with patch.object(self.mod, "get_job_description", return_value="Need Python and SQL"), patch.object(
            self.mod, "FeaturesClient", return_value=fake_features_client
        ), patch.object(self.mod, "update_job_skills") as update_job_skills:
            out = fn(
                {"run_id": "r1"},
                "amazon",
                "j1",
                "job-details/r1/amazon/j1.txt",
            )
        update_job_skills.assert_called_once_with(
            "postgresql://db",
            run_id="r1",
            company="amazon",
            external_job_id="j1",
            skills=["Python", "SQL"],
            job_description_embedding=[0.1, -0.2],
        )
        self.assertTrue(out["success"])
        self.assertEqual(out["skills_written"], 2)

    def test_task_extract_job_skills_empty_description_writes_empty_skills(self) -> None:
        fn = self.tasks["jobs_extract_skills"].fn
        with patch.object(self.mod, "get_job_description", return_value="   "), patch.object(
            self.mod, "update_job_skills"
        ) as update_job_skills:
            out = fn(
                {"run_id": "r1"},
                "amazon",
                "j1",
                "job-details/r1/amazon/j1.txt",
            )
        update_job_skills.assert_called_once_with(
            "postgresql://db",
            run_id="r1",
            company="amazon",
            external_job_id="j1",
            skills=[],
            job_description_embedding=[],
        )
        self.assertTrue(out["success"])
        self.assertEqual(out["skills_written"], 0)

    def test_task_verify_db_consistency(self) -> None:
        fn = self.tasks["verify_db_consistency"].fn
        run_info = {"run_id": "r1"}
        page_results = [
            {"company": "amazon", "job_ids": ["a1"]},
            {"company": "google", "job_ids": ["g1"]},
        ]
        detail_requests = [{"company": "amazon"}, {"company": "google"}]
        with patch.object(
            self.mod,
            "fetch_consistency_counts",
            return_value=(
                {"amazon": 1, "google": 1},
                {"amazon": 0, "google": 0},
                {"amazon": 1, "google": 1},
                {"amazon": 0, "google": 0},
                {"amazon": 0, "google": 0},
            ),
        ):
            out = fn(run_info, page_results, detail_requests)
        self.assertTrue(out["verified"])

        with patch.object(
            self.mod,
            "fetch_consistency_counts",
            return_value=(
                {"amazon": 0, "google": 1},
                {"amazon": 0, "google": 0},
                {"amazon": 1, "google": 1},
                {"amazon": 0, "google": 0},
                {"amazon": 0, "google": 0},
            ),
        ):
            with self.assertRaises(_FakeAirflowFailException):
                fn(run_info, page_results, detail_requests)

    def test_task_verify_db_consistency_more_violation_types(self) -> None:
        fn = self.tasks["verify_db_consistency"].fn
        run_info = {"run_id": "r1"}
        page_results = [{"company": "amazon", "job_ids": ["a1"]}, {"company": "google", "job_ids": ["g1"]}]
        detail_requests = [{"company": "amazon"}, {"company": "google"}]
        with patch.object(
            self.mod,
            "fetch_consistency_counts",
            return_value=(
                {"amazon": 1, "google": 1},
                {"amazon": 0, "google": 0},
                {"amazon": 0, "google": 1},
                {"amazon": 1, "google": 0},
                {"amazon": 0, "google": 1},
            ),
        ):
            with self.assertRaises(_FakeAirflowFailException):
                fn(run_info, page_results, detail_requests)

    def test_proxy_capacity_ready_sensor_callable(self) -> None:
        fn = self.sensor_callables["wait_for_proxy_capacity"]

        pm_client = SimpleNamespace(sizes=lambda scope: {"available": 2, "inuse": 1, "blocked": 0})
        with patch.object(self.mod, "ProxyManagementClient", return_value=pm_client):
            self.assertTrue(fn())

        pm_client_unavailable = SimpleNamespace(sizes=lambda scope: {"available": 0, "inuse": 1, "blocked": 0})
        with patch.object(self.mod, "ProxyManagementClient", return_value=pm_client_unavailable):
            self.assertFalse(fn())

    def test_proxy_capacity_ready_sensor_callable_handles_proxy_api_error(self) -> None:
        fn = self.sensor_callables["wait_for_proxy_capacity"]

        def _raise_sizes(*, scope: str) -> dict[str, int]:
            _ = scope
            raise RuntimeError("boom")

        pm_client_error = SimpleNamespace(sizes=_raise_sizes)
        with patch.object(self.mod, "ProxyManagementClient", return_value=pm_client_error):
            self.assertFalse(fn())

    def test_proxy_capacity_ready_sensor_callable_no_scopes(self) -> None:
        _SENSOR_CALLABLES.clear()
        with patch.object(self.mod, "_resolve_proxy_scopes", return_value=[]):
            self.mod.job_scrapers_local_dag()
        fn = _SENSOR_CALLABLES["wait_for_proxy_capacity"]
        self.assertFalse(fn())

    def test_task_update_publish_run(self) -> None:
        fn = self.tasks["update_publish_run"].fn
        run_info = {"run_id": "r1"}
        with patch.object(self.mod, "update_publish_run_status") as update_status:
            out = fn(run_info, [{"success": True}], [{"success": True}], [{"success": True}])
        self.assertEqual(out["status"], "succeeded")
        update_status.assert_called_once()

        with patch.object(self.mod, "update_publish_run_status"):
            with self.assertRaises(_FakeAirflowFailException):
                fn(run_info, [{"success": False, "error": "x"}], [], [])

    def test_task_publish_db_pointer(self) -> None:
        fn = self.tasks["publish_db_pointer"].fn
        run_info = {"run_id": "r1"}
        with patch.object(self.mod, "publish_jobs_catalog_pointer") as publish:
            out_skip = fn(run_info, {"status": "failed"})
            out_ok = fn(run_info, {"status": "succeeded"})
        self.assertFalse(out_skip["published"])
        self.assertTrue(out_ok["published"])
        publish.assert_called_once_with("postgresql://db", run_id="r1")


if __name__ == "__main__":
    unittest.main()
