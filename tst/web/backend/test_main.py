import asyncio
import importlib
import os
import sys
import unittest
from unittest.mock import Mock, patch

import requests
from fastapi import HTTPException
from starlette.requests import Request


def _fresh_import_backend_main(extra_env: dict[str, str] | None = None):
    sys.modules.pop("web.backend.main", None)
    env = {"JOBSEARCH_DB_URL": "postgresql://user:pass@localhost:5432/jobsearch"}
    if extra_env:
        env.update(extra_env)
    with patch.dict(os.environ, env, clear=False):
        return importlib.import_module("web.backend.main")


class _FakeResult:
    def __init__(self, one=None, many=None):
        self._one = one
        self._many = many if many is not None else []

    def fetchone(self):
        return self._one

    def fetchall(self):
        return self._many


class _FakeConn:
    def __init__(self, results):
        self._results = list(results)
        self.calls = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return None

    def execute(self, sql, params=None):
        self.calls.append((str(sql), params))
        if not self._results:
            raise AssertionError("No fake result left for execute()")
        return self._results.pop(0)


class _DummyResponse:
    pass


def _make_request() -> Request:
    return Request(
        {
            "type": "http",
            "method": "GET",
            "path": "/",
            "headers": [],
            "query_string": b"",
            "server": ("test", 80),
            "client": ("test", 1234),
            "scheme": "http",
        }
    )


class BackendMainTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.module = _fresh_import_backend_main()

    def test_env_int_and_normalize_db_url_and_epoch_seconds(self) -> None:
        m = self.module

        with patch.dict(os.environ, {}, clear=False):
            self.assertEqual(m._env_int("MISSING_TEST_ENV", default=9), 9)
        with patch.dict(os.environ, {"MISSING_TEST_ENV": "abc"}, clear=False):
            self.assertEqual(m._env_int("MISSING_TEST_ENV", default=9), 9)
        with patch.dict(os.environ, {"MISSING_TEST_ENV": "1"}, clear=False):
            self.assertEqual(m._env_int("MISSING_TEST_ENV", default=9, minimum=3), 3)

        self.assertEqual(
            m._normalize_db_url("postgresql+psycopg2://u:p@h/db"),
            "postgresql://u:p@h/db",
        )
        self.assertEqual(
            m._normalize_db_url("postgresql+psycopg://u:p@h/db"),
            "postgresql://u:p@h/db",
        )
        self.assertEqual(m._normalize_db_url("postgresql://u:p@h/db"), "postgresql://u:p@h/db")

        self.assertIsNone(m._epoch_seconds(None))
        naive = m.datetime(2026, 1, 1, 0, 0, 0)
        aware = m.datetime(2026, 1, 1, 0, 0, 0, tzinfo=m.timezone.utc)
        self.assertEqual(m._epoch_seconds(naive), int(aware.timestamp()))
        self.assertEqual(m._epoch_seconds(aware), int(aware.timestamp()))

    def test_load_job_description(self) -> None:
        m = self.module
        self.assertIsNone(m._load_job_description(None))
        self.assertIsNone(m._load_job_description("   "))
        with patch.object(m, "get_job_description", return_value="desc") as get_job_description:
            self.assertEqual(m._load_job_description("  job-details/r1/amazon/j1.txt  "), "desc")
        get_job_description.assert_called_once_with(key="job-details/r1/amazon/j1.txt")

    def test_normalize_skills(self) -> None:
        m = self.module
        self.assertEqual(m._normalize_skills(None), [])
        self.assertEqual(m._normalize_skills([" Python ", 7, "", "SQL "]), ["Python", "SQL"])

    def test_db_conn_uses_psycopg_connect(self) -> None:
        m = self.module
        with patch.object(m.psycopg, "connect", return_value=Mock()) as connect_mock:
            _ = m._db_conn()
        connect_mock.assert_called_once_with(m._DB_URL, row_factory=m.dict_row)

    def test_request_policy_and_client_builders(self) -> None:
        m = self.module
        policy = m._default_request_policy()
        self.assertEqual(policy.timeout_seconds, 10.0)
        self.assertEqual(policy.connect_timeout_seconds, 2.0)

        with patch.object(m, "FeaturesClient", return_value="features-client") as features_client:
            self.assertEqual(m._features_client(), "features-client")
        features_client.assert_called_once()

        with patch.object(m, "ElasticsearchClient", return_value="es-client") as es_client:
            self.assertEqual(m._es_client(), "es-client")
        es_client.assert_called_once()

    def test_active_run_id_success_and_failure(self) -> None:
        m = self.module

        with patch.object(m, "_db_conn", return_value=_FakeConn([_FakeResult(one={"run_id": "run-1"})])):
            self.assertEqual(m._active_run_id(), "run-1")

        with patch.object(m, "_db_conn", return_value=_FakeConn([_FakeResult(one=None)])):
            with self.assertRaises(HTTPException) as ctx:
                m._active_run_id()
        self.assertEqual(ctx.exception.status_code, 503)

    def test_validate_company_in_run(self) -> None:
        m = self.module

        with self.assertRaises(ValueError):
            m._validate_company_in_run("run-1", " ")

        with patch.object(m, "_db_conn", return_value=_FakeConn([_FakeResult(one=None)])):
            with self.assertRaises(ValueError):
                m._validate_company_in_run("run-1", "amazon")

        with patch.object(m, "_db_conn", return_value=_FakeConn([_FakeResult(one={"?column?": 1})])):
            self.assertEqual(m._validate_company_in_run("run-1", "Amazon"), "amazon")

    def test_log_company_request_handles_none_status(self) -> None:
        m = self.module
        with patch.object(m._LOGGER, "info") as info_mock:
            m._log_company_request(endpoint="/x", company="amazon", status=None)
        info_mock.assert_called_once()

    def test_search_helper_normalizers(self) -> None:
        m = self.module
        self.assertIsNone(m._normalize_company_filter(None))
        self.assertIsNone(m._normalize_company_filter("__all__"))
        self.assertEqual(m._normalize_company_filter(" Amazon "), "amazon")
        self.assertEqual(m._normalize_posted_within(" 7d "), "7d")
        self.assertEqual(m._normalize_posted_within("bad"), None)

        with patch.object(m, "_validate_company_in_run", return_value="amazon") as validate_company:
            self.assertIsNone(m._resolve_company_filter("run-1", None))
            self.assertEqual(m._resolve_company_filter("run-1", "Amazon"), "amazon")
        validate_company.assert_called_once_with("run-1", "amazon")

        naive = m.datetime(2026, 1, 1, 0, 0, 0)
        aware = m.datetime(2026, 1, 1, 0, 0, 0, tzinfo=m.timezone.utc)
        self.assertEqual(m._epoch_millis(naive), int(aware.timestamp() * 1000))
        self.assertEqual(m._epoch_millis(aware), int(aware.timestamp() * 1000))
        self.assertEqual(m._epoch_millis(None), None)

        self.assertEqual(m._format_es_posted_ts(1735689600), 1735689600)
        self.assertEqual(m._format_es_posted_ts(1735689600000), 1735689600)
        self.assertEqual(m._format_es_posted_ts("2026-01-01T00:00:00Z"), int(aware.timestamp()))
        self.assertIsNone(m._format_es_posted_ts("bad"))
        self.assertIsNone(m._format_es_posted_ts(""))
        self.assertIsNone(m._format_es_posted_ts(None))

        self.assertEqual(m._build_location_from_source({}), [])
        locations = m._build_location_from_source({"city": "Seattle", "state": "WA", "country": "US"})
        self.assertEqual(len(locations), 1)
        self.assertEqual(locations[0].city, "Seattle")

        self.assertEqual(m._hits_from_response({}), [])
        self.assertEqual(m._hits_from_response({"hits": {"hits": [{"_id": "1"}]}}), [{"_id": "1"}])
        self.assertEqual(m._total_hits_from_response({}), 0)
        self.assertEqual(m._total_hits_from_response({"hits": {"total": {"value": 3}}}), 3)
        self.assertEqual(m._total_hits_from_response({"hits": {"total": 4}}), 4)

        self.assertEqual(m._search_filters(None, None), [])
        self.assertEqual(m._search_filters("amazon", None), [{"term": {"company": "amazon"}}])
        self.assertEqual(m._search_filters("amazon", "7d"), [{"term": {"company": "amazon"}}, {"range": {"posted_ts": {"gte": "now-7d"}}}])

    def test_job_metadata_and_browse_helpers(self) -> None:
        m = self.module
        empty_job = m._job_metadata_from_hit({"_source": None})
        self.assertEqual(empty_job.id, "")
        self.assertEqual(empty_job.locations, [])

        source_hit = {
            "_source": {
                "external_job_id": "job-1",
                "run_id": "run-1",
                "title": "Role",
                "company": "amazon",
                "city": "Seattle",
                "state": "WA",
                "country": "US",
                "posted_ts": "2026-01-01T00:00:00Z",
                "apply_url": "https://apply",
                "details_url": "https://details",
            }
        }
        job = m._job_metadata_from_hit(source_hit)
        self.assertEqual(job.id, "job-1")
        self.assertEqual(job.runId, "run-1")
        self.assertEqual(job.company, "amazon")
        self.assertEqual(job.locations[0].country, "US")

        es_client = Mock()
        es_client.search.return_value = {"hits": {"hits": [source_hit], "total": {"value": 2}}}
        with patch.object(m, "_es_client", return_value=es_client):
            jobs, total, has_next = m._browse_jobs("amazon", posted_within="7d", page=1)
        self.assertEqual(len(jobs), 1)
        self.assertEqual(total, 2)
        self.assertTrue(has_next)
        body = es_client.search.call_args.kwargs["body"]
        self.assertEqual(body["sort"][0]["posted_ts"]["order"], "desc")
        self.assertEqual(body["query"]["bool"]["filter"], [{"term": {"company": "amazon"}}, {"range": {"posted_ts": {"gte": "now-7d"}}}])

    def test_query_embedding_rrf_and_search_helpers(self) -> None:
        m = self.module
        with patch.object(m, "_features_client", return_value=Mock(get_query_embedding=lambda text: {"embedding": [1, "2"]})):
            self.assertEqual(m._query_embedding("python"), [1.0, 2.0])
        with patch.object(m, "_features_client", return_value=Mock(get_query_embedding=lambda text: {"embedding": "bad"})):
            self.assertEqual(m._query_embedding("python"), [])

        fused = m._rrf_fuse_hits(
            [{"_id": "a", "_source": {"posted_ts": "2026-01-01T00:00:00Z"}}, {"_id": "", "_source": {}}],
            [{"_id": "b", "_source": {"posted_ts": "2025-01-01T00:00:00Z"}}, {"_id": "a", "_source": {"posted_ts": "2026-01-01T00:00:00Z"}}],
        )
        self.assertEqual([item["_id"] for item in fused], ["a", "b"])

        fused_tie = m._rrf_fuse_hits(
            [{"_id": "old", "_source": {"posted_ts": "2025-01-01T00:00:00Z"}}, {"_id": "new", "_source": {"posted_ts": "2026-01-01T00:00:00Z"}}],
            [{"_id": "new", "_source": {"posted_ts": "2026-01-01T00:00:00Z"}}, {"_id": "old", "_source": {"posted_ts": "2025-01-01T00:00:00Z"}}],
        )
        self.assertEqual([item["_id"] for item in fused_tie], ["new", "old"])

        es_client = Mock()
        es_client.search.side_effect = [
            {
                "hits": {
                    "hits": [
                        {"_id": "a", "_source": {"external_job_id": "a", "company": "amazon", "run_id": "r1", "title": "Role A"}},
                        {"_id": "b", "_source": {"external_job_id": "b", "company": "amazon", "run_id": "r1", "title": "Role B"}},
                    ]
                }
            },
            {
                "hits": {
                    "hits": [
                        {"_id": "b", "_source": {"external_job_id": "b", "company": "amazon", "run_id": "r1", "title": "Role B"}},
                    ]
                }
            },
        ]
        with patch.object(m, "_query_embedding", return_value=[0.1, -0.2]), patch.object(m, "_es_client", return_value=es_client):
            jobs, total, has_next = m._search_jobs("amazon", query="python", page=1, posted_within=None)
        self.assertEqual(len(jobs), 2)
        self.assertEqual(total, 2)
        self.assertFalse(has_next)
        bm25_body = es_client.search.call_args_list[0].kwargs["body"]
        knn_body = es_client.search.call_args_list[1].kwargs["body"]
        self.assertEqual(bm25_body["query"]["bool"]["must"][0]["multi_match"]["query"], "python")
        self.assertEqual(knn_body["knn"]["query_vector"], [0.1, -0.2])

    def test_startup_and_shutdown_events(self) -> None:
        m = self.module
        with patch.object(m._LOGGER, "info") as info_mock:
            asyncio.run(m.startup_event())
        info_mock.assert_called_once()
        self.assertIsNone(asyncio.run(m.shutdown_event()))

    def test_translate_client_errors_paths(self) -> None:
        m = self.module
        req = _make_request()

        async def call_ok(_request):
            return _DummyResponse()

        self.assertIsInstance(asyncio.run(m.translate_client_errors(req, call_ok)), _DummyResponse)

        async def call_value_error(_request):
            raise ValueError("bad")

        resp = asyncio.run(m.translate_client_errors(req, call_value_error))
        self.assertEqual(resp.status_code, 400)

        resp_429 = requests.Response()
        resp_429.status_code = 429

        async def call_http_429(_request):
            raise requests.exceptions.HTTPError(response=resp_429)

        out = asyncio.run(m.translate_client_errors(req, call_http_429))
        self.assertEqual(out.status_code, 429)

        resp_500 = requests.Response()
        resp_500.status_code = 500

        async def call_http_500(_request):
            raise requests.exceptions.HTTPError(response=resp_500)

        out = asyncio.run(m.translate_client_errors(req, call_http_500))
        self.assertEqual(out.status_code, 502)

        async def call_http_none(_request):
            raise requests.exceptions.HTTPError("boom")

        out = asyncio.run(m.translate_client_errors(req, call_http_none))
        self.assertEqual(out.status_code, 502)

        async def call_req_exc(_request):
            raise requests.exceptions.RequestException("net")

        out = asyncio.run(m.translate_client_errors(req, call_req_exc))
        self.assertEqual(out.status_code, 502)

    def test_get_jobs(self) -> None:
        m = self.module
        request = _make_request()
        payload = m.GetJobsRequest(company="amazon", pagination_index=1)

        with patch.object(m, "_active_run_id", return_value="run-1"), patch.object(
            m, "_validate_company_in_run", return_value="amazon"
        ), patch.object(
            m,
            "_browse_jobs",
            return_value=(
                [
                    m.JobMetadata(
                        id="job-1",
                        runId="run-1",
                        name="Role",
                        company="amazon",
                        locations=[m.Location(city="Seattle", state="WA", country="US")],
                        postedTs=1735689600,
                        detailsUrl="https://www.amazon.jobs/en/jobs/job-1",
                        applyUrl="https://www.amazon.jobs/applicant/jobs/job-1/apply",
                    )
                ],
                2,
                True,
            ),
        ):
            response = m.get_jobs(payload, request)

        self.assertEqual(response.status, 200)
        self.assertEqual(response.total_results, 2)
        self.assertEqual(response.page_size, m._PAGE_SIZE)
        self.assertEqual(response.total_pages, 1)
        self.assertEqual(response.pagination_index, 1)
        self.assertTrue(response.has_next_page)
        self.assertEqual(len(response.jobs), 1)
        self.assertEqual(response.jobs[0].id, "job-1")
        self.assertEqual(response.jobs[0].runId, "run-1")

        with patch.object(m, "_active_run_id", return_value="run-1"), patch.object(
            m, "_validate_company_in_run", return_value="amazon"
        ), patch.object(
            m,
            "_search_jobs",
            return_value=([], 0, False),
        ) as search_jobs:
            payload = m.GetJobsRequest(company="amazon", query="python", pagination_index=1)
            m.get_jobs(payload, request)
        search_jobs.assert_called_once_with("amazon", query="python", page=1, posted_within=None)
        self.assertEqual(response.jobs[0].detailsUrl, "https://www.amazon.jobs/en/jobs/job-1")
        self.assertEqual(response.jobs[0].applyUrl, "https://www.amazon.jobs/applicant/jobs/job-1/apply")

    def test_get_jobs_search_and_all_companies(self) -> None:
        m = self.module
        request = _make_request()
        payload = m.GetJobsRequest(company=None, query="python", pagination_index=1)

        with patch.object(m, "_active_run_id", return_value="run-1"), patch.object(
            m, "_search_jobs", return_value=([], 0, False)
        ):
            response = m.get_jobs(payload, request)

        self.assertEqual(response.total_results, 0)
        self.assertEqual(response.page_size, m._PAGE_SIZE)
        self.assertEqual(response.total_pages, 1)
        self.assertFalse(response.has_next_page)
        self.assertEqual(response.jobs, [])

    def test_get_jobs_handles_search_backend_failure(self) -> None:
        m = self.module
        request = _make_request()
        payload = m.GetJobsRequest(company=None, pagination_index=1)
        with patch.object(m, "_active_run_id", return_value="run-1"), patch.object(
            m, "_browse_jobs", side_effect=requests.exceptions.RequestException("boom")
        ):
            with self.assertRaises(HTTPException) as ctx:
                m.get_jobs(payload, request)
        self.assertEqual(ctx.exception.status_code, 502)

    def test_get_companies(self) -> None:
        m = self.module
        conn = _FakeConn([_FakeResult(many=[{"company": "amazon"}, {"company": "google"}])])

        with patch.object(m, "_active_run_id", return_value="run-1"), patch.object(
            m, "_db_conn", return_value=conn
        ):
            response = m.get_companies()

        self.assertEqual(response.status, 200)
        self.assertEqual(response.companies, ["amazon", "google"])

    def test_get_job_details_success(self) -> None:
        m = self.module
        request = _make_request()
        payload = m.GetJobDetailsRequest(job_id="job-1", company="amazon")
        conn = _FakeConn(
            [
                _FakeResult(
                    one={
                        "is_missing_details": False,
                        "external_job_id": "job-1",
                        "title": "Role",
                        "details_url": "https://www.amazon.jobs/en/jobs/job-1",
                        "apply_url": "https://www.amazon.jobs/applicant/jobs/job-1/apply",
                        "skills": ["Python", "SQL"],
                        "posted_ts": m.datetime(2026, 1, 1, tzinfo=m.timezone.utc),
                        "job_description_path": "job-details/run-1/amazon/job-1.txt",
                    }
                )
            ]
        )

        with patch.object(m, "_active_run_id", return_value="run-1"), patch.object(
            m, "_validate_company_in_run", return_value="amazon"
        ), patch.object(m, "_db_conn", return_value=conn), patch.object(
            m, "_load_job_description", return_value="desc"
        ):
            response = m.get_job_details(payload, request)

        self.assertEqual(response.status, 200)
        self.assertEqual(response.jobDescription, "desc")
        self.assertEqual(response.skills, ["Python", "SQL"])
        self.assertEqual(response.detailsUrl, "https://www.amazon.jobs/en/jobs/job-1")

    def test_get_job_details_uses_explicit_run_id(self) -> None:
        m = self.module
        request = _make_request()
        payload = m.GetJobDetailsRequest(job_id="job-1", company="amazon", runId="run-2")
        conn = _FakeConn(
            [
                _FakeResult(
                    one={
                        "is_missing_details": False,
                        "details_url": "https://www.amazon.jobs/en/jobs/job-1",
                        "skills": [],
                        "posted_ts": None,
                        "job_description_path": "job-details/run-2/amazon/job-1.txt",
                    }
                )
            ]
        )
        with patch.object(m, "_validate_company_in_run", return_value="amazon") as validate_company, patch.object(
            m, "_db_conn", return_value=conn
        ), patch.object(m, "_load_job_description", return_value="desc"):
            response = m.get_job_details(payload, request)
        self.assertEqual(response.jobDescription, "desc")
        validate_company.assert_called_once_with("run-2", "amazon")

    def test_get_job_details_success_without_optional_fields(self) -> None:
        m = self.module
        request = _make_request()
        payload = m.GetJobDetailsRequest(job_id="job-2", company="amazon")
        conn = _FakeConn(
            [
                _FakeResult(
                    one={
                        "is_missing_details": False,
                        "external_job_id": "job-2",
                        "title": "Role Two",
                        "details_url": "https://www.amazon.jobs/en/jobs/job-2",
                        "apply_url": "https://www.amazon.jobs/applicant/jobs/job-2/apply",
                        "skills": None,
                        "posted_ts": m.datetime(2026, 1, 2, tzinfo=m.timezone.utc),
                        "job_description_path": "job-details/run-1/amazon/job-2.txt",
                    }
                )
            ]
        )

        with patch.object(m, "_active_run_id", return_value="run-1"), patch.object(
            m, "_validate_company_in_run", return_value="amazon"
        ), patch.object(m, "_db_conn", return_value=conn), patch.object(
            m, "_load_job_description", return_value="desc2"
        ):
            response = m.get_job_details(payload, request)

        self.assertEqual(response.status, 200)
        self.assertEqual(response.jobDescription, "desc2")
        self.assertEqual(response.skills, [])
        self.assertEqual(response.detailsUrl, "https://www.amazon.jobs/en/jobs/job-2")

    def test_get_job_details_not_found_and_missing_details(self) -> None:
        m = self.module
        request = _make_request()
        payload = m.GetJobDetailsRequest(job_id="job-1", company="amazon")

        with patch.object(m, "_active_run_id", return_value="run-1"), patch.object(
            m, "_validate_company_in_run", return_value="amazon"
        ), patch.object(m, "_db_conn", return_value=_FakeConn([_FakeResult(one=None)])):
            with self.assertRaises(HTTPException) as ctx:
                m.get_job_details(payload, request)
        self.assertEqual(ctx.exception.status_code, 404)

        with patch.object(m, "_active_run_id", return_value="run-1"), patch.object(
            m, "_validate_company_in_run", return_value="amazon"
        ), patch.object(
            m,
            "_db_conn",
            return_value=_FakeConn(
                [
                    _FakeResult(
                        one={
                            "is_missing_details": True,
                            "skills": None,
                            "job_description_path": None,
                        }
                    )
                ]
            ),
        ):
            with self.assertRaises(HTTPException) as ctx2:
                m.get_job_details(payload, request)
        self.assertEqual(ctx2.exception.status_code, 404)


if __name__ == "__main__":
    unittest.main()
