import importlib
import json
import os
import sys
import unittest
from datetime import datetime, timezone
from unittest.mock import patch

import psycopg
from fastapi.testclient import TestClient
from psycopg.rows import dict_row
from testcontainers.postgres import PostgresContainer


def _fresh_import_backend_main(db_url: str, page_size: int = 2):
    sys.modules.pop("web.backend.main", None)
    os.environ["JOBSEARCH_DB_URL"] = db_url
    os.environ["JOBSEARCH_API_PAGE_SIZE"] = str(page_size)
    return importlib.import_module("web.backend.main")


class WebBackendIntegrationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls._pg = PostgresContainer(
            image="postgres:16",
            username="airflow",
            password="airflow",
            dbname="airflow",
        )
        cls._pg.start()

        host = cls._pg.get_container_host_ip()
        port = cls._pg.get_exposed_port(5432)
        cls.db_url = f"postgresql://airflow:airflow@{host}:{port}/airflow"

        with open("src/sql/init.sql", "r", encoding="utf-8") as fh:
            schema_sql = fh.read()
        with psycopg.connect(cls.db_url) as conn:
            with conn.cursor() as cur:
                cur.execute(schema_sql)
            conn.commit()

        cls._job_descriptions: dict[str, str] = {}
        cls.backend = _fresh_import_backend_main(cls.db_url, page_size=2)
        cls._minio_patcher = patch.object(
            cls.backend,
            "get_job_description",
            side_effect=lambda *, key: cls._job_descriptions[key],
        )
        cls._minio_patcher.start()
        cls.client = TestClient(cls.backend.app)

    @classmethod
    def tearDownClass(cls) -> None:
        cls._minio_patcher.stop()
        cls._pg.stop()

    def _exec(self, sql: str, params: tuple = ()) -> None:
        with psycopg.connect(self.db_url, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
            conn.commit()

    def _insert_run(self, run_id: str, *, db_ready: bool) -> None:
        self._exec(
            """
            INSERT INTO publish_runs (
              run_id, version_ts, status, db_ready, es_ready
            ) VALUES (%s, now(), %s, %s, FALSE)
            ON CONFLICT (run_id) DO UPDATE
            SET version_ts = EXCLUDED.version_ts,
                status = EXCLUDED.status,
                db_ready = EXCLUDED.db_ready,
                updated_at = now()
            """,
            (run_id, "succeeded" if db_ready else "in_progress", db_ready),
        )

    def _set_pointer(self, run_id: str) -> None:
        self._exec(
            """
            INSERT INTO publication_pointers (namespace, run_id, updated_at)
            VALUES ('jobs_catalog', %s, now())
            ON CONFLICT (namespace) DO UPDATE
            SET run_id = EXCLUDED.run_id,
                updated_at = now()
            """,
            (run_id,),
        )

    def _insert_company(self, run_id: str, company: str) -> None:
        self._exec(
            """
            INSERT INTO companies (run_id, version_ts, company, display_name, updated_at)
            VALUES (%s, now(), %s, %s, now())
            ON CONFLICT (run_id, company) DO UPDATE
            SET updated_at = now()
            """,
            (run_id, company, company.capitalize()),
        )

    def _insert_job(
        self,
        run_id: str,
        company: str,
        job_id: str,
        *,
        title: str,
        posted_ts: datetime | None,
        details_url: str | None = None,
        apply_url: str | None = None,
        skills: list[str] | None = None,
        is_missing_details: bool = False,
    ) -> None:
        self._exec(
            """
            INSERT INTO jobs (
              run_id, version_ts, company, external_job_id, title,
              details_url, apply_url, city, state, country, skills, posted_ts, is_missing_details, updated_at
            ) VALUES (
              %s, now(), %s, %s, %s,
              %s, %s, 'Seattle', 'WA', 'US', %s::jsonb, %s, %s, now()
            )
            ON CONFLICT (run_id, company, external_job_id) DO UPDATE
            SET title = EXCLUDED.title,
                details_url = EXCLUDED.details_url,
                apply_url = EXCLUDED.apply_url,
                skills = EXCLUDED.skills,
                posted_ts = EXCLUDED.posted_ts,
                is_missing_details = EXCLUDED.is_missing_details,
                updated_at = now()
            """,
            (
                run_id,
                company,
                job_id,
                title,
                details_url,
                apply_url,
                json.dumps(skills or []),
                posted_ts,
                is_missing_details,
            ),
        )

    def _insert_job_details(self, run_id: str, company: str, job_id: str, *, job_description: str) -> None:
        key = f"job-details/{run_id}/{company}/{job_id}.txt"
        self._job_descriptions[key] = job_description
        self._insert_job_details_custom(
            run_id,
            company,
            job_id,
            job_description_path=key,
        )

    def _insert_job_details_custom(
        self,
        run_id: str,
        company: str,
        job_id: str,
        *,
        job_description_path: str | None,
    ) -> None:
        self._exec(
            """
            INSERT INTO job_details (
              run_id, version_ts, company, external_job_id, job_description_path, updated_at
            ) VALUES (
              %s, now(), %s, %s, %s, now()
            )
            ON CONFLICT (run_id, company, external_job_id) DO UPDATE
            SET job_description_path = EXCLUDED.job_description_path,
                updated_at = now()
            """,
            (
                run_id,
                company,
                job_id,
                job_description_path,
            ),
        )

    def test_no_published_snapshot_returns_503(self) -> None:
        run_id = "it_not_ready"
        self._insert_run(run_id, db_ready=False)
        self._set_pointer(run_id)

        resp = self.client.get("/get_companies")
        self.assertEqual(resp.status_code, 503)
        self.assertIn("No published DB snapshot", resp.text)

    def test_get_companies_and_jobs_snapshot_switch_and_pagination(self) -> None:
        old_run = "it_old"
        new_run = "it_new"
        for run in (old_run, new_run):
            self._insert_run(run, db_ready=True)
            self._insert_company(run, "amazon")

        self._insert_job(
            old_run,
            "amazon",
            "old_1",
            title="Old Role",
            posted_ts=datetime(2026, 1, 2, tzinfo=timezone.utc),
        )
        self._insert_job(
            new_run,
            "amazon",
            "new_1",
            title="New Role 1",
            posted_ts=datetime(2026, 1, 4, tzinfo=timezone.utc),
        )
        self._insert_job(
            new_run,
            "amazon",
            "new_2",
            title="New Role 2",
            posted_ts=datetime(2026, 1, 3, tzinfo=timezone.utc),
        )
        self._insert_job(
            new_run,
            "amazon",
            "new_missing",
            title="Missing details role",
            posted_ts=datetime(2026, 1, 5, tzinfo=timezone.utc),
            is_missing_details=True,
        )
        self._insert_job(
            new_run,
            "amazon",
            "new_null",
            title="Null Posted",
            posted_ts=None,
        )

        self._set_pointer(old_run)
        companies_old = self.client.get("/get_companies")
        self.assertEqual(companies_old.status_code, 200)
        self.assertEqual(companies_old.json()["companies"], ["amazon"])

        with patch.object(
            self.backend,
            "_browse_jobs",
            return_value=(
                [
                    self.backend.JobMetadata(
                        id="old_1",
                        runId=old_run,
                        name="Old Role",
                        company="amazon",
                        locations=[self.backend.Location(city="Seattle", state="WA", country="US")],
                    )
                ],
                1,
                False,
            ),
        ):
            old_jobs = self.client.post("/get_jobs", json={"company": "amazon", "pagination_index": 1})
        self.assertEqual(old_jobs.status_code, 200)
        self.assertEqual(old_jobs.json()["jobs"][0]["id"], "old_1")

        self._set_pointer(new_run)
        with patch.object(
            self.backend,
            "_browse_jobs",
            side_effect=[
                (
                    [
                        self.backend.JobMetadata(id="new_1", runId=new_run, name="New Role 1", company="amazon"),
                        self.backend.JobMetadata(id="new_2", runId=new_run, name="New Role 2", company="amazon"),
                    ],
                    3,
                    True,
                ),
                (
                    [
                        self.backend.JobMetadata(id="new_null", runId=new_run, name="Null Posted", company="amazon"),
                    ],
                    3,
                    False,
                ),
            ],
        ):
            page1 = self.client.post("/get_jobs", json={"company": "amazon", "pagination_index": 1})
            self.assertEqual(page1.status_code, 200)
            body1 = page1.json()
            self.assertEqual(body1["total_results"], 3)  # excludes is_missing_details=true
            self.assertEqual(body1["page_size"], 2)
            self.assertEqual(body1["total_pages"], 2)
            self.assertEqual(body1["pagination_index"], 1)
            self.assertTrue(body1["has_next_page"])
            self.assertEqual([job["id"] for job in body1["jobs"]], ["new_1", "new_2"])

            page2 = self.client.post("/get_jobs", json={"company": "amazon", "pagination_index": 2})
        self.assertEqual(page1.status_code, 200)
        self.assertEqual(page2.status_code, 200)
        body2 = page2.json()
        self.assertEqual(body2["page_size"], 2)
        self.assertEqual(body2["total_pages"], 2)
        self.assertEqual(body2["pagination_index"], 2)
        self.assertFalse(body2["has_next_page"])
        self.assertEqual([job["id"] for job in body2["jobs"]], ["new_null"])

    def test_get_job_details_success_not_found_missing_details(self) -> None:
        run_id = "it_details"
        self._insert_run(run_id, db_ready=True)
        self._insert_company(run_id, "amazon")
        self._insert_job(
            run_id,
            "amazon",
            "detail_ok",
            title="Detail OK",
            posted_ts=datetime(2026, 1, 1, tzinfo=timezone.utc),
            details_url="https://www.amazon.jobs/en/jobs/detail_ok",
        )
        self._insert_job_details(run_id, "amazon", "detail_ok", job_description="Great role")
        self._insert_job(
            run_id,
            "amazon",
            "detail_missing",
            title="Detail Missing",
            posted_ts=datetime(2026, 1, 1, tzinfo=timezone.utc),
            details_url="https://www.amazon.jobs/en/jobs/detail_missing",
            is_missing_details=True,
        )
        self._set_pointer(run_id)

        ok = self.client.post("/get_job_details", json={"company": "amazon", "job_id": "detail_ok"})
        self.assertEqual(ok.status_code, 200)
        ok_body = ok.json()
        self.assertEqual(ok_body["jobDescription"], "Great role")
        self.assertEqual(ok_body["skills"], [])
        self.assertEqual(ok_body["detailsUrl"], "https://www.amazon.jobs/en/jobs/detail_ok")

        missing = self.client.post("/get_job_details", json={"company": "amazon", "job_id": "detail_missing"})
        self.assertEqual(missing.status_code, 404)

        not_found = self.client.post("/get_job_details", json={"company": "amazon", "job_id": "does_not_exist"})
        self.assertEqual(not_found.status_code, 404)

    def test_validation_and_company_errors(self) -> None:
        run_id = "it_validation"
        self._insert_run(run_id, db_ready=True)
        self._insert_company(run_id, "amazon")
        self._set_pointer(run_id)

        invalid = self.client.post("/get_jobs", json={"company": "amazon", "pagination_index": 0})
        self.assertEqual(invalid.status_code, 422)

        unsupported = self.client.post("/get_jobs", json={"company": "google", "pagination_index": 1})
        self.assertEqual(unsupported.status_code, 400)
        self.assertIn("Unsupported company", unsupported.text)

        with patch.object(self.backend, "_browse_jobs", return_value=([], 0, False)):
            cased = self.client.post("/get_jobs", json={"company": "Amazon", "pagination_index": 1})
        self.assertEqual(cased.status_code, 200)

    def test_db_failure_returns_500(self) -> None:
        run_id = "it_db_failure"
        self._insert_run(run_id, db_ready=True)
        self._set_pointer(run_id)

        with patch.object(self.backend, "_db_conn", side_effect=RuntimeError("db unavailable")):
            failing_client = TestClient(self.backend.app, raise_server_exceptions=False)
            resp = failing_client.get("/get_companies")
        self.assertEqual(resp.status_code, 500)

    def test_get_companies_empty_for_active_run(self) -> None:
        run_id = "it_empty_companies"
        self._insert_run(run_id, db_ready=True)
        self._set_pointer(run_id)

        resp = self.client.get("/get_companies")
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json(), {"status": 200, "error": None, "companies": []})

    def test_job_details_null_json_fields_normalized(self) -> None:
        run_id = "it_detail_nulls"
        self._insert_run(run_id, db_ready=True)
        self._insert_company(run_id, "amazon")
        self._insert_job(
            run_id,
            "amazon",
            "nulls_1",
            title="Null Fields",
            posted_ts=datetime(2026, 1, 1, tzinfo=timezone.utc),
            details_url="https://www.amazon.jobs/en/jobs/nulls_1",
        )
        self._insert_job_details_custom(
            run_id,
            "amazon",
            "nulls_1",
            job_description_path="job-details/it_detail_nulls/amazon/nulls_1.txt",
        )
        self._job_descriptions["job-details/it_detail_nulls/amazon/nulls_1.txt"] = ""
        self._set_pointer(run_id)

        resp = self.client.post("/get_job_details", json={"company": "amazon", "job_id": "nulls_1"})
        self.assertEqual(resp.status_code, 200)
        body = resp.json()
        self.assertEqual(body["jobDescription"], "")
        self.assertEqual(body["detailsUrl"], "https://www.amazon.jobs/en/jobs/nulls_1")

    def test_response_contract_shapes_stable(self) -> None:
        run_id = "it_contract"
        self._insert_run(run_id, db_ready=True)
        self._insert_company(run_id, "amazon")
        self._insert_job(
            run_id,
            "amazon",
            "contract_1",
            title="Contract Role",
            posted_ts=datetime(2026, 1, 1, tzinfo=timezone.utc),
            details_url="https://www.amazon.jobs/en/jobs/contract_1",
            skills=["Python", "SQL"],
        )
        self._insert_job_details(run_id, "amazon", "contract_1", job_description="Contract details")
        self._set_pointer(run_id)

        companies = self.client.get("/get_companies")
        self.assertEqual(companies.status_code, 200)
        self.assertEqual(set(companies.json().keys()), {"status", "error", "companies"})

        with patch.object(
            self.backend,
            "_browse_jobs",
            return_value=(
                [
                    self.backend.JobMetadata(
                        id="contract_1",
                        runId=run_id,
                        name="Contract Role",
                        company="amazon",
                        locations=[self.backend.Location(city="Seattle", state="WA", country="US")],
                    )
                ],
                1,
                False,
            ),
        ):
            jobs = self.client.post("/get_jobs", json={"company": "amazon", "pagination_index": 1})
        self.assertEqual(jobs.status_code, 200)
        jobs_body = jobs.json()
        self.assertEqual(
            set(jobs_body.keys()),
            {
                "status",
                "error",
                "jobs",
                "total_results",
                "page_size",
                "total_pages",
                "pagination_index",
                "has_next_page",
            },
        )
        self.assertGreaterEqual(len(jobs_body["jobs"]), 1)
        self.assertEqual(
            set(jobs_body["jobs"][0].keys()),
            {"id", "runId", "name", "company", "jobCategory", "locations", "postedTs", "applyUrl", "detailsUrl"},
        )
        if jobs_body["jobs"][0]["locations"]:
            self.assertEqual(set(jobs_body["jobs"][0]["locations"][0].keys()), {"country", "state", "city"})

        details = self.client.post("/get_job_details", json={"company": "amazon", "job_id": "contract_1"})
        self.assertEqual(details.status_code, 200)
        details_body = details.json()
        self.assertEqual(set(details_body.keys()), {"status", "error", "jobDescription", "skills", "postedTs", "detailsUrl"})
        self.assertIsInstance(details_body["jobDescription"], str)
        self.assertEqual(details_body["skills"], ["Python", "SQL"])
        self.assertEqual(details_body["postedTs"], 1767225600)
        self.assertEqual(details_body["detailsUrl"], "https://www.amazon.jobs/en/jobs/contract_1")


if __name__ == "__main__":
    unittest.main()
