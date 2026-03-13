# Airflow Scraper Pipeline

This folder contains the local Airflow DAG that scrapes jobs, writes versioned DB rows, validates consistency, and publishes the active DB snapshot pointer.

## DAG
- DAG ID: `job_scrapers_local`
- Schedule: `0 */N * * *`, where `N = JOBSEARCH_AIRFLOW_SCHEDULE_HOURS`
- File: `src/scrapers/airflow/dags/job_scrapers_local_dag.py`

## Current Task Flow
1. `wait_for_proxy_capacity` (`PythonSensor`)
2. `create_publish_run`
3. `stage_companies`
4. `jobs_get_first_page` (mapped per company)
5. `jobs_build_page_requests`
6. `jobs_get_page` (mapped per company/page)
7. `jobs_copy_forward_details`
8. `jobs_build_detail_requests`
9. `jobs_get_details` (mapped per company/job_id)
10. `verify_db_consistency`
11. `update_publish_run`
12. `publish_db_pointer`

Notes:
- Jobs are written to `jobs` during page scrape.
- Matching `job_details` can be copied forward from the currently published run before detail fetches are built.
- Details are written to `job_details` during detail scrape.
- True detail `404` marks `jobs.is_missing_details = TRUE` and does not fail that item.
- `verify_db_consistency` enforces:
  - job counts match expected scraped IDs (excluding `is_missing_details`)
  - `job_details` count matches jobs count
  - no empty `job_description` for included details
- `publish_db_pointer` updates `publication_pointers(namespace='jobs_catalog')` only for succeeded runs.

## Proxy Behavior
- Requests use proxy lease retry wrapper (`_call_with_proxy_retry`).
- Retryable errors include network/proxy errors and `RetryableUpstreamError`.
- Capacity gate uses per-scope and total minimum thresholds before run starts.

## Environment Variables
Main env file:
- `src/scrapers/airflow/docker.env`

Template:
- `src/scrapers/airflow/docker.env.example`

Most relevant knobs:
- `JOBSEARCH_AIRFLOW_COMPANIES`
- `JOBSEARCH_AIRFLOW_MAX_PAGES`
- `JOBSEARCH_AIRFLOW_PROXY_RETRY_ATTEMPTS`
- `JOBSEARCH_AIRFLOW_PROXY_RETRY_BACKOFF_SECONDS`
- `JOBSEARCH_AIRFLOW_FAIL_ON_COMPANY_ERROR`
- `JOBSEARCH_AIRFLOW_PROXY_SENSOR_POKE_SECONDS`
- `JOBSEARCH_AIRFLOW_PROXY_SENSOR_TIMEOUT_SECONDS`
- `JOBSEARCH_AIRFLOW_PROXY_MIN_AVAILABLE_PER_SCOPE`
- `JOBSEARCH_DB_URL`
- `JOBSEARCH_PROXY_API_URL`

## Run (Unified Docker Stack)
From repo root:

```bash
make up
```

Useful commands:

```bash
make ps
make logs SERVICE=airflow-scheduler
make logs SERVICE=airflow-webserver
make logs SERVICE=proxy-producer
make airflow-open
make airflow-schedule-status
make airflow-schedule-enable
make airflow-schedule-disable
```

Stop:

```bash
make down
```

Full teardown:

```bash
make teardown
```

## DB Inspection
```bash
make db-list
make db-peek TABLE=publish_runs
make db-peek TABLE=jobs LIMIT=5
make db-count-jobs
make db-failures
```

## Tests
Unit tests:
```bash
make test-unit
```

Integration tests:
```bash
make test-integration
```
