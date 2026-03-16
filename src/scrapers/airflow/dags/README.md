# `job_scrapers_local` DAG Structure

## Overview
- **DAG ID**: job_scrapers_local
- **Schedule**: `0 */N * * *`, where `N = JOBSEARCH_AIRFLOW_SCHEDULE_HOURS`
- **Purpose**: scrape jobs/details by company, write versioned rows to DB, verify consistency, and atomically move `jobs_catalog` pointer.

## Task Graph
```mermaid
graph TD
  A["wait_for_proxy_capacity: PythonSensor"] --> B["create_publish_run"]
  B --> C["stage_companies"]
  C --> D["jobs_get_first_page - mapped per company"]
  D --> E["jobs_build_page_requests"]
  E --> F["jobs_get_page - mapped per company/page"]
  F --> G["jobs_copy_forward_details"]
  F --> H["jobs_build_detail_requests"]
  G --> H
  H --> I["jobs_get_details - mapped per company/job_id"]
  H --> J["verify_db_consistency"]
  I --> J
  D --> K["update_publish_run"]
  F --> K
  I --> K
  J --> K
  K --> L["publish_db_pointer"]
```

## Step Responsibilities
1. `wait_for_proxy_capacity`
- Polls proxy API `sizes(scope=...)` for all configured scopes.
- Requires:
  - available per selected scope >= `JOBSEARCH_AIRFLOW_PROXY_MIN_AVAILABLE_PER_SCOPE`
  - each scope available >= `JOBSEARCH_AIRFLOW_PROXY_MIN_AVAILABLE_PER_SCOPE`

2. `create_publish_run`
- Upserts `publish_runs` for current Airflow `run_id`.
- Sets `status='in_progress'`, clears prior DB/ES readiness fields.

3. `stage_companies`
- Upserts one `companies` row per configured company for this `run_id`.

4. `jobs_get_first_page` (mapped by company)
- Calls `client.get_jobs(page=1)` with proxy retry wrapper.
- Computes `pages_to_fetch` (`meta` forced to 1; others from response up to `JOBSEARCH_AIRFLOW_MAX_PAGES`).

5. `jobs_build_page_requests`
- Builds `{company, page}` request list for mapped page scraping.

6. `jobs_get_page` (mapped by `{company,page}`)
- Fetches page jobs.
- Writes/updates `jobs` rows immediately (incremental write).
- Persists normalized `job_type` and `locations` JSON for each job.
- Sets `is_missing_details=FALSE` on successful upsert for seen jobs.

7. `jobs_copy_forward_details`
- Reads `publication_pointers(namespace='jobs_catalog')` to find the currently published run, if one exists.
- Copies matching `job_details` rows from that published run into the current `run_id`.
- Backfills current-run `jobs.posted_ts` from the copied source rows when available.

8. `jobs_build_detail_requests`
- Deduplicates scraped IDs.
- Excludes jobs that already have current-run `job_details`, including rows copied forward from the currently published run.
- Builds `{company, job_id}` only for remaining jobs that still need an upstream detail fetch.

9. `jobs_get_details` (mapped by `{company,job_id}`)
- Fetches details with proxy retry wrapper.
- On `404`: marks `jobs.is_missing_details=TRUE` and treats mapped item as handled.
- On success: uploads the description to MinIO, upserts the `job_details` path, and backfills `jobs.posted_ts` if detail has it.

10. `verify_db_consistency`
- Validates per company:
  - `jobs_count == expected_scraped_ids - missing_details_count`
  - `job_details_count == jobs_count` (excluding `is_missing_details=TRUE` jobs)
  - all included `job_details.job_description_path` are non-empty
- Fails run on any violation.

11. `update_publish_run`
- Aggregates task-level mapped errors from first-page/page/detail outputs.
- Updates `publish_runs.status` and `db_ready`.
- Raises failure if any scrape errors exist.

12. `publish_db_pointer`
- Only when run status is `succeeded`.
- Upserts `publication_pointers(namespace='jobs_catalog')` to this `run_id`.
- Sets `publish_runs.db_published_at`.

## Failure Behavior
- Proxy/network/upstream errors are retried inside `_call_with_proxy_retry`.
- If `JOBSEARCH_AIRFLOW_FAIL_ON_COMPANY_ERROR=true`, mapped task errors bubble immediately.
- Otherwise mapped tasks return `success=false`; `update_publish_run` later fails whole DAG run.
- True 404 detail misses do **not** fail the run by themselves; they are represented by `jobs.is_missing_details=TRUE` and excluded from detail parity checks.
- Detail requests are skipped only after a current-run `job_details` row already exists, either from copy-forward or a fresh fetch.

## Main Tables Touched
- `publish_runs`
- `publication_pointers`
- `companies`
- `jobs`
- `job_details`
