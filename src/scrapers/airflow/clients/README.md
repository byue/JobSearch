# Airflow Clients

This package contains company-specific scraper clients used by the Airflow DAG.

## Purpose
- Provide a uniform client interface for all company integrations.
- Normalize company-specific upstream payloads into shared schemas.
- Route all HTTP calls through shared request/proxy helpers.

## Structure
- `common/`
- shared base interface (`JobsClient`), errors, HTTP helpers, request policy, pay parsing
- `<company>/`
- concrete integration modules (typically `client.py`, `transport.py`, `parser.py`)
- `client_factory.py`
- builds a concrete client for a company key (`amazon`, `apple`, `google`, `meta`, `microsoft`, `netflix`)

## Client Contract
All clients implement `JobsClient` (`common/base.py`):
- `get_jobs(page: int = 1) -> GetJobsResponse`
- `get_job_details(job_id: str) -> GetJobDetailsResponse`

Responses are typed by shared models in `src/web/backend/schemas.py`.

## Error and Retry Semantics
- Network/proxy/transient upstream failures should bubble up as retryable failures.
- Ambiguous "successful" payloads (for example `200` with invalid/missing required fields) should raise `RetryableUpstreamError`.
- True upstream "not found" should return status `404` (used by DAG logic to mark missing details).

## Proxy + Request Behavior
- Clients use shared request helpers in `common/http_requests.py`.
- Requests acquire proxies from proxy management by target host scope.
- Per-endpoint request policies are supported via `RequestPolicy`.

## Adding a New Company Client
1. Add `src/scrapers/airflow/clients/<company>/client.py` and implement `JobsClient`.
2. Add parser/transport helpers as needed.
3. Register the company in `client_factory.py`.
4. Add unit tests under `tst/scrapers/airflow/clients/<company>/`.
5. Verify DAG integration by including the company in `JOBSEARCH_AIRFLOW_COMPANIES`.

## Tests
- Client unit tests: `tst/scrapers/airflow/clients/**`
- Factory tests: `tst/scrapers/airflow/test_client_factory.py`
