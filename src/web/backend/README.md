# Web Backend

FastAPI service serving job data from the published DB snapshot and Elasticsearch index.

## Endpoints
- `GET /get_companies`
- `POST /get_jobs`
- `GET /get_location_filters`
- `POST /get_job_details`

## `/get_jobs` filters

Supported request fields:
- `company`
- `query`
- `posted_within`
- `job_type`
- `country`
- `region`
- `city`
- `pagination_index`

`job_type` currently supports:
- `software_engineer`
- `machine_learning_engineer`
- `data_scientist`
- `manager`

Location filtering is applied against nested `locations` documents in Elasticsearch, so a multi-location job matches if any stored location object satisfies the selected filter set.

## `/get_location_filters`

Returns ES-derived dropdown values for:
- `countries`
- `regions`
- `cities`

Optional query params:
- `company`
- `posted_within`
- `job_type`
- `country`
- `region`

## Environment
- `JOBSEARCH_DB_URL` (required)
- `JOBSEARCH_API_PAGE_SIZE` (optional, default `25`)

## Run Locally
```bash
PYTHONPATH=src .venv/bin/python -m uvicorn web.backend.main:app --host 0.0.0.0 --port 8000
```

## CLI
```bash
make web-api ARGS="get-companies"
make web-api ARGS="get-jobs --company amazon --page 1 --job-type software_engineer"
make web-api ARGS="get-location-filters --company amazon --country 'United States'"
make web-api ARGS="get-job-details --company amazon --job-id <id>"
```

## Tests
Unit tests:
```bash
PYTHONPATH=src .venv/bin/python -m pytest --import-mode=importlib tst/web/backend
```

Integration tests:
```bash
PYTHONPATH=src .venv/bin/python -m pytest --import-mode=importlib integration/web/backend -v
```

## Docker
The backend runs in the unified stack:
```bash
make up
```
Service URL: `http://localhost:8000`
