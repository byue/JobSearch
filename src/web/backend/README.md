# Web Backend

FastAPI service serving job data from the published DB snapshot.

## Endpoints
- `GET /get_companies`
- `POST /get_jobs`
- `POST /get_job_details`

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
make web-api ARGS="get-jobs --company amazon --page 1"
make web-api ARGS="get-job-details --company amazon --job-id <id>"
```

## Tests
Unit tests:
```bash
PYTHONPATH=src .venv/bin/python -m unittest discover -s tst/web/backend -p "test_*.py"
```

Integration tests:
```bash
PYTHONPATH=src .venv/bin/python -m unittest discover -s integration/web/backend -p "test_*.py" -v
```

## Docker
The backend runs in the unified stack:
```bash
make up
```
Service URL: `http://localhost:8000`
