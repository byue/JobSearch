# Web Stack (Docker)

## Services
- `frontend` (React + Vite build served by Nginx): `http://localhost:5173`
- `backend` (FastAPI): `http://localhost:8000`
- `features` (FastAPI): `http://localhost:8010`
- `postgres` (DB for published snapshots)
- `scraper-db-init` (one-shot schema init from `src/sql/init.sql`)

## Run
- `make up`
- `make logs`
- `make down`

Equivalent compose command:
- `docker compose -f src/docker-compose.yml up -d --build`

## Notes
- Frontend is built with `VITE_API_BASE_URL=http://localhost:8000`.
- Backend reads from Postgres using `JOBSEARCH_DB_URL`.
- Backend also queries Elasticsearch for search and ES-derived location filter values.
- `scraper-db-init` is idempotent and safe to re-run.

## Backend CLI
- Run through Make:
  - `make web-api ARGS="get-companies"`
  - `make web-api ARGS="get-jobs --company amazon --page 1 --job-type software_engineer --country 'United States'"`
  - `make web-api ARGS="get-location-filters --company amazon --country 'United States'"`
  - `make web-api ARGS="get-job-details --company amazon --job-id <id>"`

- Direct script:
  - `PYTHONPATH=src .venv/bin/python src/web/backend/scripts/web_api_cli.py get-companies`

## Features CLI

```bash
make features-api ARGS='get-job-skills --text "Python and Docker experience"'
make features-api ARGS='normalize-locations --location "Seattle, WA, USA"'
```
