# SQL

This folder contains DB schema and helper scripts used by scrapers/web.

## Files
- `init.sql`: idempotent schema bootstrap for:
  - `publish_runs`
  - `publication_pointers`
  - `companies`
  - `jobs`
  - `job_details`
- `init_db.sh`: applies `init.sql` to a target Postgres instance.
- `peek_db.sh`: prints readable snapshots from key tables.

## Apply Schema

Using `DATABASE_URL`:
```bash
DATABASE_URL=postgresql://airflow:airflow@localhost:5432/airflow ./src/sql/init_db.sh
```

Using PG env vars:
```bash
PGHOST=localhost PGPORT=5432 PGUSER=airflow PGPASSWORD=airflow PGDATABASE=airflow ./src/sql/init_db.sh
```

## With Docker Compose

Unified stack (`src/docker-compose.yml`) runs schema init automatically via `scraper-db-init`.

Manual run:
```bash
docker compose -f src/docker-compose.yml run --rm scraper-db-init
```

## Inspect Data

Quick peek (all key tables):
```bash
./src/sql/peek_db.sh
```

Specific table/limit:
```bash
./src/sql/peek_db.sh --table jobs --limit 5
./src/sql/peek_db.sh --table job_details --limit 2 --truncate-chars 120
```

Equivalent via Make:
```bash
make db-list
make db-peek TABLE=jobs LIMIT=5
make db-count-jobs
make db-failures
```
