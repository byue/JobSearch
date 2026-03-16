# Scrapers

This package contains the scraping pipeline and proxy subsystem used to collect jobs and persist versioned DB snapshots.

## Subsystems
- `airflow/`: DAG orchestration and company client execution.
- `proxy/`: proxy producer + proxy API + Redis-backed lease manager.
- `common/`: shared helpers used across scraper services.

## Main Flow
1. Airflow waits for minimum proxy capacity.
2. Airflow creates a `publish_runs` entry.
3. Company clients fetch jobs pages and write `jobs` rows.
4. Company clients fetch details and write `job_details` rows.
5. Consistency checks run.
6. Run status is updated and DB pointer is published for succeeded runs.

Location flow:
- company clients extract raw upstream location strings
- those strings are normalized through the features service
- normalized location objects are stored on `jobs.locations`

## Data Model
DB schema and initialization scripts are in:
- `src/sql/init.sql`
- `src/sql/init_db.sh`
- `src/sql/README.md`

## Docs
- Features service: `src/features/README.md`
- Airflow pipeline: `src/scrapers/airflow/README.md`
- Airflow clients package: `src/scrapers/airflow/clients/README.md`
- Proxy subsystem: `src/scrapers/proxy/README.md`
- Proxy CLI: `src/scrapers/proxy/scripts/README.md`

## Local Run
From repo root:

```bash
make up
```

Useful checks:

```bash
make ps
make proxy-state
make db-list
make db-peek TABLE=publish_runs
```

Stop/teardown:

```bash
make down
make teardown
```

## Tests
```bash
make test-unit
make test-integration
make test
```
