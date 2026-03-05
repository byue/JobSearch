#!/usr/bin/env bash
set -euo pipefail

# Initialize scraper DB schema from src/sql/init.sql.
#
# Connection options:
# 1) DATABASE_URL (recommended), e.g. postgresql://user:pass@localhost:5432/dbname
# 2) Standard PG* env vars (PGHOST, PGPORT, PGUSER, PGPASSWORD, PGDATABASE)
#
# Examples:
#   DATABASE_URL=postgresql://airflow:airflow@localhost:5432/airflow ./src/sql/init_db.sh
#   PGHOST=localhost PGPORT=5432 PGUSER=airflow PGPASSWORD=airflow PGDATABASE=airflow ./src/sql/init_db.sh

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
SCHEMA_FILE="${ROOT_DIR}/src/sql/init.sql"

if ! command -v psql >/dev/null 2>&1; then
  echo "psql is required but not found on PATH" >&2
  exit 1
fi

if [[ ! -f "${SCHEMA_FILE}" ]]; then
  echo "schema file not found: ${SCHEMA_FILE}" >&2
  exit 1
fi

if [[ -n "${DATABASE_URL:-}" ]]; then
  echo "Applying schema using DATABASE_URL"
  psql "${DATABASE_URL}" -v ON_ERROR_STOP=1 -f "${SCHEMA_FILE}"
else
  : "${PGHOST:?PGHOST is required when DATABASE_URL is not set}"
  : "${PGPORT:?PGPORT is required when DATABASE_URL is not set}"
  : "${PGUSER:?PGUSER is required when DATABASE_URL is not set}"
  : "${PGDATABASE:?PGDATABASE is required when DATABASE_URL is not set}"
  echo "Applying schema using PG* env vars to ${PGUSER}@${PGHOST}:${PGPORT}/${PGDATABASE}"
  psql -v ON_ERROR_STOP=1 -f "${SCHEMA_FILE}"
fi

echo "Schema applied successfully from ${SCHEMA_FILE}"
