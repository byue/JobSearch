#!/usr/bin/env bash
set -euo pipefail

# Print a small, human-readable snapshot of scraper DB tables.
# Usage:
#   ./src/sql/peek_db.sh
#   ./src/sql/peek_db.sh --table jobs --limit 5
#   ./src/sql/peek_db.sh --table jobs,job_details
#   ./src/sql/peek_db.sh --table job_details --limit 2 --truncate-chars 120
# Optional env overrides:
#   COMPOSE_FILE=src/docker-compose.yml
#   DB_SERVICE=postgres
#   DB_USER=airflow
#   DB_NAME=airflow

COMPOSE_FILE="${COMPOSE_FILE:-src/docker-compose.yml}"
DB_SERVICE="${DB_SERVICE:-postgres}"
DB_USER="${DB_USER:-airflow}"
DB_NAME="${DB_NAME:-airflow}"
TABLES_ARG=""
LIMIT=2
TRUNCATE_CHARS="${TRUNCATE_CHARS:-160}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --table)
      TABLES_ARG="${2:-}"
      shift 2
      ;;
    --limit)
      LIMIT="${2:-}"
      shift 2
      ;;
    --truncate-chars)
      TRUNCATE_CHARS="${2:-}"
      shift 2
      ;;
    *)
      echo "Unknown argument: $1" >&2
      echo "Usage: $0 [--table <name[,name...]>] [--limit <n>] [--truncate-chars <n>]" >&2
      exit 2
      ;;
  esac
done

if ! [[ "${LIMIT}" =~ ^[1-9][0-9]*$ ]]; then
  echo "--limit must be a positive integer" >&2
  exit 2
fi
if ! [[ "${TRUNCATE_CHARS}" =~ ^[1-9][0-9]*$ ]]; then
  echo "--truncate-chars must be a positive integer" >&2
  exit 2
fi

declare -a KNOWN_TABLES=(
  "publish_runs"
  "publication_pointers"
  "companies"
  "jobs"
  "job_details"
)

declare -a TABLES=()
if [[ -n "${TABLES_ARG}" ]]; then
  IFS=',' read -r -a RAW_TABLES <<<"${TABLES_ARG}"
  for raw in "${RAW_TABLES[@]}"; do
    table="$(echo "${raw}" | xargs)"
    if [[ -z "${table}" ]]; then
      continue
    fi
    case "${table}" in
      publish_runs|publication_pointers|companies|jobs|job_details)
        TABLES+=("${table}")
        ;;
      *)
        echo "Unsupported table: ${table}" >&2
        echo "Supported tables: ${KNOWN_TABLES[*]}" >&2
        exit 2
        ;;
    esac
  done
else
  TABLES=("${KNOWN_TABLES[@]}")
fi

run_query() {
  local table="$1"
  local query_sql="$2"
  echo ""
  echo "=== ${table} (latest ${LIMIT}, truncate=${TRUNCATE_CHARS}) ==="
  docker compose -f "${COMPOSE_FILE}" exec -T "${DB_SERVICE}" \
    psql -U "${DB_USER}" -d "${DB_NAME}" \
      -v ON_ERROR_STOP=1 \
      -v limit_n="${LIMIT}" \
      -v trunc_n="${TRUNCATE_CHARS}" \
      -v query_sql="${query_sql}" <<'SQL'
\set QUIET 1
\pset pager off
\pset linestyle unicode
\pset border 2
\x auto
\set QUIET 0
:query_sql
SQL
}

for table in "${TABLES[@]}"; do
  case "${table}" in
    publish_runs)
      run_query "${table}" "SELECT * FROM publish_runs ORDER BY created_at DESC LIMIT :limit_n;"
      ;;
    publication_pointers)
      run_query "${table}" "SELECT * FROM publication_pointers ORDER BY updated_at DESC LIMIT :limit_n;"
      ;;
    companies)
      run_query "${table}" "SELECT * FROM companies ORDER BY updated_at DESC LIMIT :limit_n;"
      ;;
    jobs)
      run_query "${table}" "SELECT * FROM jobs ORDER BY updated_at DESC LIMIT :limit_n;"
      ;;
    job_details)
      run_query "${table}" "SELECT run_id, version_ts, company, external_job_id, \
CASE WHEN length(COALESCE(job_description_path,'')) > :trunc_n THEN left(COALESCE(job_description_path,''), :trunc_n) || ' ...' ELSE COALESCE(job_description_path,'') END AS job_description_path, \
updated_at \
FROM job_details ORDER BY updated_at DESC LIMIT :limit_n;"
      ;;
  esac
done
