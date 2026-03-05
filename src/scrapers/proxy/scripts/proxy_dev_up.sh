#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRAPERS_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"

cd "${SCRAPERS_DIR}"

echo "[proxy-dev] starting redis + proxy-producer"
docker compose up --build -d redis proxy-producer

echo "[proxy-dev] tailing proxy-producer logs (Ctrl+C to stop tail)"
docker compose logs -f proxy-producer
