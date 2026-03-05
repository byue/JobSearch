#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRAPERS_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"

cd "${SCRAPERS_DIR}"

echo "[proxy-dev] stopping redis + proxy-producer"
docker compose stop proxy-producer redis

echo "[proxy-dev] removing stopped containers"
docker compose rm -f proxy-producer redis >/dev/null 2>&1 || true
