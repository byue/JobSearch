#!/bin/sh
set -eu

KIBANA_URL="${KIBANA_URL:-http://kibana:5601}"
DATA_VIEW_ID="${KIBANA_DATA_VIEW_ID:-jobsearch-jobs-catalog}"
DATA_VIEW_NAME="${KIBANA_DATA_VIEW_NAME:-Jobs Catalog}"
DATA_VIEW_TITLE="${KIBANA_DATA_VIEW_TITLE:-jobs_catalog*}"

wait_for_kibana() {
  attempts=0
  while [ "$attempts" -lt 60 ]; do
    if curl -fsS "${KIBANA_URL}/api/status" >/dev/null 2>&1; then
      return 0
    fi
    attempts=$((attempts + 1))
    sleep 2
  done
  echo "Kibana did not become ready at ${KIBANA_URL}" >&2
  return 1
}

create_or_update_data_view() {
  payload=$(cat <<EOF
{"data_view":{"id":"${DATA_VIEW_ID}","name":"${DATA_VIEW_NAME}","title":"${DATA_VIEW_TITLE}"},"override":true}
EOF
)
  curl -fsS \
    -X POST \
    -H 'Content-Type: application/json' \
    -H 'kbn-xsrf: bootstrap' \
    "${KIBANA_URL}/api/data_views/data_view" \
    -d "${payload}" >/dev/null
}

set_default_data_view() {
  payload=$(cat <<EOF
{"data_view_id":"${DATA_VIEW_ID}","force":true}
EOF
)
  curl -fsS \
    -X POST \
    -H 'Content-Type: application/json' \
    -H 'kbn-xsrf: bootstrap' \
    "${KIBANA_URL}/api/data_views/default" \
    -d "${payload}" >/dev/null
}

wait_for_kibana
create_or_update_data_view
set_default_data_view
echo "Bootstrapped Kibana data view ${DATA_VIEW_ID} (${DATA_VIEW_TITLE})"
