#!/bin/bash
# Near-real-time bridge: triggers in-process gap-fill via the API endpoint.
# The API handles DuckDB locking internally (no cross-process lock conflicts).

set -euo pipefail

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
. "$SCRIPT_DIR/lib/runtime_env.sh"
lh_load_runtime_env host

log() { echo "[$(date -Iseconds)] $1"; }

log "Triggering gap-fill via API at ${API_URL}"

response=$(curl -s -w "\n%{http_code}" --max-time 120 \
    -X POST "${API_URL}/api/v1/gap-fill")

http_code=$(echo "$response" | tail -1)
body=$(echo "$response" | sed '$d')

if [ "$http_code" = "200" ]; then
    total=$(echo "$body" | python3 -c "import sys,json; print(json.load(sys.stdin).get('total_inserted',0))" 2>/dev/null || echo "?")
    duration=$(echo "$body" | python3 -c "import sys,json; print(json.load(sys.stdin).get('duration_seconds','?'))" 2>/dev/null || echo "?")
    log "Gap-fill complete: ${total} rows inserted in ${duration}s"
elif [ "$http_code" = "409" ]; then
    log "Gap-fill already in progress, skipping"
elif [ "$http_code" = "400" ]; then
    log "Gap-fill config error: $body"
    exit 1
else
    log "Gap-fill failed (HTTP ${http_code}): $body"
    exit 1
fi
