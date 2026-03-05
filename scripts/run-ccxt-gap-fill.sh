#!/bin/bash
# Near-real-time bridge: triggers in-process gap-fill via the API endpoint.
# The API handles DuckDB locking internally (no cross-process lock conflicts).

set -euo pipefail

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
. "$SCRIPT_DIR/lib/runtime_env.sh"
lh_load_runtime_env host

log() { echo "[$(date -Iseconds)] $1"; }

log "Triggering gap-fill via API at ${API_URL}"

token_header=""
if [ -n "${REKTSLUG_INTERNAL_TOKEN:-}" ]; then
    token_header="-H X-Internal-Token:${REKTSLUG_INTERNAL_TOKEN}"
fi

api_post_capture() {
    local endpoint="$1"
    local timeout="${2:-10}"

    if command -v curl >/dev/null 2>&1; then
        curl -s -w "\n%{http_code}" --max-time "$timeout" \
            $token_header \
            -X POST "${API_URL}${endpoint}" 2>/dev/null || true
        return 0
    fi

    python3 - "$API_URL" "$endpoint" "$timeout" "${REKTSLUG_INTERNAL_TOKEN:-}" <<'PY'
import http.client
import sys
from urllib.parse import urlparse

api_url = sys.argv[1]
endpoint = sys.argv[2]
timeout = float(sys.argv[3])
token = sys.argv[4]
parsed = urlparse(api_url)
host = parsed.hostname or "127.0.0.1"
port = parsed.port or 80

headers = {"Content-Length": "0"}
if token:
    headers["X-Internal-Token"] = token

body = ""
code = 0
try:
    conn = http.client.HTTPConnection(host, port, timeout=timeout)
    conn.request("POST", endpoint, body=None, headers=headers)
    resp = conn.getresponse()
    code = resp.status
    body = resp.read().decode("utf-8", errors="replace")
    conn.close()
except Exception:
    code = 0
    body = ""

sys.stdout.write(body)
sys.stdout.write("\n")
sys.stdout.write(str(code).zfill(3))
PY
}

test_db_write_access() {
    uv run --project "$PROJECT_DIR" python -c "
import duckdb, sys
try:
    conn = duckdb.connect('${DB_PATH}', read_only=False)
    conn.close()
    sys.exit(0)
except Exception:
    sys.exit(1)
" >/dev/null 2>&1
}

wait_for_db_write_access() {
    local attempts="${1:-5}"
    local delay_seconds="${2:-2}"
    local attempt=1

    while [ "$attempt" -le "$attempts" ]; do
        if test_db_write_access; then
            return 0
        fi
        if [ "$attempt" -lt "$attempts" ]; then
            sleep "$delay_seconds"
        fi
        attempt=$((attempt + 1))
    done

    return 1
}

run_gap_fill_cli_fallback() {
    local reason="$1"
    local -a symbol_args=()
    local prep_result refresh_result cli_status=0
    local prep_response prep_code refresh_response refresh_code
    local output_file
    local symbol

    for symbol in $SYMBOLS; do
        symbol_args+=("$symbol")
    done

    log "API gap-fill endpoint unavailable (${reason}), falling back to direct CLI execution"
    prep_response=$(api_post_capture "/api/v1/prepare-for-ingestion" 10)
    prep_code=$(echo "$prep_response" | tail -1)
    prep_result=$(echo "$prep_response" | sed '$d')
    if [ -z "$prep_result" ] || [ "$prep_code" = "000" ]; then
        prep_result='{"status":"api_unavailable"}'
    fi
    log "API prepare before CLI fallback: ${prep_result}"
    # Give in-flight readers time to drain after lock acquisition.
    sleep 1

    if ! wait_for_db_write_access 5 2; then
        log "SKIPPED_LOCK_CONTENTION: DuckDB still busy after prepare, skipping this cycle"
        cli_status=0
    else
        output_file="$(mktemp -t lh-gapfill.XXXXXX)"
        if uv run --project "$PROJECT_DIR" python "${PROJECT_DIR}/scripts/fill_gap_from_ccxt.py" \
            --symbols "${symbol_args[@]}" \
            --ccxt-catalog "$CCXT_CATALOG" \
            --db "$DB_PATH" 2>&1 | tee "$output_file"; then
            cli_status=0
        else
            cli_status=$?
            if grep -qiE "Could not set lock|Conflicting lock" "$output_file"; then
                log "SKIPPED_LOCK_CONTENTION: DuckDB lock contention during fallback fill, skipping this cycle"
                cli_status=0
            fi
        fi
        rm -f "$output_file"
    fi

    refresh_response=$(api_post_capture "/api/v1/refresh-connections" 10)
    refresh_code=$(echo "$refresh_response" | tail -1)
    refresh_result=$(echo "$refresh_response" | sed '$d')
    if [ -z "$refresh_result" ] || [ "$refresh_code" = "000" ]; then
        refresh_result='{"status":"api_unavailable"}'
    fi
    log "API refresh after CLI fallback: ${refresh_result}"

    return "$cli_status"
}

response=$(api_post_capture "/api/v1/gap-fill" 30)

http_code=$(echo "$response" | tail -1)
body=$(echo "$response" | sed '$d')

if [ "$http_code" = "200" ]; then
    total=$(echo "$body" | python3 -c "import sys,json; print(json.load(sys.stdin).get('total_inserted',0))" 2>/dev/null || echo "?")
    duration=$(echo "$body" | python3 -c "import sys,json; print(json.load(sys.stdin).get('duration_seconds','?'))" 2>/dev/null || echo "?")
    log "Gap-fill complete: ${total} rows inserted in ${duration}s"
    exit 0
elif [ "$http_code" = "409" ]; then
    log "SKIPPED_LOCK_CONTENTION: Gap-fill already in progress, skipping"
    exit 0
elif [ "$http_code" = "404" ] || [ "$http_code" = "405" ] || [ "$http_code" = "000" ] || [ -z "$http_code" ]; then
    log "Gap-fill endpoint not available (HTTP ${http_code}): ${body:-no response body}"
    if run_gap_fill_cli_fallback "http_${http_code:-unknown}"; then
        exit 0
    fi
    exit 1
elif [ "$http_code" = "500" ]; then
    log "Gap-fill API error (HTTP 500): ${body:-no response body}"
    if echo "$body" | grep -qiE "Could not set lock|Conflicting lock"; then
        log "SKIPPED_LOCK_CONTENTION: Gap-fill API lock contention, skipping this cycle"
        exit 0
    fi
    if run_gap_fill_cli_fallback "http_500"; then
        exit 0
    fi
    exit 1
elif [ "$http_code" = "400" ]; then
    log "Gap-fill config error: $body"
    exit 1
else
    log "Gap-fill failed (HTTP ${http_code}): $body"
    exit 1
fi
