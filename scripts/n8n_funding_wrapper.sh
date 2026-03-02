#!/bin/sh
# N8N wrapper script for Funding Rate DuckDB ingest
# Usage: ./n8n_funding_wrapper.sh START_MONTH END_MONTH
# Example: ./n8n_funding_wrapper.sh 2020-01 2025-11
# Handles lock cleanup and API coordination
# Ingests Funding Rates for BTCUSDT and ETHUSDT

START_MONTH="$1"
END_MONTH="$2"

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
PROJECT_DIR=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)
. "$PROJECT_DIR/scripts/lib/runtime_env.sh"
lh_load_runtime_env container

cd "$PROJECT_DIR"

echo ""
echo "=========================================="
echo "Funding Rate Ingestion Wrapper"
echo "=========================================="
echo "Month range: $START_MONTH -> $END_MONTH"
echo ""

# Step 1: Notify API to release connections
echo "[Step 1/4] Preparing database for ingestion..."
PREP_RESULT=$(wget -q -O - --timeout=10 --post-data="" "${API_URL}/api/v1/prepare-for-ingestion" 2>/dev/null || echo '{"status":"api_unavailable"}')
echo "API preparation: ${PREP_RESULT}"

# Step 2: Clean any stale locks
echo ""
echo "[Step 2/4] Cleaning stale locks..."
python3 "${PROJECT_DIR}/scripts/cleanup_duckdb_locks.py" "${DB_PATH}" || true

# Step 3: Wait for database to be available
echo ""
echo "[Step 3/4] Checking database availability..."
sleep 2

MAX_RETRIES=3
RETRY_DELAY=5

test_db_write_access() {
    python3 -c "
import duckdb
import sys
try:
    conn = duckdb.connect('${DB_PATH}', read_only=False)
    conn.close()
    sys.exit(0)
except Exception as e:
    print(f'Lock test failed: {e}', file=sys.stderr)
    sys.exit(1)
" 2>/dev/null
    return $?
}

attempt=1
while [ $attempt -le $MAX_RETRIES ]; do
    if test_db_write_access; then
        echo "Database write access confirmed"
        break
    else
        if [ $attempt -eq $MAX_RETRIES ]; then
            echo "ERROR: Cannot acquire database write lock after $MAX_RETRIES attempts"
            ps aux | grep -E "python.*duckdb|ingest" | grep -v grep || true
            exit 1
        fi
        echo "Database locked, waiting ${RETRY_DELAY}s (attempt $attempt/$MAX_RETRIES)..."
        sleep $RETRY_DELAY
        attempt=$((attempt + 1))
    fi
done

echo ""
echo "[Step 4/4] Running Funding Rate ingestion..."

# Track status
FR_BTC=0
FR_ETH=0

echo ""
echo "[1/2] Ingesting BTCUSDT Funding Rates..."
python3 scripts/ingest_funding_rate.py \
    --symbol BTCUSDT \
    --start-month "$START_MONTH" \
    --end-month "$END_MONTH" \
    --data-dir "$DATA_DIR" \
    --db "$DB_PATH" \
    --throttle-ms 100
FR_BTC=$?

echo ""
echo "[2/2] Ingesting ETHUSDT Funding Rates..."
python3 scripts/ingest_funding_rate.py \
    --symbol ETHUSDT \
    --start-month "$START_MONTH" \
    --end-month "$END_MONTH" \
    --data-dir "$DATA_DIR" \
    --db "$DB_PATH" \
    --throttle-ms 100
FR_ETH=$?

# Notify API to refresh connections
echo ""
echo "Refreshing API connections..."
REFRESH_RESULT=$(wget -q -O - --timeout=10 --post-data="" "${API_URL}/api/v1/refresh-connections" 2>/dev/null || echo '{"status":"api_unavailable"}')
echo "API refresh: ${REFRESH_RESULT}"

echo ""
echo "=========================================="
echo "Funding Rate Ingestion Summary"
echo "=========================================="
FAILED=0

if [ $FR_BTC -eq 0 ]; then
    echo "BTCUSDT Funding: Success"
else
    echo "BTCUSDT Funding: Failed"
    FAILED=$((FAILED + 1))
fi

if [ $FR_ETH -eq 0 ]; then
    echo "ETHUSDT Funding: Success"
else
    echo "ETHUSDT Funding: Failed"
    FAILED=$((FAILED + 1))
fi

if [ $FAILED -gt 0 ]; then
    echo ""
    echo "$FAILED task(s) failed"
    exit 1
fi

echo ""
echo "Funding Rate ingestion complete!"
