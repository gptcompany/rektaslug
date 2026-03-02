#!/bin/sh
# Shared runtime environment loader for host and container shell wrappers.

lh_load_runtime_env() {
    profile="${1:-host}"

    if [ -n "${PROJECT_DIR:-}" ]; then
        base_project_dir="$PROJECT_DIR"
    elif [ "$profile" = "container" ]; then
        base_project_dir="${HEATMAP_CONTAINER_PROJECT_ROOT:-/workspace/1TB/LiquidationHeatmap}"
    else
        base_project_dir="${HEATMAP_PROJECT_ROOT:-/media/sam/1TB/LiquidationHeatmap}"
    fi

    env_file="${HEATMAP_ENV_FILE:-${base_project_dir}/.env}"
    if [ -f "$env_file" ]; then
        set -a
        # shellcheck disable=SC1090
        . "$env_file"
        set +a
    fi

    if [ "$profile" = "container" ]; then
        PROJECT_DIR="${HEATMAP_CONTAINER_PROJECT_ROOT:-$base_project_dir}"
        DB_PATH="${HEATMAP_CONTAINER_DB_PATH:-/workspace/2TB-NVMe/liquidationheatmap_db/liquidations.duckdb}"
        DATA_DIR="${HEATMAP_CONTAINER_DATA_DIR:-/workspace/3TB-WDC/binance-history-data-downloader/data}"
        API_URL="${HEATMAP_CONTAINER_API_URL:-http://host.docker.internal:${HEATMAP_PORT:-8002}}"
    else
        PROJECT_DIR="${HEATMAP_PROJECT_ROOT:-$base_project_dir}"
        DB_PATH="${HEATMAP_DB_PATH:-/media/sam/2TB-NVMe/liquidationheatmap_db/liquidations.duckdb}"
        DATA_DIR="${HEATMAP_DATA_DIR:-/media/sam/3TB-WDC/binance-history-data-downloader/data}"
        API_URL="${HEATMAP_API_URL:-http://127.0.0.1:${HEATMAP_PORT:-8002}}"
    fi

    CCXT_CATALOG="${HEATMAP_CCXT_CATALOG:-/media/sam/1TB/ccxt-data-pipeline/data/catalog}"
    SHARED_ENV_FILE="${HEATMAP_SHARED_ENV_FILE:-/media/sam/1TB/.env}"
    LOG_DIR="${HEATMAP_LOG_DIR:-${PROJECT_DIR}/logs/ingestion}"
    SYMBOLS="${HEATMAP_SYMBOLS_SHELL:-BTCUSDT ETHUSDT}"

    export PROJECT_DIR DB_PATH DATA_DIR API_URL CCXT_CATALOG SHARED_ENV_FILE LOG_DIR SYMBOLS
}
