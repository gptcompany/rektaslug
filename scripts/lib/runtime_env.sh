#!/bin/sh
# Shared runtime environment loader for host and container shell wrappers.

lh_load_env_value() {
    key="$1"
    value="$2"

    case "$key" in
        HEATMAP_HOST|HEATMAP_PORT|HEATMAP_API_URL|HEATMAP_DB_PATH|HEATMAP_DATA_DIR|HEATMAP_PROJECT_ROOT|HEATMAP_CCXT_CATALOG|HEATMAP_SHARED_ENV_FILE|HEATMAP_SYMBOLS|HEATMAP_SYMBOLS_SHELL|HEATMAP_LOG_DIR|HEATMAP_CONTAINER_PROJECT_ROOT|HEATMAP_CONTAINER_DB_PATH|HEATMAP_CONTAINER_DATA_DIR|HEATMAP_CONTAINER_PORT|HEATMAP_CONTAINER_API_URL|API_PORT)
            case "$value" in
                \"*\")
                    value=${value#\"}
                    value=${value%\"}
                    ;;
                \'*\')
                    value=${value#\'}
                    value=${value%\'}
                    ;;
            esac
            eval "$key=\$value"
            ;;
    esac
}

lh_load_env_file() {
    env_file="$1"

    [ -f "$env_file" ] || return 0

    while IFS= read -r raw_line || [ -n "$raw_line" ]; do
        line=${raw_line#${raw_line%%[![:space:]]*}}
        case "$line" in
            ''|\#*)
                continue
                ;;
        esac

        key=${line%%=*}
        value=${line#*=}

        case "$key" in
            *[!A-Za-z0-9_]*|'')
                continue
                ;;
        esac

        lh_load_env_value "$key" "$value"
    done < "$env_file"
}

lh_load_runtime_env() {
    profile="${1:-host}"

    if [ -n "${PROJECT_DIR:-}" ]; then
        base_project_dir="$PROJECT_DIR"
    elif [ "$profile" = "container" ]; then
        base_project_dir="${HEATMAP_CONTAINER_PROJECT_ROOT:-/workspace/1TB/rektaslug}"
    else
        base_project_dir="${HEATMAP_PROJECT_ROOT:-/media/sam/1TB/rektaslug}"
    fi

    env_file="${HEATMAP_ENV_FILE:-${base_project_dir}/.env}"
    lh_load_env_file "$env_file"

    if [ "$profile" = "container" ]; then
        PROJECT_DIR="${HEATMAP_CONTAINER_PROJECT_ROOT:-$base_project_dir}"
        DB_PATH="${HEATMAP_CONTAINER_DB_PATH:-/workspace/2TB-NVMe/liquidationheatmap_db/liquidations.duckdb}"
        DATA_DIR="${HEATMAP_CONTAINER_DATA_DIR:-/workspace/3TB-WDC/binance-history-data-downloader/data}"
        API_URL="${HEATMAP_CONTAINER_API_URL:-http://host.docker.internal:${HEATMAP_CONTAINER_PORT:-${API_PORT:-8000}}}"
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
