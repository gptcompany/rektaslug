#!/usr/bin/env python3
"""
Bridge ccxt-data-pipeline Parquet -> DuckDB gap fill (T-2 -> T-0).

After CSV ingestion (which has ~2 day latency), reads recent Parquet files
from ccxt-data-pipeline catalog and fills the gap up to T-0.

Handles: klines (5m OHLCV), open interest, funding rate.
Venue: BINANCE only (consistent with CSV pipeline).
Dedup: INSERT OR IGNORE for klines (PK exists), anti-join for OI/funding (no UNIQUE).

Usage:
    uv run python scripts/fill_gap_from_ccxt.py --symbols BTCUSDT ETHUSDT
    uv run python scripts/fill_gap_from_ccxt.py --symbols BTCUSDT --dry-run

This CLI script delegates to the shared module at
src/liquidationheatmap/ingestion/gap_fill.py which is also used by the
in-process API endpoint POST /api/v1/gap-fill.
"""

import argparse
import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("fill_gap")

DEFAULT_DB = "/media/sam/2TB-NVMe/liquidationheatmap_db/liquidations.duckdb"
DEFAULT_CATALOG = "/media/sam/1TB/ccxt-data-pipeline/data/catalog"
DEFAULT_SYMBOLS = ["BTCUSDT", "ETHUSDT"]

# Re-export individual functions for backward compatibility with tests
from src.liquidationheatmap.ingestion.gap_fill import (  # noqa: E402, F401
    fill_funding_rate,
    fill_klines,
    fill_open_interest,
    get_watermark,
    parquet_glob,
    run_gap_fill,
    validate_gaps,
)


def main():
    parser = argparse.ArgumentParser(description="Fill DuckDB gap from ccxt-data-pipeline Parquet")
    parser.add_argument("--symbols", nargs="+", default=DEFAULT_SYMBOLS, help="Symbols to process")
    parser.add_argument("--ccxt-catalog", default=DEFAULT_CATALOG, help="Path to ccxt catalog")
    parser.add_argument("--db", default=DEFAULT_DB, help="DuckDB database path")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Count available data without writing",
    )
    args = parser.parse_args()

    log.info("Gap fill: ccxt-data-pipeline Parquet -> DuckDB")
    log.info("  Catalog: %s", args.ccxt_catalog)
    log.info("  DB: %s", args.db)
    log.info("  Symbols: %s", args.symbols)
    log.info("  Dry run: %s", args.dry_run)

    try:
        result = run_gap_fill(
            db_path=args.db,
            catalog=args.ccxt_catalog,
            symbols=args.symbols,
            dry_run=args.dry_run,
        )
    except FileNotFoundError as e:
        log.error("%s", e)
        return 1

    # Print summary
    log.info("")
    log.info("=== Gap Fill Summary ===")
    for symbol, results in result["symbols"].items():
        for data_type, dtype_result in results.items():
            ins = dtype_result.get("inserted", 0)
            skip_reason = dtype_result.get("skipped", "")
            if skip_reason:
                log.info("  %s/%s: skipped (%s)", symbol, data_type, skip_reason)
            else:
                log.info("  %s/%s: %d inserted", symbol, data_type, ins)

    log.info("  Total rows inserted: %d", result["total_inserted"])
    return 0


if __name__ == "__main__":
    sys.exit(main())
