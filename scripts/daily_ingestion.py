#!/usr/bin/env python3
"""Daily data ingestion script - callable from n8n or cron.

Ingests all recent data per symbol:
- klines (5m, 15m)
- Open Interest
- Funding Rate

Usage:
    python scripts/daily_ingestion.py --days 7                    # All default symbols, last 7 days
    python scripts/daily_ingestion.py --symbol BTCUSDT            # Single symbol
    python scripts/daily_ingestion.py --symbol BTCUSDT --symbol ETHUSDT  # Multiple symbols
"""

import argparse
import logging
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.liquidationheatmap.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

_SETTINGS = get_settings()

# Configuration (centralized via src.liquidationheatmap.settings)
PROJECT_ROOT = _SETTINGS.project_root
DATA_DIR = _SETTINGS.data_dir
DB_PATH = _SETTINGS.db_path
DEFAULT_SYMBOLS = list(_SETTINGS.symbols)


def run_script(script_name: str, args: list) -> bool:
    """Run a Python script with arguments."""
    cmd = ["uv", "run", "python", str(PROJECT_ROOT / "scripts" / script_name)] + args
    logger.info(f"Running: {' '.join(cmd)}")

    try:
        result = subprocess.run(
            cmd,
            cwd=str(PROJECT_ROOT),
            capture_output=True,
            text=True,
            timeout=600,  # 10 min timeout
        )
        if result.returncode != 0:
            logger.error(f"Script failed: {result.stderr}")
            return False
        logger.info(f"Success: {script_name}")
        return True
    except subprocess.TimeoutExpired:
        logger.error(f"Timeout: {script_name}")
        return False
    except Exception as e:
        logger.error(f"Error running {script_name}: {e}")
        return False


def ingest_klines(start_date: str, end_date: str, interval: str, symbol: str) -> bool:
    """Ingest klines for a specific interval and symbol."""
    return run_script(
        "ingest_klines_15m.py",
        [
            "--symbol",
            symbol,
            "--start-date",
            start_date,
            "--end-date",
            end_date,
            "--interval",
            interval,
            "--data-dir",
            str(DATA_DIR),
        ],
    )


def ingest_oi(start_date: str, end_date: str, symbol: str) -> bool:
    """Ingest Open Interest data for a symbol."""
    return run_script(
        "ingest_oi.py",
        [
            "--symbol",
            symbol,
            "--start-date",
            start_date,
            "--end-date",
            end_date,
            "--data-dir",
            str(DATA_DIR),
        ],
    )


def main():
    parser = argparse.ArgumentParser(description="Daily data ingestion")
    parser.add_argument(
        "--days", type=int, default=3, help="Number of days to ingest (default: 3)"
    )
    parser.add_argument(
        "--symbol",
        action="append",
        dest="symbols",
        help="Symbol to ingest (repeatable, default: HEATMAP_SYMBOLS env or BTCUSDT,ETHUSDT)",
    )
    parser.add_argument("--klines-only", action="store_true", help="Only ingest klines data")
    args = parser.parse_args()

    symbols = args.symbols or DEFAULT_SYMBOLS

    end_date = datetime.now()
    start_date = end_date - timedelta(days=args.days)

    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    print(f"\n{'=' * 60}")
    print(f"Daily Ingestion: {start_str} to {end_str}")
    print(f"Symbols: {', '.join(symbols)}")
    print(f"{'=' * 60}\n")

    results = {}

    for symbol in symbols:
        print(f"\n--- {symbol} ---")

        # Ingest klines (5m and 15m) sequentially per symbol (DuckDB single-writer)
        print(f"  [1/3] Ingesting 5m klines for {symbol}...")
        results[f"{symbol}/klines_5m"] = ingest_klines(start_str, end_str, "5m", symbol)

        print(f"  [2/3] Ingesting 15m klines for {symbol}...")
        results[f"{symbol}/klines_15m"] = ingest_klines(start_str, end_str, "15m", symbol)

        if not args.klines_only:
            print(f"  [3/3] Ingesting Open Interest for {symbol}...")
            results[f"{symbol}/oi"] = ingest_oi(start_str, end_str, symbol)
        else:
            results[f"{symbol}/oi"] = "skipped"

    # Summary
    print(f"\n{'=' * 60}")
    print("SUMMARY")
    print(f"{'=' * 60}")
    for task, status in results.items():
        icon = "✅" if status is True else ("⏭️" if status == "skipped" else "❌")
        print(f"  {icon} {task}")

    # Return exit code
    failed = sum(1 for s in results.values() if s is False)
    if failed > 0:
        print(f"\n⚠️  {failed} task(s) failed")
        sys.exit(1)
    else:
        print("\n✅ All tasks completed successfully")
        sys.exit(0)


if __name__ == "__main__":
    main()
