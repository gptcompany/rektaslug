#!/usr/bin/env python3
"""Ingest Binance Futures metrics data (long/short ratios, taker volume).

Metrics contain critical signals for liquidation heatmap calibration:
- count_toptrader_long_short_ratio: Top trader sentiment
- count_long_short_ratio: Account-based long/short ratio
- sum_taker_long_short_vol_ratio: Aggressive buy/sell ratio

CSV columns:
  create_time, symbol, sum_open_interest, sum_open_interest_value,
  count_toptrader_long_short_ratio, sum_toptrader_long_short_ratio,
  count_long_short_ratio, sum_taker_long_short_vol_ratio

Usage:
    python scripts/ingest_metrics.py --symbol BTCUSDT --start-date 2020-01-01 --end-date 2026-02-28 \
        --data-dir /media/sam/3TB-WDC/binance-history-data-downloader/data
"""

import argparse
import logging
import time
from datetime import datetime, timedelta
from pathlib import Path

import duckdb
from rich.console import Console

console = Console()
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

DB_PATH = "/media/sam/2TB-NVMe/liquidationheatmap_db/liquidations.duckdb"


def ensure_metrics_table(conn: duckdb.DuckDBPyConnection):
    """Create metrics_history table if not exists."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS metrics_history (
            id INTEGER PRIMARY KEY DEFAULT(nextval('metrics_seq')),
            timestamp TIMESTAMP NOT NULL,
            symbol VARCHAR NOT NULL,
            count_toptrader_long_short_ratio DOUBLE,
            sum_toptrader_long_short_ratio DOUBLE,
            count_long_short_ratio DOUBLE,
            sum_taker_long_short_vol_ratio DOUBLE,
            source VARCHAR DEFAULT 'binance_csv'
        )
    """)
    conn.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_metrics_ts_sym
        ON metrics_history(timestamp, symbol)
    """)
    logger.info("Table metrics_history ensured")


def get_metrics_files(data_dir: Path, symbol: str, start_date: str, end_date: str) -> list[Path]:
    """Get metrics CSV files within date range."""
    metrics_dir = Path(data_dir) / symbol / "metrics"
    if not metrics_dir.exists():
        raise FileNotFoundError(f"Metrics directory not found: {metrics_dir}")

    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")

    files = []
    current = start_dt
    while current <= end_dt:
        date_str = current.strftime("%Y-%m-%d")
        file_path = metrics_dir / f"{symbol}-metrics-{date_str}.csv"
        if file_path.exists():
            files.append(file_path)
        current += timedelta(days=1)

    return sorted(files)


def ingest_file(conn: duckdb.DuckDBPyConnection, file_path: Path, symbol: str) -> tuple[int, int]:
    """Ingest a single metrics CSV file. Returns (inserted, skipped)."""
    try:
        # Read CSV into temporary table
        conn.execute(f"""
            CREATE OR REPLACE TEMPORARY TABLE tmp_metrics AS
            SELECT
                CAST(create_time AS TIMESTAMP) as timestamp,
                symbol,
                CAST(count_toptrader_long_short_ratio AS DOUBLE) as count_toptrader_long_short_ratio,
                CAST(sum_toptrader_long_short_ratio AS DOUBLE) as sum_toptrader_long_short_ratio,
                CAST(count_long_short_ratio AS DOUBLE) as count_long_short_ratio,
                CAST(sum_taker_long_short_vol_ratio AS DOUBLE) as sum_taker_long_short_vol_ratio
            FROM read_csv('{file_path}', header=true, auto_detect=true)
            WHERE symbol = '{symbol}'
        """)

        total = conn.execute("SELECT COUNT(*) FROM tmp_metrics").fetchone()[0]

        # Count before insert
        before = conn.execute("SELECT COUNT(*) FROM metrics_history").fetchone()[0]

        # Insert with conflict handling (skip duplicates)
        conn.execute("""
            INSERT OR IGNORE INTO metrics_history
                (timestamp, symbol, count_toptrader_long_short_ratio,
                 sum_toptrader_long_short_ratio, count_long_short_ratio,
                 sum_taker_long_short_vol_ratio)
            SELECT timestamp, symbol, count_toptrader_long_short_ratio,
                   sum_toptrader_long_short_ratio, count_long_short_ratio,
                   sum_taker_long_short_vol_ratio
            FROM tmp_metrics
        """)

        after = conn.execute("SELECT COUNT(*) FROM metrics_history").fetchone()[0]
        inserted = after - before
        skipped = total - inserted

        return inserted, skipped

    except Exception as e:
        logger.error(f"Error ingesting {file_path.name}: {e}")
        return 0, 0


def main():
    parser = argparse.ArgumentParser(description="Ingest Binance metrics CSV")
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--data-dir", required=True)
    parser.add_argument("--db", default=DB_PATH)
    args = parser.parse_args()

    console.print(f"\nMetrics Streaming Ingestion")
    console.print(f"Symbol: {args.symbol}")
    console.print(f"Date range: {args.start_date} to {args.end_date}")
    console.print(f"Database: {args.db}\n")

    files = get_metrics_files(Path(args.data_dir), args.symbol, args.start_date, args.end_date)
    if not files:
        console.print("[red]No metrics files found in range[/red]")
        return

    console.print(f"Found {len(files)} CSV files to process\n")

    conn = duckdb.connect(args.db)

    # Create sequence if not exists
    try:
        conn.execute("CREATE SEQUENCE IF NOT EXISTS metrics_seq START 1")
    except Exception:
        pass

    ensure_metrics_table(conn)

    total_inserted = 0
    total_skipped = 0
    failed = 0

    for i, f in enumerate(files, 1):
        inserted, skipped = ingest_file(conn, f, args.symbol)
        total_inserted += inserted
        total_skipped += skipped
        if inserted == 0 and skipped == 0:
            failed += 1
        logger.info(f"[{i}/{len(files)}] {f.name}: {inserted} inserted, {skipped} skipped")
        time.sleep(0.05)  # Minimal throttle

    logger.info(f"\n✅ Completed: {len(files)} files processed, {failed} failed")
    logger.info(f"📊 Total rows inserted: {total_inserted:,}, skipped: {total_skipped:,}")

    # Stats
    stats = conn.execute(f"""
        SELECT COUNT(*), MIN(timestamp), MAX(timestamp)
        FROM metrics_history WHERE symbol='{args.symbol}'
    """).fetchone()
    console.print(f"\n✅ Complete! Inserted {total_inserted:,} rows")
    console.print(f"\nDatabase stats:")
    console.print(f"  Total rows: {stats[0]:,}")
    console.print(f"  Date range: {stats[1]} → {stats[2]}")

    conn.close()


if __name__ == "__main__":
    main()
