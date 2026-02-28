#!/usr/bin/env python3
"""Ingest Hyperliquid L4 fills from zstd-compressed JSONL into DuckDB.

Usage:
    uv run python scripts/ingest_hl_fills.py --date 2026-02-27
    uv run python scripts/ingest_hl_fills.py --backfill
    uv run python scripts/ingest_hl_fills.py --date 2026-02-27 --coins BTC,ETH --crossed-only

Data source:
    /media/sam/4TB-NVMe/hyperliquid/filtered/node_fills_by_block/hourly/YYYYMMDD/*.zst
"""

import argparse
import io
import json
import logging
import sys
import time
from pathlib import Path

import duckdb
import pandas as pd
import zstandard as zstd

logger = logging.getLogger(__name__)

# Default paths
DEFAULT_DATA_ROOT = Path("/media/sam/4TB-NVMe/hyperliquid/filtered/node_fills_by_block/hourly")
DEFAULT_DB_PATH = Path("/media/sam/2TB-NVMe/liquidationheatmap_db/liquidations.duckdb")

# Required fields in a fill object
REQUIRED_FILL_FIELDS = {"coin", "px", "sz", "side", "dir", "time", "tid"}


def create_hl_schema(conn: duckdb.DuckDBPyConnection) -> None:
    """Create hl_fills_l4 table and hl_liquidations_l4 view."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS hl_fills_l4 (
            block_time TIMESTAMP NOT NULL,
            block_number BIGINT NOT NULL,
            event_time_ms BIGINT,
            coin VARCHAR NOT NULL,
            price DOUBLE NOT NULL,
            size DOUBLE NOT NULL,
            side VARCHAR(1) NOT NULL,
            direction VARCHAR(20) NOT NULL,
            closed_pnl DOUBLE,
            start_position DOUBLE,
            crossed BOOLEAN,
            fee DOUBLE,
            wallet VARCHAR,
            tx_hash VARCHAR,
            order_id BIGINT,
            trade_id BIGINT,
            source_file VARCHAR,
            ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(block_number, trade_id)
        )
    """)

    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_hl_fills_l4_coin_time
        ON hl_fills_l4(coin, block_time)
    """)

    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_hl_fills_l4_liq_filter
        ON hl_fills_l4(direction, crossed, closed_pnl)
    """)

    conn.execute("""
        CREATE OR REPLACE VIEW hl_liquidations_l4 AS
        SELECT *
        FROM hl_fills_l4
        WHERE direction IN ('Close Long', 'Close Short')
          AND closed_pnl < 0
          AND crossed = true
    """)


def extract_fills_from_block(
    block: dict,
    coins: list[str] | None = None,
) -> list[dict]:
    """Extract fill records from a single JSONL block.

    Args:
        block: Parsed JSON block with 'events', 'block_time', 'block_number'.
        coins: Optional list of coins to filter (e.g. ['BTC', 'ETH']).
               None means accept all coins.

    Returns:
        List of flat dicts ready for DB insertion.
    """
    events = block.get("events", [])
    if not events:
        return []

    block_time = block.get("block_time", "")
    block_number = block.get("block_number", 0)
    fills = []

    for event in events:
        # Each event is [wallet_address, fill_dict]
        if not isinstance(event, (list, tuple)) or len(event) < 2:
            continue

        wallet = event[0]
        fill = event[1]

        if not isinstance(fill, dict):
            continue

        # Check required fields
        if not REQUIRED_FILL_FIELDS.issubset(fill.keys()):
            continue

        coin = fill["coin"]

        # Coin filter
        if coins is not None and coin not in coins:
            continue

        # Parse numeric fields - skip on conversion error
        try:
            price = float(fill["px"])
            size = float(fill["sz"])
            closed_pnl = float(fill.get("closedPnl", 0.0))
            start_position = float(fill.get("startPosition", 0.0))
            fee = float(fill.get("fee", 0.0))
        except (ValueError, TypeError):
            continue

        fills.append(
            {
                "block_time": block_time,
                "block_number": block_number,
                "event_time_ms": fill.get("time"),
                "coin": coin,
                "price": price,
                "size": size,
                "side": fill["side"],
                "direction": fill["dir"],
                "closed_pnl": closed_pnl,
                "start_position": start_position,
                "crossed": fill.get("crossed"),
                "fee": fee,
                "wallet": wallet,
                "tx_hash": fill.get("hash"),
                "order_id": fill.get("oid"),
                "trade_id": fill["tid"],
            }
        )

    return fills


def parse_zst_file(
    zst_path: Path,
    coins: list[str] | None = None,
) -> list[dict]:
    """Decompress and parse a .zst JSONL file, yielding fill dicts.

    Uses streaming decompression to handle large files efficiently.

    Args:
        zst_path: Path to the .zst compressed file.
        coins: Optional coin filter.

    Returns:
        List of fill dicts with 'source_file' added.
    """
    dctx = zstd.ZstdDecompressor()
    source_name = zst_path.name
    fills = []
    parse_errors = 0

    with open(zst_path, "rb") as fh:
        with dctx.stream_reader(fh) as reader:
            text_stream = io.TextIOWrapper(reader, encoding="utf-8")
            for line in text_stream:
                line = line.strip()
                if not line:
                    continue
                try:
                    block = json.loads(line)
                except json.JSONDecodeError:
                    parse_errors += 1
                    continue

                block_fills = extract_fills_from_block(block, coins=coins)
                for f in block_fills:
                    f["source_file"] = source_name
                fills.extend(block_fills)

    if parse_errors > 0:
        logger.warning(f"{zst_path.name}: {parse_errors} JSON parse errors skipped")

    return fills


def ingest_date(
    conn: duckdb.DuckDBPyConnection,
    day_dir: Path,
    coins: list[str] | None = None,
    pnl_threshold: float = 0,
    crossed_only: bool = True,
) -> dict:
    """Ingest all .zst files from a day directory into hl_fills_l4.

    Args:
        conn: DuckDB connection (schema must exist).
        day_dir: Path to YYYYMMDD directory containing .zst files.
        coins: Optional coin filter.
        pnl_threshold: Not used for filtering at ingestion (all fills stored).
        crossed_only: Not used for filtering at ingestion (all fills stored).

    Returns:
        Dict with ingestion stats.
    """
    stats = {
        "files_processed": 0,
        "lines_parsed": 0,
        "fills_extracted": 0,
        "inserted": 0,
        "duplicates_skipped": 0,
        "errors": 0,
    }

    zst_files = sorted(day_dir.glob("*.zst"))
    if not zst_files:
        logger.info(f"No .zst files in {day_dir}")
        return stats

    columns = [
        "block_time",
        "block_number",
        "event_time_ms",
        "coin",
        "price",
        "size",
        "side",
        "direction",
        "closed_pnl",
        "start_position",
        "crossed",
        "fee",
        "wallet",
        "tx_hash",
        "order_id",
        "trade_id",
        "source_file",
    ]

    for zst_path in zst_files:
        try:
            fills = parse_zst_file(zst_path, coins=coins)
        except Exception as e:
            logger.error(f"Failed to parse {zst_path}: {e}")
            stats["errors"] += 1
            continue

        stats["files_processed"] += 1
        stats["fills_extracted"] += len(fills)

        if not fills:
            continue

        # Batch insert via DataFrame (orders of magnitude faster than executemany)
        count_before = conn.execute("SELECT COUNT(*) FROM hl_fills_l4").fetchone()[0]

        df = pd.DataFrame(fills, columns=columns)  # noqa: F841 - used by DuckDB SQL below
        conn.execute("""
            INSERT OR IGNORE INTO hl_fills_l4 (
                block_time, block_number, event_time_ms, coin, price, size,
                side, direction, closed_pnl, start_position, crossed,
                fee, wallet, tx_hash, order_id, trade_id, source_file
            ) SELECT
                block_time, block_number, event_time_ms, coin, price, size,
                side, direction, closed_pnl, start_position, crossed,
                fee, wallet, tx_hash, order_id, trade_id, source_file
            FROM df
        """)

        count_after = conn.execute("SELECT COUNT(*) FROM hl_fills_l4").fetchone()[0]
        newly_inserted = count_after - count_before
        stats["inserted"] += newly_inserted
        stats["duplicates_skipped"] += len(fills) - newly_inserted

    return stats


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(description="Ingest Hyperliquid L4 fills into DuckDB")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--date",
        type=str,
        help="Single date to ingest (YYYY-MM-DD)",
    )
    group.add_argument(
        "--backfill",
        action="store_true",
        help="Ingest all available dates",
    )
    parser.add_argument(
        "--coins",
        type=str,
        default="BTC,ETH",
        help="Comma-separated coin filter (default: BTC,ETH)",
    )
    parser.add_argument(
        "--data-root",
        type=Path,
        default=DEFAULT_DATA_ROOT,
        help="Root directory for hourly .zst files",
    )
    parser.add_argument(
        "--db-path",
        type=Path,
        default=DEFAULT_DB_PATH,
        help="DuckDB database path",
    )
    parser.add_argument(
        "--pnl-threshold",
        type=float,
        default=0,
        help="PnL threshold for liquidation heuristic (default: 0)",
    )
    parser.add_argument(
        "--crossed-only",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Require crossed=true for liquidation heuristic (default: true)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    """Main entry point."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    args = parse_args(argv)
    coins = [c.strip() for c in args.coins.split(",")]

    # Resolve date directories
    data_root = args.data_root
    if not data_root.exists():
        logger.error(f"Data root not found: {data_root}")
        sys.exit(1)

    if args.backfill:
        day_dirs = sorted(d for d in data_root.iterdir() if d.is_dir() and d.name.isdigit())
    else:
        date_str = args.date.replace("-", "")
        day_dir = data_root / date_str
        if not day_dir.exists():
            logger.error(f"Date directory not found: {day_dir}")
            sys.exit(1)
        day_dirs = [day_dir]

    # Connect to DuckDB
    conn = duckdb.connect(str(args.db_path))
    create_hl_schema(conn)

    total_stats = {
        "files_processed": 0,
        "lines_parsed": 0,
        "fills_extracted": 0,
        "inserted": 0,
        "duplicates_skipped": 0,
        "errors": 0,
    }

    t0 = time.time()

    for day_dir in day_dirs:
        logger.info(f"Processing {day_dir.name}...")
        day_stats = ingest_date(
            conn=conn,
            day_dir=day_dir,
            coins=coins,
            pnl_threshold=args.pnl_threshold,
            crossed_only=args.crossed_only,
        )

        for k in total_stats:
            total_stats[k] += day_stats.get(k, 0)

        logger.info(
            f"  {day_dir.name}: "
            f"{day_stats['inserted']} inserted, "
            f"{day_stats['duplicates_skipped']} duplicates, "
            f"{day_stats['errors']} errors"
        )

    elapsed = time.time() - t0
    conn.close()

    # Summary
    print("\n" + "=" * 60)
    print("INGESTION SUMMARY")
    print("=" * 60)
    print(f"  Days processed:       {len(day_dirs)}")
    print(f"  Files processed:      {total_stats['files_processed']}")
    print(f"  Fills extracted:      {total_stats['fills_extracted']}")
    print(f"  Rows inserted:        {total_stats['inserted']}")
    print(f"  Duplicates skipped:   {total_stats['duplicates_skipped']}")
    print(f"  Errors:               {total_stats['errors']}")
    print(f"  Elapsed:              {elapsed:.1f}s")
    if elapsed > 0 and total_stats["inserted"] > 0:
        print(f"  Rate:                 {total_stats['inserted'] / elapsed:.0f} rows/sec")
    print("=" * 60)


if __name__ == "__main__":
    main()
