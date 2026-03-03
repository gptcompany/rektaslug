#!/usr/bin/env python3
"""Complete end-to-end aggTrades orchestrator - N8N PRODUCTION READY.

Features:
1. Creates database schema if not exists
2. Auto-detects and fills gaps
3. Full ingestion mode for initial bulk load
4. Plain text output (N8N compatible)
5. Idempotent and safe to re-run (INSERT OR IGNORE)

Modes:
- auto: Gap detection and filling (incremental updates)
- full: Load all files in date range (initial bulk ingestion)
- dry-run: Validation only, no data loaded
"""

import argparse
import logging
import sys
from pathlib import Path
from typing import List, Tuple

import duckdb

# Adjust path for imports
sys.path.insert(0, "/media/sam/1TB/rektaslug")

from src.liquidationheatmap.ingestion.aggtrades_streaming import load_aggtrades_streaming

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class CompleteIngestionOrchestrator:
    """Complete end-to-end orchestrator with schema creation."""

    def __init__(
        self,
        symbol: str,
        data_dir: Path,
        db_path: Path,
        throttle_ms: int = 200,
        start_date: str = None,
        end_date: str = None,
    ):
        self.symbol = symbol
        self.data_dir = data_dir
        self.db_path = db_path
        self.throttle_ms = throttle_ms
        self.start_date = start_date
        self.end_date = end_date
        self.conn = None

    def connect(self):
        """Open database connection."""
        self.conn = duckdb.connect(str(self.db_path))
        logger.info(f"Connected to database: {self.db_path}")

    def close(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()

    def ensure_schema(self):
        """Create table and indexes if they don't exist."""
        print("\n🔧 Phase 0: Schema Validation")

        try:
            # Check if table exists
            result = self.conn.execute("""
                SELECT COUNT(*) FROM information_schema.tables
                WHERE table_name = 'aggtrades_history'
            """).fetchone()

            if result[0] == 0:
                print("⚠️  Table does not exist - creating schema...")

                # Create table with COMPOSITE PRIMARY KEY (agg_trade_id, symbol, exchange)
                self.conn.execute("""
                    CREATE TABLE aggtrades_history (
                        agg_trade_id BIGINT NOT NULL,
                        timestamp TIMESTAMP NOT NULL,
                        symbol VARCHAR(20) NOT NULL,
                        exchange VARCHAR(20) NOT NULL DEFAULT 'binance',
                        price DECIMAL(18, 8) NOT NULL,
                        quantity DECIMAL(18, 8) NOT NULL,
                        side VARCHAR(4) NOT NULL,
                        gross_value DOUBLE NOT NULL,
                        PRIMARY KEY (agg_trade_id, symbol, exchange)
                    )
                """)
                print(
                    "✅ Table created with COMPOSITE PRIMARY KEY (agg_trade_id, symbol, exchange)"
                )

                # Create indexes
                self.conn.execute(
                    "CREATE INDEX idx_aggtrades_timestamp_symbol ON aggtrades_history(timestamp, symbol)"
                )
                self.conn.execute(
                    "CREATE INDEX idx_aggtrades_timestamp ON aggtrades_history(timestamp)"
                )
                self.conn.execute("CREATE INDEX idx_aggtrades_symbol ON aggtrades_history(symbol)")
                self.conn.execute(
                    "CREATE INDEX idx_aggtrades_exchange ON aggtrades_history(exchange)"
                )
                print("✅ Indexes created (4 indexes)")
            else:
                # Verify COMPOSITE PRIMARY KEY exists with symbol column
                pk_check = self.conn.execute("""
                    SELECT column_name
                    FROM information_schema.key_column_usage
                    WHERE table_name = 'aggtrades_history'
                """).fetchall()

                pk_columns = [row[0] for row in pk_check]
                if "symbol" in pk_columns and "exchange" in pk_columns:
                    print("✅ Schema validated (COMPOSITE PRIMARY KEY exists)")
                elif pk_columns:
                    print("⚠️  WARNING: Table has OLD PRIMARY KEY (agg_trade_id only)!")
                    print("   PK columns:", pk_columns)
                    print("   Delete database and re-run to fix schema!")
                    return False
                else:
                    print("⚠️  WARNING: Table exists but has NO PRIMARY KEY!")
                    return False

        except Exception as e:
            print(f"❌ Schema error: {e}")
            return False

        return True

    def discover_files(self) -> List[Path]:
        """Phase 1: Discover available CSV files."""
        print("\n📂 Phase 1: Discovery")

        aggtrades_dir = self.data_dir / self.symbol / "aggTrades"
        if not aggtrades_dir.exists():
            print(f"❌ Directory not found: {aggtrades_dir}")
            return []

        all_files = sorted(aggtrades_dir.glob(f"{self.symbol}-aggTrades-*.csv"))
        print(f"✅ Found {len(all_files):,} CSV files")

        return all_files

    def detect_gaps(self) -> List[Tuple[str, str]]:
        """Phase 2: Detect missing date ranges in database."""
        print("\n🔍 Phase 2: Gap Detection")

        try:
            result = self.conn.execute(f"""
                SELECT MIN(DATE(timestamp)), MAX(DATE(timestamp))
                FROM aggtrades_history
                WHERE symbol = '{self.symbol}'
            """).fetchone()

            if not result[0]:
                print("⚠️  Empty database - will ingest all available data")
                return []

            min_date, max_date = result

            gaps_query = f"""
                WITH RECURSIVE date_series AS (
                    SELECT DATE '{min_date}' as expected_date
                    UNION ALL
                    SELECT expected_date + INTERVAL 1 DAY
                    FROM date_series
                    WHERE expected_date < DATE '{max_date}'
                ),
                actual_dates AS (
                    SELECT DISTINCT DATE(timestamp) as actual_date
                    FROM aggtrades_history
                    WHERE symbol = '{self.symbol}'
                )
                SELECT d.expected_date
                FROM date_series d
                LEFT JOIN actual_dates a ON d.expected_date = a.actual_date
                WHERE a.actual_date IS NULL
                ORDER BY d.expected_date
            """

            missing_dates = [row[0] for row in self.conn.execute(gaps_query).fetchall()]

            if not missing_dates:
                print("✅ No gaps detected")
                return []

            gaps = self._consolidate_gaps(missing_dates)
            print(f"⚠️  Found {len(gaps)} gap(s):")
            for gap in gaps:
                start, end, days = gap[0], gap[1], gap[2]
                print(f"   • {start} → {end} ({days} days)")

            # Return only (start, end) tuples for compatibility with fill_gaps
            return [(g[0], g[1]) for g in gaps]

        except Exception as e:
            logger.error(f"Gap detection failed: {e}")
            return []

    def _consolidate_gaps(self, missing_dates: List) -> List[Tuple]:
        """Consolidate consecutive missing dates into ranges.

        Returns list of tuples: (start_date, end_date, days_count)
        Dates are Python date objects for calculation, converted to strings later.
        """
        if not missing_dates:
            return []

        from datetime import date

        # Ensure we have date objects
        def to_date(d):
            if isinstance(d, date):
                return d
            if isinstance(d, str):
                return date.fromisoformat(d)
            return d

        dates = [to_date(d) for d in missing_dates]

        gaps = []
        range_start = dates[0]
        prev_date = dates[0]

        for current_date in dates[1:]:
            if (current_date - prev_date).days > 1:
                days = (prev_date - range_start).days + 1
                gaps.append((str(range_start), str(prev_date), days))
                range_start = current_date
            prev_date = current_date

        # Don't forget the last gap
        days = (prev_date - range_start).days + 1
        gaps.append((str(range_start), str(prev_date), days))
        return gaps

    def fill_gaps(self, gaps: List[Tuple[str, str]], max_retries: int = 3) -> bool:
        """Phase 3: Fill detected gaps with retry logic."""
        if not gaps:
            return True

        print(f"\n🔧 Phase 3: Gap Filling ({len(gaps)} gap(s))")

        success_count = 0
        fail_count = 0

        for idx, (start_date, end_date) in enumerate(gaps, 1):
            print(f"\nGap {idx}/{len(gaps)}: {start_date} → {end_date}")

            for attempt in range(1, max_retries + 1):
                try:
                    total_rows = load_aggtrades_streaming(
                        self.conn,
                        self.data_dir,
                        self.symbol,
                        start_date,
                        end_date,
                        throttle_ms=self.throttle_ms,
                    )

                    print(f"✅ Filled: {total_rows:,} rows")
                    success_count += 1
                    break

                except Exception as e:
                    if attempt < max_retries:
                        print(f"⚠️  Attempt {attempt} failed: {e}")
                    else:
                        print(f"❌ Failed after {max_retries} attempts: {e}")
                        fail_count += 1

        print(f"\n📊 Gap filling: ✅ {success_count}/{len(gaps)} ❌ {fail_count}/{len(gaps)}")
        return fail_count == 0

    def generate_report(self) -> dict:
        """Phase 4: Generate final summary report."""
        print("\n📊 Phase 4: Final Report")

        stats = self.conn.execute(f"""
            SELECT
                COUNT(*) as total_rows,
                MIN(timestamp) as min_timestamp,
                MAX(timestamp) as max_timestamp,
                COUNT(DISTINCT DATE(timestamp)) as days_with_data,
                MIN(price) as min_price,
                MAX(price) as max_price,
                SUM(gross_value) as total_volume
            FROM aggtrades_history
            WHERE symbol = '{self.symbol}'
        """).fetchone()

        total_rows, min_ts, max_ts, days, min_price, max_price, volume = stats

        print("=" * 70)
        print("  INGESTION SUMMARY")
        print("=" * 70)
        print(f"Total Rows:     {total_rows:,}")
        print(f"Date Range:     {min_ts} → {max_ts}")
        print(f"Days with Data: {days:,}")
        print(f"Avg Rows/Day:   {total_rows // days:,}" if days > 0 else "Avg Rows/Day:   N/A")
        print(f"Price Range:    ${min_price:,.2f} - ${max_price:,.2f}")
        print(f"Total Volume:   ${volume:,.2f}")
        print("=" * 70)

        # Final gap check
        gaps = self.detect_gaps()
        if gaps:
            print(f"\n⚠️  WARNING: {len(gaps)} gap(s) remaining!")
            for start, end in gaps:
                print(f"   • {start} → {end}")
        else:
            print("\n✅ No gaps detected - data is continuous")

        return {"total_rows": total_rows, "gaps": len(gaps)}

    def run(self, mode: str = "auto") -> int:
        """Main orchestration workflow."""
        print("\n" + "=" * 70)
        print("  COMPLETE INGESTION ORCHESTRATOR (N8N-READY)")
        print("=" * 70)
        print(f"Symbol: {self.symbol}")
        print(f"Mode: {mode}")
        print(f"Throttle: {self.throttle_ms}ms")
        if self.start_date and self.end_date:
            print(f"Date Range: {self.start_date} → {self.end_date}")

        try:
            self.connect()

            # Phase 0: Ensure schema exists
            if not self.ensure_schema():
                print("\n❌ Schema validation failed")
                return 1

            # Phase 1: Discovery
            files = self.discover_files()
            if not files:
                print("\n⚠️  No files found")
                return 0

            if mode == "dry-run":
                print("\n✅ Dry run complete")
                return 0

            # MODE: FULL - Load all files in date range (initial bulk ingestion)
            if mode == "full":
                if not self.start_date or not self.end_date:
                    print("\n❌ --start-date and --end-date required for --mode full")
                    return 1

                print("\n🚀 Phase 2: Full Ingestion Mode")
                print(f"Loading ALL files from {self.start_date} to {self.end_date}")
                print("(Duplicates automatically ignored via INSERT OR IGNORE)")

                try:
                    total_rows = load_aggtrades_streaming(
                        self.conn,
                        self.data_dir,
                        self.symbol,
                        self.start_date,
                        self.end_date,
                        throttle_ms=self.throttle_ms,
                    )
                    print(f"✅ Loaded: {total_rows:,} rows")

                except Exception as e:
                    print(f"❌ Full ingestion failed: {e}")
                    logger.exception("Full ingestion failed")
                    return 1

                # Phase 4: Report
                self.generate_report()
                print("\n✅ Full ingestion complete!")
                return 0

            # MODE: AUTO - Gap detection and filling (incremental updates)
            # Phase 2-3: Detect and fill gaps
            gaps = self.detect_gaps()
            if gaps and mode == "auto":
                success = self.fill_gaps(gaps)
                if not success:
                    print("\n⚠️  Some gaps could not be filled")
                    # Don't return 1 - continue to report
            elif gaps and mode != "auto":
                print(f"\n⚠️  {len(gaps)} gaps detected but not filling (mode={mode})")

            # Phase 4: Report
            self.generate_report()

            print("\n✅ Orchestration complete!")
            return 0

        except Exception as e:
            print(f"\n❌ Fatal error: {e}")
            logger.exception("Orchestration failed")
            return 1

        finally:
            self.close()


def main():
    parser = argparse.ArgumentParser(
        description="Complete aggTrades orchestrator (N8N-ready)",
        epilog="""
Examples:
  # Gap filling (daily incremental updates)
  python3 ingest_full_history_n8n.py --data-dir /path/to/data --mode auto

  # Full ingestion (initial bulk load)
  python3 ingest_full_history_n8n.py --data-dir /path/to/data --mode full \\
    --start-date 2021-12-01 --end-date 2025-11-01

  # Dry run (validation only)
  python3 ingest_full_history_n8n.py --data-dir /path/to/data --mode dry-run
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--symbol", default="BTCUSDT", help="Trading pair (default: BTCUSDT)")
    parser.add_argument(
        "--data-dir", required=True, help="Base data directory containing CSV files"
    )
    parser.add_argument(
        "--db",
        default="/media/sam/2TB-NVMe/liquidationheatmap_db/liquidations.duckdb",
        help="DuckDB database path",
    )
    parser.add_argument(
        "--mode",
        default="auto",
        choices=["auto", "full", "dry-run"],
        help="Ingestion mode: auto (gaps), full (bulk load), dry-run (validate)",
    )
    parser.add_argument(
        "--start-date", help="Start date for full mode (YYYY-MM-DD, e.g., 2021-12-01)"
    )
    parser.add_argument("--end-date", help="End date for full mode (YYYY-MM-DD, e.g., 2025-11-01)")
    parser.add_argument(
        "--throttle-ms",
        type=int,
        default=200,
        help="I/O throttle between files in milliseconds (default: 200ms)",
    )

    args = parser.parse_args()

    # Validate: full mode requires date range
    if args.mode == "full" and (not args.start_date or not args.end_date):
        parser.error("--mode full requires both --start-date and --end-date")

    orchestrator = CompleteIngestionOrchestrator(
        symbol=args.symbol,
        data_dir=Path(args.data_dir),
        db_path=Path(args.db),
        throttle_ms=args.throttle_ms,
        start_date=args.start_date,
        end_date=args.end_date,
    )

    sys.exit(orchestrator.run(mode=args.mode))


if __name__ == "__main__":
    main()
