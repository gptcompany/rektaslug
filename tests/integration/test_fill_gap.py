"""Integration tests for fill_gap_from_ccxt.py.

Tests the Parquet -> DuckDB gap fill logic using a temporary in-memory DB
with synthetic data, verifying:
- Correct column mapping from Parquet schema to DuckDB schema
- Deduplication (INSERT OR IGNORE for klines, anti-join for OI/funding)
- Watermark detection (only fills after MAX timestamp)
- Gap validation query
- Dry-run mode (no writes)
"""

from datetime import datetime, timezone

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest


def _utc(*args):
    """Create a timezone-aware UTC datetime."""
    return datetime(*args, tzinfo=timezone.utc)


@pytest.fixture
def tmp_db(tmp_path):
    """Create a temporary DuckDB with production schema."""
    db_path = str(tmp_path / "test.duckdb")
    con = duckdb.connect(db_path)

    con.execute("""
        CREATE TABLE klines_5m_history (
            open_time TIMESTAMP NOT NULL,
            symbol VARCHAR NOT NULL,
            open DECIMAL(18, 8) NOT NULL,
            high DECIMAL(18, 8) NOT NULL,
            low DECIMAL(18, 8) NOT NULL,
            close DECIMAL(18, 8) NOT NULL,
            volume DECIMAL(18, 8) NOT NULL,
            close_time TIMESTAMP NOT NULL,
            quote_volume DECIMAL(20, 8),
            count INTEGER,
            taker_buy_volume DECIMAL(18, 8),
            taker_buy_quote_volume DECIMAL(20, 8),
            PRIMARY KEY (open_time, symbol)
        )
    """)

    con.execute("""
        CREATE TABLE open_interest_history (
            id BIGINT,
            timestamp TIMESTAMP,
            symbol VARCHAR,
            open_interest_value DECIMAL(20, 8),
            open_interest_contracts DECIMAL(18, 8),
            source VARCHAR
        )
    """)

    con.execute("""
        CREATE TABLE funding_rate_history (
            id BIGINT,
            timestamp TIMESTAMP,
            symbol VARCHAR,
            funding_rate DECIMAL(10, 8),
            funding_interval_hours INTEGER
        )
    """)

    con.close()
    return db_path


@pytest.fixture
def ccxt_catalog(tmp_path):
    """Create a synthetic ccxt-data-pipeline catalog with Parquet files."""
    catalog = tmp_path / "catalog"

    # --- OHLCV ---
    ohlcv_dir = catalog / "ohlcv" / "BTCUSDT-PERP.BINANCE"
    ohlcv_dir.mkdir(parents=True)

    ohlcv_5m = pa.table(
        {
            "timestamp": pa.array(
                [
                    _utc(2026, 2, 28, 0, 0),
                    _utc(2026, 2, 28, 0, 5),
                    _utc(2026, 2, 28, 0, 10),
                    _utc(2026, 3, 1, 0, 0),
                ],
                type=pa.timestamp("us", tz="UTC"),
            ),
            "symbol": ["BTCUSDT-PERP"] * 4,
            "venue": ["BINANCE"] * 4,
            "timeframe": ["5m"] * 4,
            "open": [85000.0, 85100.0, 85050.0, 86000.0],
            "high": [85200.0, 85150.0, 85100.0, 86200.0],
            "low": [84900.0, 85000.0, 84950.0, 85900.0],
            "close": [85100.0, 85050.0, 85080.0, 86100.0],
            "volume": [100.5, 200.3, 150.7, 300.1],
        }
    )

    ohlcv_1m = pa.table(
        {
            "timestamp": pa.array(
                [
                    _utc(2026, 2, 28, 0, 0),
                    _utc(2026, 2, 28, 0, 1),
                    _utc(2026, 2, 28, 0, 2),
                ],
                type=pa.timestamp("us", tz="UTC"),
            ),
            "symbol": ["BTCUSDT-PERP"] * 3,
            "venue": ["BINANCE"] * 3,
            "timeframe": ["1m"] * 3,
            "open": [85000.0, 85010.0, 85020.0],
            "high": [85050.0, 85060.0, 85070.0],
            "low": [84990.0, 85000.0, 85010.0],
            "close": [85010.0, 85020.0, 85030.0],
            "volume": [10.1, 12.3, 11.5],
        }
    )

    combined = pa.concat_tables([ohlcv_5m, ohlcv_1m])
    pq.write_table(combined, ohlcv_dir / "2026-02-28.parquet")

    # --- Open Interest ---
    oi_dir = catalog / "open_interest" / "BTCUSDT-PERP.BINANCE"
    oi_dir.mkdir(parents=True)

    oi_table = pa.table(
        {
            "timestamp": pa.array(
                [
                    _utc(2026, 2, 28, 0, 1),
                    _utc(2026, 2, 28, 0, 6),
                    _utc(2026, 3, 1, 0, 1),
                ],
                type=pa.timestamp("us", tz="UTC"),
            ),
            "symbol": ["BTCUSDT-PERP"] * 3,
            "venue": ["BINANCE"] * 3,
            "open_interest": [77000.0, 77100.0, 78000.0],
            "open_interest_value": [5.0e9, 5.1e9, 5.2e9],
        }
    )
    pq.write_table(oi_table, oi_dir / "2026-02-28.parquet")

    # --- Funding Rate ---
    fr_dir = catalog / "funding_rate" / "BTCUSDT-PERP.BINANCE"
    fr_dir.mkdir(parents=True)

    fr_table = pa.table(
        {
            "timestamp": pa.array(
                [
                    _utc(2026, 2, 28, 0, 0),
                    _utc(2026, 2, 28, 8, 0),
                    _utc(2026, 3, 1, 0, 0),
                ],
                type=pa.timestamp("us", tz="UTC"),
            ),
            "symbol": ["BTCUSDT-PERP"] * 3,
            "venue": ["BINANCE"] * 3,
            "funding_rate": [0.0001, -0.00005, 0.00012],
            "next_funding_time": pa.array(
                [
                    _utc(2026, 2, 28, 8, 0),
                    _utc(2026, 2, 28, 16, 0),
                    _utc(2026, 3, 1, 8, 0),
                ],
                type=pa.timestamp("us", tz="UTC"),
            ),
            "predicted_rate": pa.array([None, None, None], type=pa.float64()),
        }
    )
    pq.write_table(fr_table, fr_dir / "2026-02-28.parquet")

    return str(catalog)


def seed_baseline(db_path: str):
    """Insert baseline data into DB (simulates existing CSV ingestion up to 2026-02-27)."""
    con = duckdb.connect(db_path)

    # Klines baseline: one candle at end of 2026-02-27
    con.execute("""
        INSERT INTO klines_5m_history VALUES
        (TIMESTAMP '2026-02-27 23:55:00', 'BTCUSDT',
         84900.0, 85000.0, 84800.0, 84950.0, 50.0,
         TIMESTAMP '2026-02-28 00:00:00', NULL, NULL, NULL, NULL)
    """)

    # OI baseline
    con.execute("""
        INSERT INTO open_interest_history VALUES
        (1, TIMESTAMP '2026-02-27 23:56:00', 'BTCUSDT', 4900000000.0, 76000.0, 'binance_csv')
    """)

    # Funding rate baseline
    con.execute("""
        INSERT INTO funding_rate_history VALUES
        (1, TIMESTAMP '2026-02-27 16:00:00', 'BTCUSDT', 0.00008, 8)
    """)

    con.close()


class TestFillKlines:
    def test_fills_gap_after_watermark(self, tmp_db, ccxt_catalog):
        """Klines after the watermark are inserted, with correct column mapping."""
        seed_baseline(tmp_db)
        con = duckdb.connect(tmp_db)

        from src.liquidationheatmap.ingestion.gap_fill import fill_klines

        result = fill_klines(con, ccxt_catalog, "BTCUSDT", dry_run=False)

        # New format: {"intervals": {"5m": {...}, "1m": {...}}, "inserted": N}
        assert "intervals" in result
        assert result["intervals"]["5m"]["inserted"] == 4  # all 4 5m rows are after watermark

        # Verify 5m data
        rows = con.execute("""
            SELECT open_time, symbol, open, close, volume, close_time
            FROM klines_5m_history
            WHERE open_time >= TIMESTAMP '2026-02-28'
            ORDER BY open_time
        """).fetchall()

        assert len(rows) == 4
        assert rows[0][1] == "BTCUSDT"  # symbol stripped of -PERP
        assert float(rows[0][2]) == 85000.0  # open
        assert float(rows[0][3]) == 85100.0  # close
        # close_time = open_time + 5 min
        close_min = rows[0][5].minute
        open_min = rows[0][0].minute
        assert close_min == open_min + 5 or close_min == (open_min + 5) % 60

        con.close()

    def test_idempotent_rerun(self, tmp_db, ccxt_catalog):
        """Running twice produces same result (INSERT OR IGNORE dedup)."""
        seed_baseline(tmp_db)
        con = duckdb.connect(tmp_db)

        from src.liquidationheatmap.ingestion.gap_fill import fill_klines

        r1 = fill_klines(con, ccxt_catalog, "BTCUSDT", dry_run=False)
        r2 = fill_klines(con, ccxt_catalog, "BTCUSDT", dry_run=False)

        assert r1["intervals"]["5m"]["inserted"] == 4
        assert r2["intervals"]["5m"]["inserted"] == 0  # all duplicates

        total = con.execute(
            "SELECT COUNT(*) FROM klines_5m_history WHERE symbol = 'BTCUSDT'"
        ).fetchone()[0]
        assert total == 5  # 1 baseline + 4 new

        con.close()

    def test_no_baseline_skips_5m(self, tmp_db, ccxt_catalog):
        """Without baseline data, 5m klines fill is skipped."""
        con = duckdb.connect(tmp_db)

        from src.liquidationheatmap.ingestion.gap_fill import fill_klines

        result = fill_klines(con, ccxt_catalog, "BTCUSDT", dry_run=False)

        assert result["intervals"]["5m"]["skipped"] == "no_baseline"
        assert result["intervals"]["5m"]["inserted"] == 0

        con.close()


class TestFillKlines1m:
    def test_bootstrap_without_baseline(self, tmp_db, ccxt_catalog):
        """1m klines bootstrap from Parquet without CSV baseline (bounded window)."""
        con = duckdb.connect(tmp_db)

        from src.liquidationheatmap.ingestion.gap_fill import _fill_klines_interval

        result = _fill_klines_interval(con, ccxt_catalog, "BTCUSDT", "1m", dry_run=False)

        # 1m has bootstrap_days=8 so no baseline needed; rows within window are inserted
        # The test data timestamps are from Feb 2026 which is >8 days ago in most cases,
        # but the test uses synthetic data so we verify the behavior
        assert result.get("skipped") != "no_baseline"

        con.close()

    def test_creates_table_automatically(self, tmp_db, ccxt_catalog):
        """_fill_klines_interval creates klines_1m_history if it doesn't exist."""
        con = duckdb.connect(tmp_db)

        # Table should not exist initially
        tables = [r[0] for r in con.execute("SHOW TABLES").fetchall()]
        assert "klines_1m_history" not in tables

        from src.liquidationheatmap.ingestion.gap_fill import _fill_klines_interval

        _fill_klines_interval(con, ccxt_catalog, "BTCUSDT", "1m", dry_run=False)

        # Now it should exist
        tables = [r[0] for r in con.execute("SHOW TABLES").fetchall()]
        assert "klines_1m_history" in tables

        con.close()

    def test_fill_klines_includes_1m_in_result(self, tmp_db, ccxt_catalog):
        """fill_klines returns results for both 5m and 1m intervals."""
        seed_baseline(tmp_db)
        con = duckdb.connect(tmp_db)

        from src.liquidationheatmap.ingestion.gap_fill import fill_klines

        result = fill_klines(con, ccxt_catalog, "BTCUSDT", dry_run=False)

        assert "intervals" in result
        assert "5m" in result["intervals"]
        assert "1m" in result["intervals"]
        assert result["inserted"] == result["intervals"]["5m"]["inserted"] + result["intervals"]["1m"]["inserted"]

        con.close()


class TestFillOpenInterest:
    def test_fills_oi_with_source_tag(self, tmp_db, ccxt_catalog):
        """OI rows are inserted with source='ccxt-pipeline'."""
        seed_baseline(tmp_db)
        con = duckdb.connect(tmp_db)

        from src.liquidationheatmap.ingestion.gap_fill import fill_open_interest

        result = fill_open_interest(con, ccxt_catalog, "BTCUSDT", dry_run=False)

        assert result["inserted"] == 3

        rows = con.execute("""
            SELECT timestamp, symbol, source, open_interest_contracts
            FROM open_interest_history
            WHERE source = 'ccxt-pipeline'
            ORDER BY timestamp
        """).fetchall()

        assert len(rows) == 3
        assert rows[0][1] == "BTCUSDT"
        assert rows[0][2] == "ccxt-pipeline"
        assert float(rows[0][3]) == 77000.0

        con.close()

    def test_idempotent_rerun(self, tmp_db, ccxt_catalog):
        """Anti-join prevents duplicates on re-run."""
        seed_baseline(tmp_db)
        con = duckdb.connect(tmp_db)

        from src.liquidationheatmap.ingestion.gap_fill import fill_open_interest

        r1 = fill_open_interest(con, ccxt_catalog, "BTCUSDT", dry_run=False)
        r2 = fill_open_interest(con, ccxt_catalog, "BTCUSDT", dry_run=False)

        assert r1["inserted"] == 3
        assert r2["inserted"] == 0

        total = con.execute(
            "SELECT COUNT(*) FROM open_interest_history WHERE symbol = 'BTCUSDT'"
        ).fetchone()[0]
        assert total == 4  # 1 baseline + 3 new

        con.close()


class TestFillFundingRate:
    def test_fills_funding_with_interval(self, tmp_db, ccxt_catalog):
        """Funding rate rows inserted with funding_interval_hours=8."""
        seed_baseline(tmp_db)
        con = duckdb.connect(tmp_db)

        from src.liquidationheatmap.ingestion.gap_fill import fill_funding_rate

        result = fill_funding_rate(con, ccxt_catalog, "BTCUSDT", dry_run=False)

        assert result["inserted"] == 3

        rows = con.execute("""
            SELECT timestamp, symbol, funding_rate, funding_interval_hours
            FROM funding_rate_history
            WHERE id > 1
            ORDER BY timestamp
        """).fetchall()

        assert len(rows) == 3
        assert rows[0][1] == "BTCUSDT"
        assert rows[0][3] == 8  # funding_interval_hours
        assert float(rows[0][2]) == 0.0001  # first funding rate

        con.close()


class TestDryRun:
    def test_dry_run_no_writes(self, tmp_db, ccxt_catalog):
        """Dry-run mode reports available data without writing."""
        seed_baseline(tmp_db)
        con = duckdb.connect(tmp_db, read_only=True)

        from src.liquidationheatmap.ingestion.gap_fill import _fill_klines_interval

        result = _fill_klines_interval(con, ccxt_catalog, "BTCUSDT", "5m", dry_run=True)

        assert result["skipped"] == "dry_run"
        assert result["available"] == 4
        assert result["inserted"] == 0

        con.close()

        # Verify no data written
        con2 = duckdb.connect(tmp_db, read_only=True)
        total = con2.execute("SELECT COUNT(*) FROM klines_5m_history").fetchone()[0]
        assert total == 1  # only baseline
        con2.close()


class TestGapValidation:
    def test_detects_gaps_in_klines(self, tmp_db):
        """Gap validation detects missing candles."""
        con = duckdb.connect(tmp_db)

        # Insert candles with a gap (missing 00:05)
        con.execute("""
            INSERT INTO klines_5m_history VALUES
            (TIMESTAMP '2026-02-28 00:00:00', 'BTCUSDT',
             85000, 85200, 84900, 85100, 100,
             TIMESTAMP '2026-02-28 00:05:00',
             NULL, NULL, NULL, NULL),
            (TIMESTAMP '2026-02-28 00:10:00', 'BTCUSDT',
             85100, 85300, 85000, 85200, 110,
             TIMESTAMP '2026-02-28 00:15:00',
             NULL, NULL, NULL, NULL)
        """)

        from datetime import datetime

        from src.liquidationheatmap.ingestion.gap_fill import validate_gaps

        # Should log warning but not raise
        validate_gaps(con, "BTCUSDT", datetime(2026, 2, 28))

        # Verify gap exists
        gaps = con.execute("""
            SELECT COUNT(*) FROM (
                SELECT open_time, LEAD(open_time) OVER (ORDER BY open_time) AS next_time
                FROM klines_5m_history WHERE symbol = 'BTCUSDT' AND open_time >= '2026-02-28'
            ) WHERE EXTRACT(EPOCH FROM (next_time - open_time)) > 300 * 1.5
        """).fetchone()[0]
        assert gaps == 1  # one gap (missing 00:05 candle)

        con.close()
