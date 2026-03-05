"""End-to-end test for run_gap_fill() with a temporary DuckDB and Parquet catalog.

Verifies that klines_5m, klines_1m, OI, and funding are all filled, and
checks freshness (max timestamp within expected window).
"""

from datetime import datetime, timezone

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from src.liquidationheatmap.ingestion.gap_fill import run_gap_fill


def _utc(*args):
    return datetime(*args, tzinfo=timezone.utc)


@pytest.fixture
def e2e_catalog(tmp_path):
    """Create a complete ccxt-data-pipeline catalog with 5m, 1m, OI, funding."""
    catalog = tmp_path / "catalog"

    # --- OHLCV (5m + 1m) ---
    ohlcv_dir = catalog / "ohlcv" / "BTCUSDT-PERP.BINANCE"
    ohlcv_dir.mkdir(parents=True)

    ohlcv_5m = pa.table({
        "timestamp": pa.array([
            _utc(2026, 3, 1, 0, 0),
            _utc(2026, 3, 1, 0, 5),
        ], type=pa.timestamp("us", tz="UTC")),
        "symbol": ["BTCUSDT-PERP"] * 2,
        "venue": ["BINANCE"] * 2,
        "timeframe": ["5m"] * 2,
        "open": [90000.0, 90100.0],
        "high": [90200.0, 90150.0],
        "low": [89900.0, 90000.0],
        "close": [90100.0, 90050.0],
        "volume": [100.5, 200.3],
    })

    ohlcv_1m = pa.table({
        "timestamp": pa.array([
            _utc(2026, 3, 1, 0, 0),
            _utc(2026, 3, 1, 0, 1),
            _utc(2026, 3, 1, 0, 2),
        ], type=pa.timestamp("us", tz="UTC")),
        "symbol": ["BTCUSDT-PERP"] * 3,
        "venue": ["BINANCE"] * 3,
        "timeframe": ["1m"] * 3,
        "open": [90000.0, 90010.0, 90020.0],
        "high": [90050.0, 90060.0, 90070.0],
        "low": [89990.0, 90000.0, 90010.0],
        "close": [90010.0, 90020.0, 90030.0],
        "volume": [10.1, 12.3, 11.5],
    })

    combined = pa.concat_tables([ohlcv_5m, ohlcv_1m])
    pq.write_table(combined, ohlcv_dir / "2026-03-01.parquet")

    # --- Open Interest ---
    oi_dir = catalog / "open_interest" / "BTCUSDT-PERP.BINANCE"
    oi_dir.mkdir(parents=True)
    oi_table = pa.table({
        "timestamp": pa.array([
            _utc(2026, 3, 1, 0, 1),
            _utc(2026, 3, 1, 0, 6),
        ], type=pa.timestamp("us", tz="UTC")),
        "symbol": ["BTCUSDT-PERP"] * 2,
        "venue": ["BINANCE"] * 2,
        "open_interest": [80000.0, 80100.0],
        "open_interest_value": [7.2e9, 7.3e9],
    })
    pq.write_table(oi_table, oi_dir / "2026-03-01.parquet")

    # --- Funding Rate ---
    fr_dir = catalog / "funding_rate" / "BTCUSDT-PERP.BINANCE"
    fr_dir.mkdir(parents=True)
    fr_table = pa.table({
        "timestamp": pa.array([
            _utc(2026, 3, 1, 0, 0),
        ], type=pa.timestamp("us", tz="UTC")),
        "symbol": ["BTCUSDT-PERP"] * 1,
        "venue": ["BINANCE"] * 1,
        "funding_rate": [0.00015],
        "next_funding_time": pa.array([
            _utc(2026, 3, 1, 8, 0),
        ], type=pa.timestamp("us", tz="UTC")),
        "predicted_rate": pa.array([None], type=pa.float64()),
    })
    pq.write_table(fr_table, fr_dir / "2026-03-01.parquet")

    return str(catalog)


@pytest.fixture
def e2e_db(tmp_path):
    """Create a temporary DuckDB with production schema and baseline data."""
    db_path = str(tmp_path / "e2e.duckdb")
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

    # Baseline data (simulates existing CSV ingestion)
    con.execute("""
        INSERT INTO klines_5m_history VALUES
        (TIMESTAMP '2026-02-28 23:55:00', 'BTCUSDT',
         89900.0, 90000.0, 89800.0, 89950.0, 50.0,
         TIMESTAMP '2026-03-01 00:00:00', NULL, NULL, NULL, NULL)
    """)
    con.execute("""
        INSERT INTO open_interest_history VALUES
        (1, TIMESTAMP '2026-02-28 23:56:00', 'BTCUSDT', 7100000000.0, 79000.0, 'binance_csv')
    """)
    con.execute("""
        INSERT INTO funding_rate_history VALUES
        (1, TIMESTAMP '2026-02-28 16:00:00', 'BTCUSDT', 0.00008, 8)
    """)

    con.close()
    return db_path


class TestGapFillE2E:
    def test_full_pipeline(self, e2e_db, e2e_catalog):
        """run_gap_fill fills klines_5m, klines_1m, OI, and funding for BTCUSDT."""
        result = run_gap_fill(
            db_path=e2e_db,
            catalog=e2e_catalog,
            symbols=["BTCUSDT"],
            dry_run=False,
        )

        assert result["total_inserted"] > 0

        # Verify per-symbol results exist
        btc = result["symbols"]["BTCUSDT"]
        assert "klines" in btc
        assert "oi" in btc
        assert "funding" in btc

        # Verify klines has both intervals
        klines = btc["klines"]
        assert "intervals" in klines
        assert "5m" in klines["intervals"]
        assert "1m" in klines["intervals"]

        # Verify actual data in DB
        con = duckdb.connect(e2e_db, read_only=True)

        # 5m klines: 2 new rows + 1 baseline = 3
        count_5m = con.execute(
            "SELECT COUNT(*) FROM klines_5m_history WHERE symbol = 'BTCUSDT'"
        ).fetchone()[0]
        assert count_5m == 3

        # 1m klines table should exist and have rows
        tables = [r[0] for r in con.execute("SHOW TABLES").fetchall()]
        assert "klines_1m_history" in tables
        count_1m = con.execute(
            "SELECT COUNT(*) FROM klines_1m_history WHERE symbol = 'BTCUSDT'"
        ).fetchone()[0]
        assert count_1m == 3

        # OI: 2 new rows + 1 baseline = 3
        count_oi = con.execute(
            "SELECT COUNT(*) FROM open_interest_history WHERE symbol = 'BTCUSDT'"
        ).fetchone()[0]
        assert count_oi == 3

        # Funding: 1 new row + 1 baseline = 2
        count_fr = con.execute(
            "SELECT COUNT(*) FROM funding_rate_history WHERE symbol = 'BTCUSDT'"
        ).fetchone()[0]
        assert count_fr == 2

        con.close()

    def test_freshness(self, e2e_db, e2e_catalog):
        """After gap-fill, max timestamps should be at the expected Parquet boundary."""
        run_gap_fill(
            db_path=e2e_db,
            catalog=e2e_catalog,
            symbols=["BTCUSDT"],
            dry_run=False,
        )

        con = duckdb.connect(e2e_db, read_only=True)

        max_5m = con.execute(
            "SELECT MAX(open_time) FROM klines_5m_history WHERE symbol = 'BTCUSDT'"
        ).fetchone()[0]
        assert max_5m == datetime(2026, 3, 1, 0, 5)

        max_oi = con.execute(
            "SELECT MAX(timestamp) FROM open_interest_history WHERE symbol = 'BTCUSDT'"
        ).fetchone()[0]
        assert max_oi == datetime(2026, 3, 1, 0, 6)

        max_fr = con.execute(
            "SELECT MAX(timestamp) FROM funding_rate_history WHERE symbol = 'BTCUSDT'"
        ).fetchone()[0]
        assert max_fr == datetime(2026, 3, 1, 0, 0)

        con.close()

    def test_idempotent(self, e2e_db, e2e_catalog):
        """Running gap-fill twice produces no additional rows."""
        r1 = run_gap_fill(e2e_db, e2e_catalog, ["BTCUSDT"], dry_run=False)
        r2 = run_gap_fill(e2e_db, e2e_catalog, ["BTCUSDT"], dry_run=False)

        assert r1["total_inserted"] > 0
        assert r2["total_inserted"] == 0
