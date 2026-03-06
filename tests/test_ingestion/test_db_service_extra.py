"""Extra tests for DuckDBService to increase coverage."""

import os
import time
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock, patch
import pytest
import pandas as pd
import duckdb

from src.liquidationheatmap.ingestion.db_service import DuckDBService, IngestionLockError
from src.liquidationheatmap.models.position import HeatmapSnapshot, HeatmapCell

@pytest.fixture
def temp_db(tmp_path):
    db_path = str(tmp_path / "test_extra.duckdb")
    DuckDBService.reset_singletons()
    service = DuckDBService(db_path)
    
    # Initialize basic tables with ALL columns
    service.conn.execute("""
        CREATE TABLE IF NOT EXISTS klines_5m_history (
            open_time TIMESTAMP NOT NULL,
            symbol VARCHAR NOT NULL,
            open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE, volume DOUBLE,
            quote_volume DOUBLE,
            PRIMARY KEY (open_time, symbol)
        )
    """)
    service.conn.execute("""
        CREATE TABLE IF NOT EXISTS klines_1m_history (
            open_time TIMESTAMP NOT NULL,
            symbol VARCHAR NOT NULL,
            open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE, volume DOUBLE,
            quote_volume DOUBLE,
            PRIMARY KEY (open_time, symbol)
        )
    """)
    service.conn.execute("""
        CREATE TABLE IF NOT EXISTS aggtrades_history (
            agg_trade_id BIGINT NOT NULL,
            timestamp TIMESTAMP NOT NULL,
            symbol VARCHAR NOT NULL,
            exchange VARCHAR NOT NULL DEFAULT 'binance',
            price DOUBLE NOT NULL,
            quantity DOUBLE NOT NULL,
            side VARCHAR(4) NOT NULL,
            gross_value DOUBLE NOT NULL,
            PRIMARY KEY (agg_trade_id, symbol, exchange)
        )
    """)
    
    yield service
    service.close()
    DuckDBService.reset_singletons()

class TestDuckDBServiceExtra:
    def test_get_large_trades_csv_fallback(self, temp_db):
        # Empty DB for this symbol
        # DuckDBService.get_large_trades uses hardcoded path /media/sam/3TB-WDC/binance-history-data-downloader/data
        # If it doesn't exist, it should log error and return empty DF
        df = temp_db.get_large_trades("NONEXISTENT", min_gross_value=100000.0)
        assert df.empty

    def test_calculate_liquidations_sql_simple(self, temp_db):
        now = datetime.now()
        temp_db.conn.execute("INSERT INTO aggtrades_history VALUES (1, ?, 'BTCUSDT', 'binance', 50000.0, 10.0, 'buy', 1000000.0)", [now])
        df = temp_db.calculate_liquidations_sql("BTCUSDT", current_price=45000.0, min_gross_value=500000.0)
        assert not df.empty

    def test_load_and_cache_data_oi_fresh(self, temp_db):
        temp_db.conn.execute("DROP TABLE IF EXISTS open_interest_history")
        df = pd.DataFrame({
            "timestamp": [datetime.now()],
            "symbol": ["BTCUSDT"],
            "open_interest_value": [2000000.0],
            "open_interest_contracts": [40.0]
        })
        with patch("src.liquidationheatmap.ingestion.db_service.load_csv_glob", return_value=df), \
             patch("src.liquidationheatmap.ingestion.db_service._fetch_binance_price", return_value=Decimal("50000")):
            price, oi = temp_db._load_and_cache_data("BTCUSDT")
            assert float(oi) == 2000000.0

    def test_resolve_oi_kline_source_data(self, temp_db):
        symbol = "BTCUSDT"
        now = datetime.now()
        for i in range(1000):
            temp_db.conn.execute("INSERT INTO klines_1m_history (open_time, symbol) VALUES (?, ?)", [now - timedelta(minutes=i), symbol])
        table, interval = temp_db._resolve_oi_kline_source(symbol, lookback_days=1, kline_interval="auto")
        assert interval == "1m"

    def test_snapshot_lifecycle(self, temp_db):
        temp_db.initialize_snapshot_tables()
        now = datetime.now().replace(microsecond=0)
        snapshot = HeatmapSnapshot(timestamp=now, symbol="BTCUSDT")
        snapshot.get_cell(Decimal("50000")).long_density = Decimal("1000")
        temp_db.save_snapshot(snapshot)
        loaded = temp_db.load_snapshots("BTCUSDT", now - timedelta(minutes=1), now + timedelta(minutes=1))
        assert len(loaded) == 1

    def test_singleton_concurrency(self, tmp_path):
        db = str(tmp_path / "concurrent.db")
        DuckDBService.reset_singletons()
        import threading
        results = []
        def create():
            results.append(DuckDBService(db))
        threads = [threading.Thread(target=create) for _ in range(5)]
        for t in threads: t.start()
        for t in threads: t.join()
        assert all(r is results[0] for r in results)

    def test_close_all_instances(self, tmp_path):
        DuckDBService.reset_singletons()
        DuckDBService(str(tmp_path / "1.db"))
        DuckDBService(str(tmp_path / "2.db"))
        count = DuckDBService.close_all_instances()
        assert count >= 2
        
    def test_is_ingestion_locked_real(self):
        DuckDBService.set_ingestion_lock()
        assert DuckDBService.is_ingestion_locked()
        DuckDBService.release_ingestion_lock()
        assert not DuckDBService.is_ingestion_locked()
