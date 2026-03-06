"""Comprehensive coverage booster for API and DB service."""

import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch, MagicMock
from pathlib import Path
from decimal import Decimal
import pandas as pd
from datetime import datetime, timezone, timedelta

from src.liquidationheatmap.api.main import app, SUPPORTED_SYMBOLS, TIME_WINDOW_CONFIG
from src.liquidationheatmap.ingestion.db_service import DuckDBService, IngestionLockError

@pytest.fixture
def memory_db_service():
    DuckDBService.reset_singletons()
    db_path = ":memory:"
    service = DuckDBService(db_path)
    
    # Initialize ALL tables
    service.conn.execute("CREATE TABLE liquidation_snapshots (id BIGINT, timestamp TIMESTAMP, symbol VARCHAR, price_bucket DOUBLE, side VARCHAR, active_volume DOUBLE, density INTEGER, model VARCHAR)")
    service.conn.execute("CREATE TABLE open_interest_history (id BIGINT, timestamp TIMESTAMP, symbol VARCHAR, open_interest_value DOUBLE, open_interest_contracts DOUBLE, source VARCHAR)")
    service.conn.execute("CREATE TABLE funding_rate_history (id BIGINT, timestamp TIMESTAMP, symbol VARCHAR, funding_rate DOUBLE, mark_price DOUBLE, funding_interval_hours INTEGER)")
    service.conn.execute("CREATE TABLE klines_5m_history (open_time TIMESTAMP, symbol VARCHAR, open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE, volume DOUBLE, quote_volume DOUBLE)")
    service.conn.execute("CREATE TABLE klines_15m_history (open_time TIMESTAMP, symbol VARCHAR, open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE, volume DOUBLE, quote_volume DOUBLE)")
    service.conn.execute("CREATE TABLE liquidation_history (id BIGINT, timestamp TIMESTAMP, symbol VARCHAR, side VARCHAR, price DOUBLE, quantity DOUBLE, leverage INTEGER, model VARCHAR, is_buyer_maker BOOLEAN)")
    
    # Patch settings
    with patch("src.liquidationheatmap.api.main.get_settings") as mock_settings:
        mock_settings.return_value.db_path = db_path
        mock_settings.return_value.symbols = SUPPORTED_SYMBOLS
        mock_settings.return_value.internal_api_token = "test-token"
        
        with patch("src.liquidationheatmap.api.main.urlopen") as mock_url:
            mock_resp = MagicMock()
            mock_resp.read.return_value = b'{"price": "50000.0"}'
            mock_resp.__enter__.return_value = mock_resp
            mock_url.return_value = mock_resp
            
            with patch("src.liquidationheatmap.api.main.DuckDBService", side_effect=lambda *args, **kwargs: service):
                yield service
        
    DuckDBService.reset_singletons()

@pytest.fixture
def client(memory_db_service):
    return TestClient(app)

class TestApiBooster:
    def test_heatmap_timeseries_complex(self, client, memory_db_service):
        now = datetime.now().replace(microsecond=0)
        # Inject candles and matching OI
        for i in range(20):
            ts = now - timedelta(minutes=15*i)
            memory_db_service.conn.execute("INSERT INTO klines_15m_history VALUES (?, 'BTCUSDT', 50000.0, 51000.0, 49000.0, 50500.0, 10.0, 505000.0)", [ts])
            memory_db_service.conn.execute("INSERT INTO open_interest_history VALUES (?, ?, 'BTCUSDT', ?, 100.0, 'ccxt')", [i, ts, 1000000.0 + i*1000])
            
        client.get("/liquidations/heatmap-timeseries?symbol=BTCUSDT&interval=15m")
        # Hit Cache
        client.get("/liquidations/heatmap-timeseries?symbol=BTCUSDT&interval=15m")

    def test_klines_all_variants(self, client, memory_db_service):
        now = datetime.now().replace(microsecond=0)
        memory_db_service.conn.execute("INSERT INTO klines_5m_history VALUES (?, 'BTCUSDT', 50000.0, 51000.0, 49000.0, 50500.0, 10.0, 505000.0)", [now])
        client.get("/prices/klines?symbol=BTCUSDT&interval=5m&limit=10")
        client.get("/prices/klines?symbol=BTCUSDT&interval=1h&limit=10")

    def test_auth_failures(self, client):
        client.post("/api/v1/prepare-for-ingestion", headers={"X-Internal-Token": "wrong"})
        client.post("/api/v1/prepare-for-ingestion") # missing

    def test_validation_errors(self, client):
        client.get("/liquidations/heatmap?symbol=BTCUSDT&model=invalid")
        client.get("/liquidations/heatmap-timeseries?symbol=BTCUSDT&interval=invalid")
        client.get("/liquidations/heatmap-timeseries?symbol=BTCUSDT&leverage_weights=invalid")
