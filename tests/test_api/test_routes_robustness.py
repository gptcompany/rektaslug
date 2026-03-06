"""Robustness tests for API routes."""

import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch, MagicMock
from pathlib import Path
from decimal import Decimal
import json
import pandas as pd
from datetime import datetime, timezone, timedelta

from src.liquidationheatmap.api.main import app, SUPPORTED_SYMBOLS
from src.liquidationheatmap.ingestion.db_service import DuckDBService, IngestionLockError

@pytest.fixture
def memory_db_service():
    DuckDBService.reset_singletons()
    db_path = ":memory:"
    service = DuckDBService(db_path)
    
    # Initialize basic tables
    service.conn.execute("""
        CREATE TABLE IF NOT EXISTS liquidation_snapshots (
            id BIGINT PRIMARY KEY,
            timestamp TIMESTAMP NOT NULL,
            symbol VARCHAR NOT NULL,
            price_bucket DOUBLE NOT NULL,
            side VARCHAR NOT NULL,
            active_volume DOUBLE NOT NULL,
            density INTEGER DEFAULT 1,
            model VARCHAR DEFAULT 'binance_standard'
        )
    """)
    service.conn.execute("""
        CREATE TABLE IF NOT EXISTS open_interest_history (
            id BIGINT,
            timestamp TIMESTAMP NOT NULL,
            symbol VARCHAR NOT NULL,
            open_interest_value DOUBLE NOT NULL,
            open_interest_contracts DOUBLE NOT NULL,
            source VARCHAR DEFAULT 'ccxt'
        )
    """)
    service.conn.execute("""
        CREATE TABLE IF NOT EXISTS klines_5m_history (
            open_time TIMESTAMP NOT NULL,
            symbol VARCHAR NOT NULL,
            open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE, volume DOUBLE,
            PRIMARY KEY (open_time, symbol)
        )
    """)
    service.conn.execute("""
        CREATE TABLE IF NOT EXISTS klines_15m_history (
            open_time TIMESTAMP NOT NULL,
            symbol VARCHAR NOT NULL,
            open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE, volume DOUBLE,
            PRIMARY KEY (open_time, symbol)
        )
    """)
    service.conn.execute("""
        CREATE TABLE IF NOT EXISTS liquidation_history (
            id BIGINT PRIMARY KEY,
            timestamp TIMESTAMP NOT NULL,
            symbol VARCHAR NOT NULL,
            side VARCHAR NOT NULL,
            price DOUBLE NOT NULL,
            quantity DOUBLE NOT NULL,
            leverage INTEGER DEFAULT 10,
            model VARCHAR DEFAULT 'binance_standard',
            is_buyer_maker BOOLEAN
        )
    """)
    
    # Mock settings
    with patch("src.liquidationheatmap.api.main.get_settings") as mock_settings:
        mock_settings.return_value.db_path = db_path
        mock_settings.return_value.symbols = SUPPORTED_SYMBOLS
        mock_settings.return_value.oi_kline_interval = "5m"
        
        # Patch urlopen
        with patch("src.liquidationheatmap.api.main.urlopen") as mock_url:
            mock_resp = MagicMock()
            mock_resp.read.return_value = b'{"price": "50000.0"}'
            mock_resp.__enter__.return_value = mock_resp
            mock_url.return_value = mock_resp
            
            # Use a smart mock for DuckDBService that supports lock simulation
            def mock_service_init(db_path=None, read_only=False):
                if DuckDBService.is_ingestion_locked():
                    raise IngestionLockError("Locked")
                return service
                
            with patch("src.liquidationheatmap.api.main.DuckDBService", side_effect=mock_service_init):
                yield service
        
    DuckDBService.reset_singletons()

@pytest.fixture
def client(memory_db_service):
    return TestClient(app)

class TestApiRobustness:
    """Test suite for API error handling and edge cases."""

    def test_health_check(self, client):
        response = client.get("/health")
        assert response.status_code == 200
        assert response.json()["status"] == "ok"

    def test_heatmap_with_real_data(self, client, memory_db_service):
        now = datetime.now(timezone.utc).replace(microsecond=0)
        memory_db_service.conn.execute("""
            INSERT INTO liquidation_snapshots (id, timestamp, symbol, price_bucket, side, active_volume)
            VALUES (1, ?, 'BTCUSDT', 50000.0, 'long', 1000000.0)
        """, [now])
        memory_db_service.conn.execute("""
            INSERT INTO open_interest_history (id, timestamp, symbol, open_interest_value, open_interest_contracts)
            VALUES (1, ?, 'BTCUSDT', 5000000.0, 100.0)
        """, [now])
        
        response = client.get("/liquidations/heatmap?symbol=BTCUSDT")
        assert response.status_code == 200
        data = response.json()
        assert len(data["data"]) == 1

    def test_compare_models_real(self, client, memory_db_service):
        now = datetime.now(timezone.utc).replace(microsecond=0)
        memory_db_service.conn.execute("""
            INSERT INTO open_interest_history (id, timestamp, symbol, open_interest_value, open_interest_contracts)
            VALUES (1, ?, 'BTCUSDT', 5000000.0, 100.0)
        """, [now])
        
        response = client.get("/liquidations/compare-models?symbol=BTCUSDT")
        assert response.status_code == 200
        assert "models" in response.json()

    def test_liquidation_history_real(self, client, memory_db_service):
        now = datetime.now(timezone.utc).replace(microsecond=0)
        memory_db_service.conn.execute("""
            INSERT INTO liquidation_history (id, timestamp, symbol, side, price, quantity)
            VALUES (1, ?, 'BTCUSDT', 'buy', 50000.0, 1.0)
        """, [now])
        
        response = client.get("/liquidations/history?symbol=BTCUSDT")
        assert response.status_code == 200
        assert len(response.json()) >= 1

    def test_klines_aggregation_real(self, client, memory_db_service):
        now = datetime.now(timezone.utc).replace(microsecond=0)
        for i in range(5):
            memory_db_service.conn.execute("""
                INSERT INTO klines_15m_history (open_time, symbol, open, high, low, close, volume)
                VALUES (?, 'BTCUSDT', 50000.0, 51000.0, 49000.0, 50500.0, 10.0)
            """, [now - timedelta(minutes=15*i)])
            
        response = client.get("/prices/klines?symbol=BTCUSDT&interval=1h&limit=10")
        assert response.status_code == 200
        data = response.json()
        assert len(data["data"]) >= 1

    def test_invalid_symbol_400(self, client):
        response = client.get("/data/date-range?symbol=INVALID")
        assert response.status_code == 400

    def test_ingestion_lock_503(self, client):
        with patch("src.liquidationheatmap.ingestion.db_service.DuckDBService.is_ingestion_locked", return_value=True):
            response = client.get("/liquidations/heatmap?symbol=BTCUSDT")
            assert response.status_code == 503
