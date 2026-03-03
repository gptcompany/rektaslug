"""Tests for POST /api/v1/gap-fill in-process endpoint."""

import asyncio
from pathlib import Path
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from src.liquidationheatmap.api.main import _gap_fill_lock, app
from src.liquidationheatmap.ingestion.db_service import DuckDBService, IngestionLockError


@pytest.fixture
def client():
    return TestClient(app)


@pytest.fixture(autouse=True)
def cleanup_ingestion_lock():
    """Ensure ingestion lock is released before and after each test."""
    DuckDBService.release_ingestion_lock()
    yield
    DuckDBService.release_ingestion_lock()


class TestGapFillEndpoint:
    def test_returns_400_when_catalog_missing(self, client):
        """Gap-fill should 400 when ccxt catalog path does not exist."""
        with patch("src.liquidationheatmap.api.main._settings") as mock_settings:
            mock_settings.ccxt_catalog = Path("/nonexistent/catalog")
            mock_settings.db_path = Path("/nonexistent/db")
            mock_settings.symbols = ("BTCUSDT",)
            response = client.post("/api/v1/gap-fill")

        assert response.status_code == 400
        data = response.json()
        assert data["status"] == "error"
        assert "not found" in data["message"].lower()

    def test_returns_409_when_already_running(self, client):
        """Gap-fill should 409 when another gap-fill is in progress."""
        # Acquire the lock externally to simulate an in-progress gap-fill
        loop = asyncio.new_event_loop()
        loop.run_until_complete(_gap_fill_lock.acquire())
        try:
            response = client.post("/api/v1/gap-fill")
            assert response.status_code == 409
            data = response.json()
            assert data["status"] == "conflict"
        finally:
            _gap_fill_lock.release()
            loop.close()

    def test_ingestion_lock_blocks_db_access(self):
        """IngestionLockError should be raised when lock is active."""
        DuckDBService.set_ingestion_lock()
        try:
            with pytest.raises(IngestionLockError):
                DuckDBService(read_only=True)
        finally:
            DuckDBService.release_ingestion_lock()

    def test_ingestion_lock_handler_returns_503(self, client):
        """Routes should return 503 when ingestion lock is active."""
        DuckDBService.set_ingestion_lock()
        try:
            # Any DB-backed route should trigger IngestionLockError -> 503
            response = client.get("/data/date-range?symbol=BTCUSDT")
            assert response.status_code == 503
            data = response.json()
            assert data["error"] == "Service Unavailable"
            assert "Retry-After" in response.headers
        finally:
            DuckDBService.release_ingestion_lock()

    def test_successful_gap_fill_with_mock(self, client):
        """Gap-fill should succeed when run_gap_fill returns results."""
        mock_result = {
            "symbols": {
                "BTCUSDT": {
                    "klines": {"inserted": 10, "duplicates": 2},
                    "oi": {"inserted": 5, "duplicates": 0},
                    "funding": {"inserted": 3, "duplicates": 0},
                }
            },
            "total_inserted": 18,
        }
        with patch("src.liquidationheatmap.api.main.run_gap_fill", return_value=mock_result):
            response = client.post("/api/v1/gap-fill")

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "success"
        assert data["total_inserted"] == 18
        assert "duration_seconds" in data
        assert "BTCUSDT" in data["symbols"]

    def test_dry_run_flag(self, client):
        """Gap-fill with dry_run=true should not write data."""
        mock_result = {
            "symbols": {
                "BTCUSDT": {
                    "klines": {"inserted": 0, "available": 100, "skipped": "dry_run"},
                    "oi": {"inserted": 0, "available": 50, "skipped": "dry_run"},
                    "funding": {"inserted": 0, "available": 10, "skipped": "dry_run"},
                }
            },
            "total_inserted": 0,
        }
        with patch("src.liquidationheatmap.api.main.run_gap_fill", return_value=mock_result):
            response = client.post("/api/v1/gap-fill?dry_run=true")

        assert response.status_code == 200
        data = response.json()
        assert data["total_inserted"] == 0


class TestRunGapFillModule:
    """Test the extracted gap_fill module directly."""

    def test_run_gap_fill_raises_on_missing_catalog(self, tmp_path):
        from src.liquidationheatmap.ingestion.gap_fill import run_gap_fill

        db_file = tmp_path / "test.duckdb"
        db_file.touch()
        with pytest.raises(FileNotFoundError, match="catalog"):
            run_gap_fill(str(db_file), "/nonexistent/catalog", ["BTCUSDT"])

    def test_run_gap_fill_raises_on_missing_db(self, tmp_path):
        from src.liquidationheatmap.ingestion.gap_fill import run_gap_fill

        catalog_dir = tmp_path / "catalog"
        catalog_dir.mkdir()
        with pytest.raises(FileNotFoundError, match="database"):
            run_gap_fill("/nonexistent/db.duckdb", str(catalog_dir), ["BTCUSDT"])
