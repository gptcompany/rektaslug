"""Unit tests for gap_fill.py edge cases."""

import pytest
from unittest.mock import MagicMock, patch
from pathlib import Path

from src.liquidationheatmap.ingestion.gap_fill import (
    _kline_minutes,
    _fill_klines_interval,
    fill_open_interest,
    fill_funding_rate,
    validate_gaps,
    run_gap_fill
)
import duckdb

def test_kline_minutes_invalid():
    """Test _kline_minutes raises ValueError for invalid input."""
    with pytest.raises(ValueError, match="Unsupported kline interval"):
        _kline_minutes("1h")
    with pytest.raises(ValueError, match="Unsupported kline interval"):
        _kline_minutes("m")
    with pytest.raises(ValueError, match="Unsupported kline interval"):
        _kline_minutes("5")

def test_fill_klines_no_table(tmp_path):
    """Test _fill_klines_interval handles missing table (CatalogException) gracefully."""
    con = MagicMock()
    # Mock _ensure_klines_table
    with patch("src.liquidationheatmap.ingestion.gap_fill.get_watermark", side_effect=duckdb.CatalogException("Table does not exist")), \
         patch("src.liquidationheatmap.ingestion.gap_fill.parquet_glob", return_value="*.parquet"), \
         patch("src.liquidationheatmap.ingestion.gap_fill._ensure_klines_table"):
        
        # When get_watermark raises CatalogException, it should proceed with watermark=None
        # and then execute query. We mock execute to throw an exception to hit the 'no_parquet' path or return 0
        con.execute.side_effect = Exception("No files")
        
        result = _fill_klines_interval(con, str(tmp_path), "BTCUSDT", "1m", False)
        
        assert result["inserted"] == 0
        assert result["skipped"] == "no_parquet"

def test_fill_klines_no_parquet(tmp_path):
    """Test _fill_klines_interval handles Exception when counting Parquet."""
    con = MagicMock()
    with patch("src.liquidationheatmap.ingestion.gap_fill.get_watermark", return_value="2025-01-01"), \
         patch("src.liquidationheatmap.ingestion.gap_fill.parquet_glob", return_value="*.parquet"), \
         patch("src.liquidationheatmap.ingestion.gap_fill._ensure_klines_table"):
        
        con.execute.side_effect = Exception("File not found")
        result = _fill_klines_interval(con, str(tmp_path), "BTCUSDT", "5m", False)
        assert result["skipped"] == "no_parquet"

def test_fill_klines_dry_run(tmp_path):
    """Test _fill_klines_interval dry_run mode."""
    con = MagicMock()
    with patch("src.liquidationheatmap.ingestion.gap_fill.get_watermark", return_value="2025-01-01"), \
         patch("src.liquidationheatmap.ingestion.gap_fill.parquet_glob", return_value="*.parquet"), \
         patch("src.liquidationheatmap.ingestion.gap_fill._ensure_klines_table"):
        
        con.execute.return_value.fetchone.return_value = [10]
        result = _fill_klines_interval(con, str(tmp_path), "BTCUSDT", "5m", True)
        assert result["skipped"] == "dry_run"
        assert result["available"] == 10

def test_fill_open_interest_no_baseline(tmp_path):
    """Test fill_open_interest handles None watermark."""
    con = MagicMock()
    with patch("src.liquidationheatmap.ingestion.gap_fill.get_watermark", return_value=None), \
         patch("src.liquidationheatmap.ingestion.gap_fill.parquet_glob", return_value="*.parquet"):
        
        result = fill_open_interest(con, str(tmp_path), "BTCUSDT", False)
        assert result["skipped"] == "no_baseline"

def test_fill_open_interest_no_parquet(tmp_path):
    """Test fill_open_interest handles Exception when counting Parquet."""
    con = MagicMock()
    with patch("src.liquidationheatmap.ingestion.gap_fill.get_watermark", return_value="2025-01-01"), \
         patch("src.liquidationheatmap.ingestion.gap_fill.parquet_glob", return_value="*.parquet"):
        
        con.execute.side_effect = Exception("File not found")
        result = fill_open_interest(con, str(tmp_path), "BTCUSDT", False)
        assert result["skipped"] == "no_parquet"

def test_fill_open_interest_dry_run(tmp_path):
    """Test fill_open_interest dry_run mode."""
    con = MagicMock()
    with patch("src.liquidationheatmap.ingestion.gap_fill.get_watermark", return_value="2025-01-01"), \
         patch("src.liquidationheatmap.ingestion.gap_fill.parquet_glob", return_value="*.parquet"):
        
        con.execute.return_value.fetchone.return_value = [20]
        result = fill_open_interest(con, str(tmp_path), "BTCUSDT", True)
        assert result["skipped"] == "dry_run"
        assert result["available"] == 20

def test_fill_funding_no_baseline(tmp_path):
    """Test fill_funding_rate handles None watermark."""
    con = MagicMock()
    with patch("src.liquidationheatmap.ingestion.gap_fill.get_watermark", return_value=None), \
         patch("src.liquidationheatmap.ingestion.gap_fill.parquet_glob", return_value="*.parquet"):
        
        result = fill_funding_rate(con, str(tmp_path), "BTCUSDT", False)
        assert result["skipped"] == "no_baseline"

def test_fill_funding_no_parquet(tmp_path):
    """Test fill_funding_rate handles Exception when counting Parquet."""
    con = MagicMock()
    with patch("src.liquidationheatmap.ingestion.gap_fill.get_watermark", return_value="2025-01-01"), \
         patch("src.liquidationheatmap.ingestion.gap_fill.parquet_glob", return_value="*.parquet"):
        
        con.execute.side_effect = Exception("File not found")
        result = fill_funding_rate(con, str(tmp_path), "BTCUSDT", False)
        assert result["skipped"] == "no_parquet"

def test_fill_funding_dry_run(tmp_path):
    """Test fill_funding_rate dry_run mode."""
    con = MagicMock()
    with patch("src.liquidationheatmap.ingestion.gap_fill.get_watermark", return_value="2025-01-01"), \
         patch("src.liquidationheatmap.ingestion.gap_fill.parquet_glob", return_value="*.parquet"):
        
        con.execute.return_value.fetchone.return_value = [5]
        result = fill_funding_rate(con, str(tmp_path), "BTCUSDT", True)
        assert result["skipped"] == "dry_run"
        assert result["available"] == 5

def test_validate_gaps_none_watermark():
    """Test validate_gaps does nothing if watermark is None."""
    con = MagicMock()
    validate_gaps(con, "BTCUSDT", None)
    con.execute.assert_not_called()

def test_run_gap_fill_catalog_not_found(tmp_path):
    """Test run_gap_fill raises FileNotFoundError for missing catalog."""
    with pytest.raises(FileNotFoundError, match="CCXT catalog not found"):
        run_gap_fill("dummy_db.duckdb", "/nonexistent/catalog", ["BTCUSDT"])

def test_run_gap_fill_db_not_found(tmp_path):
    """Test run_gap_fill raises FileNotFoundError for missing db."""
    # Create fake catalog
    catalog = tmp_path / "catalog"
    catalog.mkdir()
    
    with pytest.raises(FileNotFoundError, match="DuckDB database not found"):
        run_gap_fill(str(tmp_path / "nonexistent.db"), str(catalog), ["BTCUSDT"])
