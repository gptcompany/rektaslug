"""Unit tests for data validation utilities."""

from decimal import Decimal
import pandas as pd
import pytest

from src.liquidationheatmap.ingestion.validators import (
    validate_price,
    validate_date_range,
    detect_outliers,
    validate_volume,
    validate_funding_rate,
    validate_symbol
)


def test_validate_price():
    """Should validate price within range."""
    assert validate_price(Decimal("67000.00")) is True
    assert validate_price(Decimal("10000.00")) is True
    assert validate_price(Decimal("500000.00")) is True
    
    # Out of range
    assert validate_price(Decimal("5000.00")) is False
    assert validate_price(Decimal("600000.00")) is False
    
    # Invalid types
    assert validate_price(None) is False
    assert validate_price("invalid") is False


def test_validate_date_range():
    """Should validate date range coverage."""
    # 7 days of data
    df = pd.DataFrame({
        'timestamp': pd.date_range('2024-10-22', periods=7, freq='D')
    })
    
    assert validate_date_range(df, expected_days=7) is True
    assert validate_date_range(df, expected_days=8, tolerance=1) is True
    assert validate_date_range(df, expected_days=6, tolerance=1) is True
    
    # Missing too many days
    assert validate_date_range(df, expected_days=10, tolerance=1) is False
    
    # Empty or missing column
    assert validate_date_range(pd.DataFrame(), expected_days=7) is False
    assert validate_date_range(pd.DataFrame({'val': [1]}), expected_days=7) is False


def test_detect_outliers():
    """Should detect outliers using Z-score."""
    df = pd.DataFrame({'value': [10, 12, 11, 10, 100, 9]})
    outliers = detect_outliers(df, 'value', std_threshold=2.0)
    
    assert 4 in outliers  # Index of 100
    assert len(outliers) == 1
    
    # No outliers if threshold is very high
    assert detect_outliers(df, 'value', std_threshold=10.0) == []
    
    # Empty or missing column
    assert detect_outliers(pd.DataFrame(), 'value') == []
    assert detect_outliers(df, 'missing') == []
    
    # Constant values (std = 0)
    df_const = pd.DataFrame({'value': [10, 10, 10]})
    assert detect_outliers(df_const, 'value') == []


def test_validate_volume():
    """Should validate non-negative volume."""
    assert validate_volume(Decimal("1234567.89")) is True
    assert validate_volume(Decimal("0")) is True
    assert validate_volume(Decimal("-1.0")) is False
    assert validate_volume("invalid") is False


def test_validate_funding_rate():
    """Should validate funding rate within bounds."""
    assert validate_funding_rate(Decimal("0.0001")) is True
    assert validate_funding_rate(Decimal("-0.0001")) is True
    assert validate_funding_rate(Decimal("0.01")) is True
    
    # Out of bounds
    assert validate_funding_rate(Decimal("0.05")) is False
    assert validate_funding_rate(Decimal("-0.02")) is False
    assert validate_funding_rate("invalid") is False


def test_validate_symbol():
    """Should validate supported trading symbols."""
    assert validate_symbol('BTCUSDT') is True
    assert validate_symbol('ETHUSDT') is True
    
    # Custom allowed list
    assert validate_symbol('SOLUSDT', allowed_symbols=['SOLUSDT', 'BTCUSDT']) is True
    assert validate_symbol('XRPUSDT', allowed_symbols=['BTCUSDT']) is False
