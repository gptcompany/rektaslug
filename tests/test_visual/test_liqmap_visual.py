"""Unit tests for scripts/validate_liqmap_visual.py."""

from __future__ import annotations

import json
import sys
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from threading import Thread
from unittest.mock import patch

import pytest

# Ensure scripts directory is importable
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))


@pytest.fixture()
def free_port():
    """Find a free TCP port."""
    import socket

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class _LevelsHandler(BaseHTTPRequestHandler):
    """Minimal handler returning a canned /liquidations/levels response."""

    response_body: str = ""

    def do_GET(self):
        if "/liquidations/levels" in self.path:
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(self.response_body.encode())
        elif self.path == "/health":
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"ok")
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, *_args):
        pass  # suppress noisy logs


def test_preflight_liqmap_api_parses_response(free_port):
    """preflight_liqmap_api should parse a valid /liquidations/levels JSON."""
    from validate_liqmap_visual import preflight_liqmap_api

    canned = json.dumps(
        {
            "current_price": "86000.5",
            "long_liquidations": [
                {"price_level": "84000", "volume": "100", "count": 1, "leverage": "10x"}
            ],
            "short_liquidations": [
                {"price_level": "88000", "volume": "200", "count": 1, "leverage": "5x"},
                {"price_level": "89000", "volume": "150", "count": 1, "leverage": "25x"},
            ],
        }
    )
    _LevelsHandler.response_body = canned
    server = HTTPServer(("127.0.0.1", free_port), _LevelsHandler)
    t = Thread(target=server.serve_forever, daemon=True)
    t.start()
    try:
        result = preflight_liqmap_api(
            api_base=f"http://127.0.0.1:{free_port}",
            symbol="BTCUSDT",
            model="openinterest",
            timeframe=7,
        )
        assert result["ok"] is True
        assert result["long_count"] == 1
        assert result["short_count"] == 2
        assert result["current_price"] == "86000.5"
    finally:
        server.shutdown()


def test_preflight_liqmap_api_handles_error():
    """preflight_liqmap_api should return ok=False when server is unreachable."""
    from validate_liqmap_visual import preflight_liqmap_api

    result = preflight_liqmap_api(
        api_base="http://127.0.0.1:1",  # unreachable port
        symbol="BTCUSDT",
        model="openinterest",
        timeframe=7,
    )
    assert result["ok"] is False
    assert "error" in result


def test_validate_liqmap_cli_args_defaults():
    """parse_args should provide sensible defaults."""
    from validate_liqmap_visual import parse_args

    with patch("sys.argv", ["validate_liqmap_visual.py"]):
        args = parse_args()
    assert args.symbol == "BTCUSDT"
    assert args.model == "openinterest"
    assert args.timeframe == 7
    assert args.coin == "BTC"
    assert args.exchange == "binance"
    assert args.coinank_timeframe == "1w"
    assert args.chart_mode == "bar"
    assert args.max_freshness_minutes == 5
    assert args.allow_stale_data is False


# --- Step 2: fetch_liqmap_payload tests ---


def test_fetch_liqmap_payload_returns_full_payload(free_port):
    """fetch_liqmap_payload should return the complete API JSON."""
    from validate_liqmap_visual import fetch_liqmap_payload

    canned = json.dumps(
        {
            "symbol": "BTCUSDT",
            "model": "openinterest",
            "current_price": "86000.5",
            "long_liquidations": [
                {"price_level": "84000", "volume": "100", "count": 1, "leverage": "10x"},
            ],
            "short_liquidations": [
                {"price_level": "88000", "volume": "200", "count": 1, "leverage": "5x"},
            ],
        }
    )
    _LevelsHandler.response_body = canned
    server = HTTPServer(("127.0.0.1", free_port), _LevelsHandler)
    t = Thread(target=server.serve_forever, daemon=True)
    t.start()
    try:
        payload = fetch_liqmap_payload(
            api_base=f"http://127.0.0.1:{free_port}",
            symbol="BTCUSDT",
            model="openinterest",
            timeframe=7,
        )
        assert "long_liquidations" in payload
        assert "short_liquidations" in payload
        assert payload["current_price"] == "86000.5"
        assert len(payload["long_liquidations"]) == 1
        assert len(payload["short_liquidations"]) == 1
    finally:
        server.shutdown()


def test_fetch_liqmap_payload_returns_none_on_error():
    """fetch_liqmap_payload should return None when server is unreachable."""
    from validate_liqmap_visual import fetch_liqmap_payload

    result = fetch_liqmap_payload(
        api_base="http://127.0.0.1:1",
        symbol="BTCUSDT",
        model="openinterest",
        timeframe=7,
    )
    assert result is None


# --- Step 4: compute_validation_metrics tests ---

SAMPLE_PAYLOAD = {
    "current_price": "86000.5",
    "long_liquidations": [
        {"price_level": "84000", "volume": "500000", "count": 1, "leverage": "10x"},
        {"price_level": "82000", "volume": "300000", "count": 1, "leverage": "25x"},
        {"price_level": "80000", "volume": "200000", "count": 1, "leverage": "5x"},
        {"price_level": "78000", "volume": "150000", "count": 1, "leverage": "50x"},
        {"price_level": "76000", "volume": "100000", "count": 1, "leverage": "100x"},
        {"price_level": "75000", "volume": "80000", "count": 1, "leverage": "10x"},
    ],
    "short_liquidations": [
        {"price_level": "88000", "volume": "400000", "count": 1, "leverage": "10x"},
        {"price_level": "90000", "volume": "350000", "count": 1, "leverage": "25x"},
        {"price_level": "92000", "volume": "250000", "count": 1, "leverage": "5x"},
        {"price_level": "94000", "volume": "200000", "count": 1, "leverage": "50x"},
        {"price_level": "96000", "volume": "150000", "count": 1, "leverage": "100x"},
    ],
}


def test_compute_validation_metrics_price_range():
    """Metrics should contain correct price range info."""
    from validate_liqmap_visual import compute_validation_metrics

    metrics = compute_validation_metrics(SAMPLE_PAYLOAD)
    pr = metrics["price_range"]
    assert pr["min_price"] == 75000.0
    assert pr["max_price"] == 96000.0
    assert pr["current_price"] == 86000.5
    assert 0.0 <= pr["pct_below"] <= 100.0
    assert 0.0 <= pr["pct_above"] <= 100.0


def test_compute_validation_metrics_volume_totals():
    """Metrics should contain volume totals and ratio."""
    from validate_liqmap_visual import compute_validation_metrics

    metrics = compute_validation_metrics(SAMPLE_PAYLOAD)
    vt = metrics["volume_totals"]
    assert vt["long_total"] == 1330000.0
    assert vt["short_total"] == 1350000.0
    assert isinstance(vt["long_short_ratio"], float)
    assert vt["long_short_ratio"] == pytest.approx(1330000.0 / 1350000.0, rel=1e-4)


def test_compute_validation_metrics_leverage_distribution():
    """Metrics should contain per-tier leverage distribution percentages."""
    from validate_liqmap_visual import compute_validation_metrics

    metrics = compute_validation_metrics(SAMPLE_PAYLOAD)
    ld = metrics["leverage_distribution"]
    # Must sum to ~100%
    total_pct = sum(ld.values())
    assert total_pct == pytest.approx(100.0, abs=0.1)
    # Known tiers present
    assert "10x" in ld
    assert "25x" in ld


def test_compute_validation_metrics_top_levels():
    """Metrics should contain top-5 levels for long and short."""
    from validate_liqmap_visual import compute_validation_metrics

    metrics = compute_validation_metrics(SAMPLE_PAYLOAD)
    tl = metrics["top_levels"]
    assert len(tl["long"]) <= 5
    assert len(tl["short"]) <= 5
    # Top long should be highest volume
    assert tl["long"][0]["price_level"] == 84000.0
    assert tl["long"][0]["volume"] == 500000.0


def test_compute_validation_metrics_empty_payload():
    """Metrics should handle empty/partial payload gracefully."""
    from validate_liqmap_visual import compute_validation_metrics

    empty = {"current_price": "86000", "long_liquidations": [], "short_liquidations": []}
    metrics = compute_validation_metrics(empty)
    assert metrics["volume_totals"]["long_total"] == 0.0
    assert metrics["volume_totals"]["short_total"] == 0.0
    assert metrics["volume_totals"]["long_short_ratio"] == 0.0
    assert len(metrics["top_levels"]["long"]) == 0
    assert len(metrics["top_levels"]["short"]) == 0


def test_compute_validation_metrics_none_payload():
    """Metrics should handle None payload returning error dict."""
    from validate_liqmap_visual import compute_validation_metrics

    metrics = compute_validation_metrics(None)
    assert "error" in metrics


# --- Step 5: fetch_data_freshness tests ---


class _FreshnessHandler(BaseHTTPRequestHandler):
    """Handler that returns canned /data/date-range and /liquidations/levels."""

    date_range_body: str = ""
    levels_body: str = ""

    def do_GET(self):
        if "/data/date-range" in self.path:
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(self.date_range_body.encode())
        elif "/liquidations/levels" in self.path:
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(self.levels_body.encode())
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, *_args):
        pass


def test_fetch_data_freshness_returns_fields(free_port):
    """fetch_data_freshness should return fresh-state fields for recent data."""
    from validate_liqmap_visual import fetch_data_freshness

    recent_end = (datetime.now(timezone.utc) - timedelta(minutes=2)).isoformat()
    canned = json.dumps(
        {
            "symbol": "BTCUSDT",
            "start_date": "2025-01-01T00:00:00",
            "end_date": recent_end,
        }
    )
    _FreshnessHandler.date_range_body = canned
    server = HTTPServer(("127.0.0.1", free_port), _FreshnessHandler)
    t = Thread(target=server.serve_forever, daemon=True)
    t.start()
    try:
        result = fetch_data_freshness(
            api_base=f"http://127.0.0.1:{free_port}",
            symbol="BTCUSDT",
        )
        assert "end_date" in result
        assert "age_hours" in result
        assert "age_minutes" in result
        assert isinstance(result["age_hours"], float)
        assert isinstance(result["age_minutes"], float)
        assert result["stale"] is False
        assert "warning" not in result
    finally:
        server.shutdown()


def test_fetch_data_freshness_marks_stale_when_over_threshold(free_port):
    """fetch_data_freshness should mark stale payloads when older than the threshold."""
    from validate_liqmap_visual import fetch_data_freshness

    stale_end = (datetime.now(timezone.utc) - timedelta(minutes=9)).isoformat()
    canned = json.dumps(
        {
            "symbol": "BTCUSDT",
            "start_date": "2025-01-01T00:00:00",
            "end_date": stale_end,
        }
    )
    _FreshnessHandler.date_range_body = canned
    server = HTTPServer(("127.0.0.1", free_port), _FreshnessHandler)
    t = Thread(target=server.serve_forever, daemon=True)
    t.start()
    try:
        result = fetch_data_freshness(
            api_base=f"http://127.0.0.1:{free_port}",
            symbol="BTCUSDT",
            max_age_minutes=5,
        )
        assert result["stale"] is True
        assert "warning" in result
        assert result["max_age_minutes"] == 5
    finally:
        server.shutdown()


def test_fetch_data_freshness_returns_error_on_failure():
    """fetch_data_freshness should return error dict when endpoint unreachable."""
    from validate_liqmap_visual import fetch_data_freshness

    result = fetch_data_freshness(
        api_base="http://127.0.0.1:1",
        symbol="BTCUSDT",
    )
    assert "error" in result
