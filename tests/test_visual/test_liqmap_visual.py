"""Unit tests for scripts/validate_liqmap_visual.py."""

from __future__ import annotations

import json
import sys
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
            "long_liquidations": [{"price": "84000", "volume": 100}],
            "short_liquidations": [
                {"price": "88000", "volume": 200},
                {"price": "89000", "volume": 150},
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
