"""Integration tests for shell script scenarios using curl mocking (non-interactive)."""

import os
import subprocess
import pytest
from pathlib import Path

@pytest.fixture
def mock_curl(tmp_path):
    """Create a mock curl script to intercept API calls."""
    mock_bin = tmp_path / "bin"
    mock_bin.mkdir()
    curl_script = mock_bin / "curl"
    
    # Improved mock curl that correctly handles the body + http_code pattern
    # ensuring they are on separate lines for tail -n1 to work.
    script_content = """#!/bin/bash
# Mock curl
RESPONSE_BODY='{"status": "success"}'
HTTP_CODE="200"

if [[ "$*" == *"gap-fill"* ]]; then
    RESPONSE_BODY='{"status": "success", "total_inserted": 42}'
elif [[ "$*" == *"prepare-for-ingestion"* ]]; then
    RESPONSE_BODY='{"status": "success", "connections_closed": 5}'
fi

if [ -f "$MOCK_CURL_RESPONSE_FILE" ]; then
    RESPONSE_BODY=$(cat "$MOCK_CURL_RESPONSE_FILE")
    if [[ "$RESPONSE_BODY" == *"conflicting lock held"* ]]; then
        HTTP_CODE="500"
    fi
fi

# If -w "%{http_code}" is present, output body + code on new line
if [[ "$*" == *"%{http_code}"* ]]; then
    echo "$RESPONSE_BODY"
    echo "$HTTP_CODE"
else
    echo "$RESPONSE_BODY"
fi
exit 0
"""
    curl_script.write_text(script_content)
    curl_script.chmod(0o755)
    return mock_bin

@pytest.fixture
def mock_env(tmp_path, mock_curl):
    """Set up environment variables for the shell script."""
    env = os.environ.copy()
    env["PATH"] = f"{mock_curl}:{env['PATH']}"
    env["HEATMAP_ENV_FILE"] = "/dev/null"
    env["HEATMAP_API_URL"] = "http://mock-api"
    env["HEATMAP_PROJECT_ROOT"] = str(Path(".").resolve())
    env["HEATMAP_DB_PATH"] = str(tmp_path / "test.duckdb")
    env["HEATMAP_CCXT_CATALOG"] = str(tmp_path / "catalog")
    env["HEATMAP_SYMBOLS_SHELL"] = "BTCUSDT"
    env["HEATMAP_LOG_DIR"] = str(tmp_path / "logs")
    env["REKTSLUG_INTERNAL_TOKEN"] = ""
    
    os.makedirs(env["HEATMAP_LOG_DIR"], exist_ok=True)
    os.makedirs(env["HEATMAP_CCXT_CATALOG"], exist_ok=True)
    
    import duckdb
    conn = duckdb.connect(env["HEATMAP_DB_PATH"])
    conn.execute("CREATE TABLE IF NOT EXISTS klines_5m_history (open_time TIMESTAMP, symbol VARCHAR, open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE, volume DOUBLE, close_time TIMESTAMP, quote_volume DOUBLE, count INTEGER, taker_buy_volume DOUBLE, taker_buy_quote_volume DOUBLE)")
    conn.execute("CREATE TABLE IF NOT EXISTS open_interest_history (id BIGINT, timestamp TIMESTAMP, symbol VARCHAR, open_interest_value DOUBLE, open_interest_contracts DOUBLE, source VARCHAR)")
    conn.execute("CREATE TABLE IF NOT EXISTS funding_rate_history (id BIGINT, timestamp TIMESTAMP, symbol VARCHAR, funding_rate DOUBLE, funding_interval_hours INTEGER)")
    conn.execute("INSERT INTO klines_5m_history (open_time, symbol) VALUES ('2026-01-01 00:00:00', 'BTCUSDT')")
    conn.execute("INSERT INTO open_interest_history (timestamp, symbol) VALUES ('2026-01-01 00:00:00', 'BTCUSDT')")
    conn.execute("INSERT INTO funding_rate_history (timestamp, symbol) VALUES ('2026-01-01 00:00:00', 'BTCUSDT')")
    conn.close()
    
    return env

class TestShellScenariosMocked:
    """Test shell scripts using curl mocking instead of a live server."""

    def test_run_gap_fill_success(self, mock_env):
        """Test success path."""
        result = subprocess.run(
            ["bash", "scripts/run-ccxt-gap-fill.sh"],
            env=mock_env, capture_output=True, text=True
        )
        assert result.returncode == 0
        assert "Gap-fill complete" in result.stdout

    def test_run_gap_fill_lock_contention(self, mock_env, tmp_path):
        """Test non-fatal lock contention handling."""
        resp_file = tmp_path / "resp.json"
        resp_file.write_text("Could not set lock: conflicting lock held")
        mock_env["MOCK_CURL_RESPONSE_FILE"] = str(resp_file)

        result = subprocess.run(
            ["bash", "scripts/run-ccxt-gap-fill.sh"],
            env=mock_env, capture_output=True, text=True
        )
        assert result.returncode == 0
        assert "SKIPPED_LOCK_CONTENTION:" in result.stdout
        assert "lock contention" in result.stdout.lower()

    def test_run_ingestion_dry_run(self, mock_env):
        """Test main ingestion script in dry-run mode."""
        result = subprocess.run(
            ["bash", "scripts/run-ingestion.sh", "--dry-run"],
            env=mock_env, capture_output=True, text=True
        )
        assert result.returncode == 0
        assert "Database write access confirmed" in result.stdout
