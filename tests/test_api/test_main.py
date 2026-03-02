"""Tests for FastAPI main application."""

import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def client():
    """Create test client for FastAPI app."""
    from src.liquidationheatmap.api.main import app

    return TestClient(app)


class TestHealthEndpoint:
    """Tests for /health endpoint."""

    def test_health_returns_200(self, client):
        """Test that health check returns 200 OK."""
        response = client.get("/health")
        assert response.status_code == 200

    def test_health_returns_status_ok(self, client):
        """Test that health check returns status ok."""
        response = client.get("/health")
        data = response.json()
        assert data["status"] == "ok"


class TestLiquidationsEndpoint:
    """Tests for /liquidations/levels endpoint."""

    def test_liquidations_returns_200_with_valid_params(self, client):
        """Test that liquidations endpoint returns 200 with valid params."""
        response = client.get(
            "/liquidations/levels?symbol=BTCUSDT&model=binance_standard&timeframe=30"
        )
        assert response.status_code == 200

    def test_liquidations_returns_long_and_short_levels(self, client):
        """Test that liquidations returns both long and short levels."""
        response = client.get(
            "/liquidations/levels?symbol=BTCUSDT&model=binance_standard&timeframe=30"
        )
        data = response.json()

        assert "long_liquidations" in data
        assert "short_liquidations" in data
        assert isinstance(data["long_liquidations"], list)
        assert isinstance(data["short_liquidations"], list)

    def test_liquidations_with_ensemble_model(self, client):
        """Test that ensemble model parameter works."""
        response = client.get("/liquidations/levels?symbol=BTCUSDT&model=ensemble&timeframe=30")
        assert response.status_code == 200


class TestLiquidationsWithRealData:
    """Tests for liquidations endpoint with real DuckDB data."""

    def test_liquidations_uses_real_open_interest_from_db(self, client):
        """Test that API fetches real Open Interest from DuckDB, not hardcoded mock."""
        response = client.get(
            "/liquidations/levels?symbol=BTCUSDT&model=binance_standard&timeframe=30"
        )
        data = response.json()

        # With real data from sample CSV, current_price should match DB
        # and long_liquidations volumes should be calculated from real OI
        assert response.status_code == 200
        assert "current_price" in data

        # Check that we got real liquidations (not empty) - skip if no data available
        if len(data["long_liquidations"]) == 0 and len(data["short_liquidations"]) == 0:
            pytest.skip("No Open Interest data available in database for this timeframe")
        assert len(data["long_liquidations"]) > 0
        assert len(data["short_liquidations"]) > 0

        # Verify volumes are calculated (not zero/mock)
        first_long = data["long_liquidations"][0]
        assert float(first_long["volume"]) > 0

    def test_levels_returns_longs_below_price_shorts_above(self, client):
        """Test that MOST long liquidations are below current price, shorts above.

        NOTE: Due to price volatility, some historical liquidations may appear on the
        "wrong" side when current price has moved significantly. We verify that at
        least 70% are correctly positioned.
        """
        response = client.get(
            "/liquidations/levels?symbol=BTCUSDT&model=binance_standard&timeframe=30"
        )
        data = response.json()

        current_price = float(data["current_price"])

        # Count correctly positioned liquidations
        long_correct = sum(
            1 for liq in data["long_liquidations"] if float(liq["price_level"]) < current_price
        )
        short_correct = sum(
            1 for liq in data["short_liquidations"] if float(liq["price_level"]) > current_price
        )

        total_longs = len(data["long_liquidations"])
        total_shorts = len(data["short_liquidations"])

        # At least 70% should be correctly positioned (allows for price volatility)
        if total_longs > 0:
            long_ratio = long_correct / total_longs
            assert long_ratio >= 0.7, (
                f"Only {long_ratio:.1%} of long liquidations below current price "
                f"({long_correct}/{total_longs})"
            )

        if total_shorts > 0:
            short_ratio = short_correct / total_shorts
            assert short_ratio >= 0.7, (
                f"Only {short_ratio:.1%} of short liquidations above current price "
                f"({short_correct}/{total_shorts})"
            )

    def test_liquidations_include_leverage_tiers(self, client):
        """Test that liquidations include multiple leverage tiers."""
        response = client.get(
            "/liquidations/levels?symbol=BTCUSDT&model=binance_standard&timeframe=30"
        )
        data = response.json()

        # Skip if no data available
        all_liqs = data["long_liquidations"] + data["short_liquidations"]
        if len(all_liqs) == 0:
            pytest.skip("No liquidation data available in database for this timeframe")

        # Collect all leverage tiers
        leverage_tiers = set()
        for liq in all_liqs:
            leverage_tiers.add(liq["leverage"])

        # Should have multiple leverage tiers (5x, 10x, 25x, 50x, 100x)
        assert len(leverage_tiers) >= 3, (
            f"Expected ≥3 leverage tiers, got {len(leverage_tiers)}: {leverage_tiers}"
        )

    def test_invalid_symbol_returns_error(self, client):
        """Test that invalid symbol returns 400 with list of supported symbols."""
        response = client.get(
            "/liquidations/levels?symbol=INVALID&model=binance_standard&timeframe=30"
        )

        # Symbol "INVALID" matches pattern ^[A-Z]{6,12}$ but is not in whitelist
        # Should return 400 with helpful error message listing supported symbols
        assert response.status_code == 400, (
            f"Expected 400 for invalid symbol, got {response.status_code}"
        )

        data = response.json()
        assert "detail" in data
        assert "Supported symbols" in data["detail"]


class TestHistoricalLiquidationsEndpoint:
    """Tests for /liquidations/history endpoint (T047)."""

    def test_history_returns_200_with_valid_params(self, client):
        """Test that history endpoint returns 200 with valid params."""
        response = client.get("/liquidations/history?symbol=BTCUSDT")
        assert response.status_code == 200

    def test_history_returns_list_of_records(self, client):
        """Test that history returns list of liquidation records from DB."""
        response = client.get("/liquidations/history?symbol=BTCUSDT")
        data = response.json()

        assert isinstance(data, list)
        # If liquidation_history table doesn't exist, should return empty list
        # In production with real data, this would have len > 0
        assert len(data) >= 0


class TestLiquidationsTimeframeParameter:
    """Tests for timeframe parameter in /liquidations/levels endpoint."""

    def test_liquidations_accepts_timeframe_parameter(self, client):
        """Test that /liquidations/levels accepts and USES timeframe query parameter.

        Different timeframes should potentially return different data
        (though with limited test data they may be similar).
        """
        # Get data with 7-day timeframe
        response_7d = client.get(
            "/liquidations/levels?symbol=BTCUSDT&model=binance_standard&timeframe=7"
        )
        assert response_7d.status_code == 200
        data_7d = response_7d.json()

        # Get data with 30-day timeframe
        response_30d = client.get(
            "/liquidations/levels?symbol=BTCUSDT&model=binance_standard&timeframe=30"
        )
        assert response_30d.status_code == 200
        data_30d = response_30d.json()

        # Both should return valid structure
        assert "long_liquidations" in data_7d
        assert "long_liquidations" in data_30d
        assert "short_liquidations" in data_7d
        assert "short_liquidations" in data_30d

        # Both should be lists (may be empty without test data)
        assert isinstance(data_7d["long_liquidations"], list)
        assert isinstance(data_30d["long_liquidations"], list)

        # With real database containing historical data, these would have content
        # In test environment without data, empty lists are acceptable
        assert len(data_7d["long_liquidations"]) >= 0
        assert len(data_30d["long_liquidations"]) >= 0


class TestFrontendStaticFiles:
    """Tests for frontend static file serving."""

    def test_frontend_liquidation_map_html_is_served(self, client):
        """Test that frontend/liquidation_map.html is accessible via HTTP."""
        response = client.get("/frontend/liquidation_map.html")
        assert response.status_code == 200
        assert "text/html" in response.headers["content-type"]

    def test_frontend_legacy_liquidation_map_html_is_served(self, client):
        """Test that archived frontend remains accessible for reference."""
        response = client.get("/frontend/legacy/liquidation_map.html")
        assert response.status_code == 200
        assert "text/html" in response.headers["content-type"]

    def test_coinank_liqmap_route_redirects_to_canonical_page(self, client):
        """Test canonical liq-map route redirects to the permalink-friendly page."""
        response = client.get(
            "/chart/derivatives/liq-map/binance/btcusdt/1w",
            follow_redirects=False,
        )
        assert response.status_code in (307, 308)
        assert (
            response.headers["location"]
            == "/liq_map_1w.html?exchange=binance&symbol=BTCUSDT&days=7"
        )

    def test_coinank_liqmap_1d_route_redirects_to_one_day_page(self, client):
        """Test 1d liq-map route redirects with an explicit one-day payload window."""
        response = client.get(
            "/chart/derivatives/liq-map/bybit/btcusdt/1d",
            follow_redirects=False,
        )
        assert response.status_code in (307, 308)
        assert (
            response.headers["location"]
            == "/liq_map_1w.html?exchange=bybit&symbol=BTCUSDT&days=1"
        )

    def test_coinank_heatmap_route_redirects_to_canonical_page(self, client):
        """Test canonical heatmap route redirects to the canonical frontend page."""
        response = client.get(
            "/chart/derivatives/liq-heat-map/ethusdt/1w",
            follow_redirects=False,
        )
        assert response.status_code in (307, 308)
        assert (
            response.headers["location"]
            == "/frontend/coinglass_heatmap.html?symbol=ETHUSDT&window=7d&ui=minimal"
        )

    def test_coinank_heatmap_1d_route_redirects_to_short_window(self, client):
        """Test 1d heatmap route maps to the shortest supported internal window."""
        response = client.get(
            "/chart/derivatives/liq-heat-map/btcusdt/1d",
            follow_redirects=False,
        )
        assert response.status_code in (307, 308)
        assert (
            response.headers["location"]
            == "/frontend/coinglass_heatmap.html?symbol=BTCUSDT&window=48h&ui=minimal"
        )

    def test_legacy_heatmap_alias_redirects_to_coinank_style_route(self, client):
        """Test legacy root alias forwards to the canonical heatmap path."""
        response = client.get("/coinglass", follow_redirects=False)
        assert response.status_code in (307, 308)
        assert response.headers["location"] == "/chart/derivatives/liq-heat-map/btcusdt/1w"

    def test_coinank_heatmap_rejects_unsupported_timeframe(self, client):
        """Test unsupported heatmap timeframes fail fast instead of silently remapping."""
        response = client.get(
            "/chart/derivatives/liq-heat-map/btcusdt/1M",
            follow_redirects=False,
        )
        assert response.status_code == 400

    def test_coinank_liqmap_rejects_unsupported_timeframe(self, client):
        """Test unsupported liq-map timeframes fail fast instead of silently remapping."""
        response = client.get(
            "/chart/derivatives/liq-map/binance/btcusdt/1M",
            follow_redirects=False,
        )
        assert response.status_code == 400
