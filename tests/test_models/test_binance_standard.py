"""Tests for Binance Standard liquidation model."""

from decimal import Decimal

from src.liquidationheatmap.models.binance_standard import BinanceStandardModel


class TestBinanceStandardModel:
    """Tests for BinanceStandardModel liquidation calculations."""

    def test_long_10x_liquidation_at_90_percent_of_entry(self):
        """Test that 10x long position liquidates at ~90.4% of entry price.

        Formula: long_liq = entry * (1 - 1/leverage + mmr)
        For 10x with 0.4% MMR: entry * (1 - 0.1 + 0.004) = entry * 0.904
        """
        model = BinanceStandardModel()

        entry_price = Decimal("67000.00")
        open_interest = Decimal("40000.00")  # <50k USDT → MMR 0.4%
        leverage = 10

        # Get MMR for this position size
        mmr = model._get_mmr(open_interest)  # Should be 0.004 (0.4%)

        # Calculate liquidation price
        liq_price = model._calculate_long_liquidation(entry_price, leverage, mmr)

        # Should liquidate at ~90.4% (with MMR adjustment)
        expected_liq = entry_price * Decimal("0.904")
        assert abs(liq_price - expected_liq) < Decimal("1.00")  # ±$1 tolerance

    def test_short_100x_liquidation_at_101_percent_of_entry(self):
        """Test that 100x short position liquidates at ~100.6% of entry price.

        Formula: short_liq = entry * (1 + 1/leverage - mmr)
        For 100x with 0.4% MMR: entry * (1 + 0.01 - 0.004) = entry * 1.006
        """
        model = BinanceStandardModel()

        entry_price = Decimal("67000.00")
        open_interest = Decimal("40000.00")  # <50k USDT → MMR 0.4%
        leverage = 100

        # Get MMR for this position size
        mmr = model._get_mmr(open_interest)  # Should be 0.004 (0.4%)

        # Calculate liquidation price
        liq_price = model._calculate_short_liquidation(entry_price, leverage, mmr)

        # Should liquidate at ~100.6% (with MMR adjustment)
        expected_liq = entry_price * Decimal("1.006")
        assert abs(liq_price - expected_liq) < Decimal("1.00")  # ±$1 tolerance

    def test_mmr_tier_changes_with_position_size(self):
        """Test that MMR changes based on position size (Open Interest).

        - OI < 50k → MMR 0.4%
        - OI 1M-10M → MMR 2.5%
        """
        model = BinanceStandardModel()

        current_price = Decimal("67000.00")

        # Small position: OI < 50k → MMR 0.4%
        liq_small = model.calculate_liquidations(
            current_price, Decimal("40000"), leverage_tiers=[10], num_bins=1
        )[0]  # First = long

        # Large position: OI 5M → MMR 2.5%
        liq_large = model.calculate_liquidations(
            current_price, Decimal("5000000"), leverage_tiers=[10], num_bins=1
        )[0]  # First = long

        # Large position should have different liquidation price (higher MMR = closer to entry)
        assert liq_small.price_level != liq_large.price_level
        # Higher MMR → higher liquidation price for long (less risky)
        assert liq_large.price_level > liq_small.price_level

    def test_confidence_score_is_095(self):
        """Test that BinanceStandardModel has 0.95 confidence (highest)."""
        model = BinanceStandardModel()
        assert model.confidence_score() == Decimal("0.95")

    def test_calculate_liquidations_real_trades(self):
        """Test Mode 1: Processing real trade data."""
        import pandas as pd
        model = BinanceStandardModel()
        
        # Create mock aggTrades data
        trades = pd.DataFrame([
            {"price": 60000.0, "gross_value": 10000.0, "side": "buy", "timestamp": 12345},
            {"price": 60000.0, "gross_value": 5000.0, "side": "sell", "timestamp": 12346}
        ])
        
        current_price = Decimal("60000")
        open_interest = Decimal("1000000")
        
        # Run calculation with real trades
        liquidations = model.calculate_liquidations(
            current_price, open_interest, large_trades=trades, leverage_tiers=[10]
        )
        
        # Should have results for both buy and sell trades
        assert len(liquidations) > 0
        
        # Separate results
        longs = [l for l in liquidations if l.side == "long"]
        shorts = [l for l in liquidations if l.side == "short"]
        
        # Long entries (buy) should result in liquidations BELOW 60000
        for l in longs:
            assert l.price_level < Decimal("60000")
            
        # Short entries (sell) should result in liquidations ABOVE 60000
        for s in shorts:
            assert s.price_level > Decimal("60000")
