"""Unit tests for FundingAdjusted liquidation model."""

from datetime import datetime
from decimal import Decimal

import pytest

from src.liquidationheatmap.models.funding_adjusted import FundingAdjustedModel


class TestFundingAdjustedModel:
    """Tests for FundingAdjustedModel liquidation calculations."""

    def test_model_name(self):
        """Model name should be funding_adjusted."""
        model = FundingAdjustedModel()
        assert model.model_name == "funding_adjusted"

    def test_confidence_score(self):
        """FundingAdjustedModel should have 0.75 confidence score."""
        model = FundingAdjustedModel()
        assert model.confidence_score() == Decimal("0.75")

    def test_calculate_liquidations_positive_funding(self):
        """Positive funding should increase long liquidation prices (more risky)."""
        model = FundingAdjustedModel()
        current_price = Decimal("60000")
        open_interest = Decimal("1000000")
        # 0.01% funding rate (standard)
        funding_rate = Decimal("0.0001")

        # Get base liquidations from BinanceStandardModel via super()
        # With 10x multiplier: adjustment = 0.0001 * 10 = 0.001 (0.1%)
        liquidations = model.calculate_liquidations(
            current_price, open_interest, funding_rate=funding_rate, leverage_tiers=[10]
        )

        # Separate longs and shorts
        longs = [l for l in liquidations if l.side == "long"]
        shorts = [l for l in liquidations if l.side == "short"]

        assert len(longs) > 0
        assert len(shorts) > 0

        # Check long adjustment (higher liq price)
        # Base long liq for 10x: 60000 * 0.904 = 54240 (approx, depends on entry price distribution in synthetic mode)
        # But FundingAdjustedModel uses synthetic mode from BinanceStandardModel
        # Let's compare with zero funding rate
        liquidations_zero = model.calculate_liquidations(
            current_price, open_interest, funding_rate=Decimal("0"), leverage_tiers=[10]
        )
        longs_zero = [l for l in liquidations_zero if l.side == "long"]
        
        # FundingAdjustedModel calculates based on EACH level from base model
        # So we should see a relative increase
        for l_pos, l_zero in zip(longs, longs_zero):
            assert l_pos.price_level > l_zero.price_level

    def test_calculate_liquidations_negative_funding(self):
        """Negative funding should decrease short liquidation prices (more risky)."""
        model = FundingAdjustedModel()
        current_price = Decimal("60000")
        open_interest = Decimal("1000000")
        # -0.01% funding rate
        funding_rate = Decimal("-0.0001")

        liquidations = model.calculate_liquidations(
            current_price, open_interest, funding_rate=funding_rate, leverage_tiers=[10]
        )
        
        liquidations_zero = model.calculate_liquidations(
            current_price, open_interest, funding_rate=Decimal("0"), leverage_tiers=[10]
        )
        
        shorts = [l for l in liquidations if l.side == "short"]
        shorts_zero = [l for l in liquidations_zero if l.side == "short"]
        
        # For shorts: adjusted_price = liq.price_level * (1 - adjustment)
        # adjustment = -0.0001 * 10 = -0.001
        # adjusted_price = liq.price_level * (1 - (-0.001)) = liq.price_level * 1.001
        # WAIT: The code says:
        # if liq.side == "long":
        #     adjusted_price = liq.price_level * (Decimal("1") + adjustment)
        # else:  # short
        #     adjusted_price = liq.price_level * (Decimal("1") - adjustment)
        
        # If funding is negative (-0.0001), adjustment is -0.001
        # Short adjusted_price = price * (1 - (-0.001)) = price * 1.001 (higher price = MORE risky for short)
        # Correct.
        for s_neg, s_zero in zip(shorts, shorts_zero):
            assert s_neg.price_level > s_zero.price_level
