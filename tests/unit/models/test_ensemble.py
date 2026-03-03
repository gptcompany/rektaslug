"""Unit tests for Ensemble liquidation model."""

from decimal import Decimal

import pytest

from src.liquidationheatmap.models.ensemble import EnsembleModel


class TestEnsembleModel:
    """Tests for EnsembleModel weighted aggregation."""

    def test_model_name(self):
        """Model name should be ensemble."""
        model = EnsembleModel()
        assert model.model_name == "ensemble"

    def test_confidence_score_is_085(self):
        """Default confidence score should be 0.85."""
        model = EnsembleModel()
        assert model.confidence_score() == Decimal("0.85")

    def test_adjust_weights(self):
        """Weights should be adjusted to sum to 1.0."""
        model = EnsembleModel()
        weights = model.get_weights()
        assert sum(weights.values()) == Decimal("1.0")
        assert "binance_standard" in weights
        assert "funding_adjusted" in weights

    def test_calculate_liquidations_aggregates_multiple_models(self):
        """Ensemble should combine results from multiple models."""
        model = EnsembleModel()
        current_price = Decimal("60000")
        open_interest = Decimal("1000000")

        liquidations = model.calculate_liquidations(
            current_price, open_interest, leverage_tiers=[10]
        )

        # Should have results (BinanceStandardModel always returns synthetic bins)
        assert len(liquidations) > 0
        
        # Verify we have both sides
        longs = [l for l in liquidations if l.side == "long"]
        shorts = [l for l in liquidations if l.side == "short"]
        
        assert len(longs) > 0
        assert len(shorts) > 0

    def test_disagreement_lowers_confidence(self):
        """Confidence should drop to 0.70 if models disagree significantly."""
        model = EnsembleModel()
        
        # Force a large disagreement by mocking model results
        from unittest.mock import MagicMock
        from src.liquidationheatmap.models.base import LiquidationLevel
        from datetime import datetime
        
        model.models["binance_standard"].calculate_liquidations = MagicMock(return_value=[
            LiquidationLevel(
                timestamp=datetime.now(),
                symbol="BTCUSDT",
                price_level=Decimal("50000"),
                liquidation_volume=Decimal("1000"),
                leverage_tier="10x",
                side="long",
                confidence=Decimal("0.95")
            )
        ])
        
        model.models["funding_adjusted"].calculate_liquidations = MagicMock(return_value=[
            LiquidationLevel(
                timestamp=datetime.now(),
                symbol="BTCUSDT",
                price_level=Decimal("40000"), # 20% difference
                liquidation_volume=Decimal("1000"),
                leverage_tier="10x",
                side="long",
                confidence=Decimal("0.75")
            )
        ])
        
        liquidations = model.calculate_liquidations(
            Decimal("60000"), Decimal("1000000"), leverage_tiers=[10]
        )
        
        # 20% difference is > 5% threshold, so confidence should be 0.70
        assert liquidations[0].confidence == Decimal("0.70")
