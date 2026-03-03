"""Unit tests for BinanceStandardBiasModel."""

from decimal import Decimal
from unittest.mock import MagicMock, patch
from datetime import datetime

import pytest
import pandas as pd

from src.liquidationheatmap.models.binance_standard_bias import BinanceStandardBiasModel


class TestBinanceStandardBiasModel:
    """Tests for bias-adjusted liquidation calculations."""

    @patch('src.services.funding.adjustment_config.load_config')
    def test_init_disabled(self, mock_load_config):
        """Should initialize with bias calculator disabled."""
        mock_config = MagicMock()
        mock_config.enabled = False
        mock_load_config.return_value = mock_config
        
        model = BinanceStandardBiasModel()
        assert model.bias_calculator is None
        assert model.model_name == "binance_standard_bias"

    @patch('src.services.funding.adjustment_config.load_config')
    @patch('src.liquidationheatmap.models.binance_standard_bias.CompleteBiasCalculator')
    def test_init_enabled(self, mock_calc_class, mock_load_config):
        """Should initialize with bias calculator enabled."""
        mock_config = MagicMock()
        mock_config.enabled = True
        # Provide real values to avoid MagicMock comparison errors in CompleteBiasCalculator/BiasCalculator
        mock_config.sensitivity = 50.0
        mock_config.max_adjustment = 0.20
        mock_config.outlier_cap = 0.10
        mock_load_config.return_value = mock_config
        
        model = BinanceStandardBiasModel()
        assert model.bias_calculator is not None
        mock_calc_class.assert_called_once_with(mock_config)

    @pytest.mark.asyncio
    async def test_get_bias_adjustment_disabled(self):
        """Should return 50/50 if bias calculator is disabled."""
        with patch('src.services.funding.adjustment_config.load_config') as mock_load:
            mock_config = MagicMock()
            mock_config.enabled = False
            mock_config.sensitivity = 50.0
            mock_config.max_adjustment = 0.20
            mock_config.outlier_cap = 0.10
            mock_load.return_value = mock_config
            
            model = BinanceStandardBiasModel()
            long_ratio, short_ratio = await model.get_bias_adjustment("BTCUSDT", Decimal("1000"))
            
            assert long_ratio == Decimal("0.5")
            assert short_ratio == Decimal("0.5")

    @pytest.mark.asyncio
    async def test_get_bias_adjustment_error_fallback(self):
        """Should return 50/50 fallback on calculation error."""
        with patch('src.services.funding.adjustment_config.load_config') as mock_load:
            mock_config = MagicMock()
            mock_config.enabled = True
            mock_config.sensitivity = 50.0
            mock_config.max_adjustment = 0.20
            mock_config.outlier_cap = 0.10
            mock_load.return_value = mock_config
            
            with patch('src.liquidationheatmap.models.binance_standard_bias.CompleteBiasCalculator') as mock_calc_class:
                mock_calc = MagicMock()
                mock_calc.calculate_bias_adjustment.side_effect = Exception("error")
                mock_calc_class.return_value = mock_calc
                
                model = BinanceStandardBiasModel()
                long_ratio, short_ratio = await model.get_bias_adjustment("BTCUSDT", Decimal("1000"))
                
                assert long_ratio == Decimal("0.5")
                assert short_ratio == Decimal("0.5")

    def test_calculate_liquidations_real_trades_skips_bias(self):
        """Mode 1 (real trades) should skip bias adjustment (handled by parent)."""
        with patch('src.services.funding.adjustment_config.load_config') as mock_load:
            mock_config = MagicMock()
            mock_config.enabled = True
            mock_config.sensitivity = 50.0
            mock_config.max_adjustment = 0.20
            mock_config.outlier_cap = 0.10
            mock_load.return_value = mock_config
            
            model = BinanceStandardBiasModel()
            
            # Create mock aggTrades data
            trades = pd.DataFrame([
                {"price": 60000.0, "gross_value": 10000.0, "side": "buy", "timestamp": 12345}
            ])
            
            # This should call BinanceStandardModel.calculate_liquidations
            liquidations = model.calculate_liquidations(
                Decimal("60000"), Decimal("1000000"), large_trades=trades, leverage_tiers=[10]
            )
            
            assert len(liquidations) > 0

    def test_calculate_liquidations_mode2_with_bias(self):
        """Mode 2 (synthetic) should apply bias ratios to volumes."""
        with patch('src.services.funding.adjustment_config.load_config') as mock_load:
            mock_config = MagicMock()
            mock_config.enabled = True
            mock_config.sensitivity = 50.0
            mock_config.max_adjustment = 0.20
            mock_config.outlier_cap = 0.10
            mock_load.return_value = mock_config
            
            with patch('src.liquidationheatmap.models.binance_standard_bias.CompleteBiasCalculator') as mock_calc_class:
                # Mock bias calculation result (e.g. 70% long bias)
                mock_adj = MagicMock()
                mock_adj.long_ratio = Decimal("0.7")
                mock_adj.short_ratio = Decimal("0.3")
                
                mock_calc = MagicMock()
                # Mock the async method to return our adjustment
                mock_calc.calculate_bias_adjustment.return_value = mock_adj
                mock_calc_class.return_value = mock_calc
                
                # Mock get_event_loop to handle run_until_complete
                with patch('asyncio.get_event_loop') as mock_loop:
                    mock_loop.return_value.run_until_complete.return_value = (Decimal("0.7"), Decimal("0.3"))
                    
                    model = BinanceStandardBiasModel()
                    
                    # Run synthetic calculation
                    liquidations = model.calculate_liquidations(
                        Decimal("60000"), Decimal("1000000"), leverage_tiers=[10], num_bins=10
                    )
                    
                    long_vol = sum(l.liquidation_volume for l in liquidations if l.side == "long")
                    short_vol = sum(l.liquidation_volume for l in liquidations if l.side == "short")
                    
                    # 70/30 ratio should be reflected in volumes
                    total_vol = long_vol + short_vol
                    assert abs(long_vol / total_vol - Decimal("0.7")) < Decimal("0.01")
                    assert abs(short_vol / total_vol - Decimal("0.3")) < Decimal("0.01")
