"""Binance Standard liquidation model with funding rate bias adjustment."""

import logging
from datetime import datetime
from decimal import Decimal
from typing import List, Optional

from src.models.funding.adjustment_config import AdjustmentConfigModel
from src.services.funding.complete_calculator import CompleteBiasCalculator

from .base import LiquidationLevel
from .binance_standard import BinanceStandardModel

logger = logging.getLogger(__name__)


class BinanceStandardBiasModel(BinanceStandardModel):
    """Binance Standard model with funding rate bias adjustment.

    Extends BinanceStandardModel to dynamically adjust long/short position
    distribution based on funding rate instead of naive 50/50 split.

    Feature: LIQHEAT-005
    Task: T023
    """

    def __init__(self, bias_config: Optional[AdjustmentConfigModel] = None):
        """Initialize with optional bias configuration.

        Args:
            bias_config: Configuration for bias adjustment. If None, uses defaults.
        """
        super().__init__()

        # Initialize bias configuration
        if bias_config is None:
            # Load default configuration
            from src.services.funding.adjustment_config import load_config

            bias_config = load_config()

        self.bias_config = bias_config

        # Initialize bias calculator if enabled
        if self.bias_config.enabled:
            self.bias_calculator = CompleteBiasCalculator(self.bias_config)
            logger.info(f"Bias adjustment ENABLED for {self.model_name}")
        else:
            self.bias_calculator = None
            logger.info(f"Bias adjustment DISABLED for {self.model_name}")

    @property
    def model_name(self) -> str:
        """Model identifier."""
        return "binance_standard_bias"

    async def get_bias_adjustment(
        self, symbol: str, open_interest: Decimal
    ) -> tuple[Decimal, Decimal]:
        """Get bias-adjusted long/short ratios.

        Args:
            symbol: Trading symbol (e.g., BTCUSDT)
            open_interest: Total open interest

        Returns:
            Tuple of (long_ratio, short_ratio) that sum to 1.0
        """
        if not self.bias_calculator:
            # Fallback to 50/50 if disabled
            return Decimal("0.5"), Decimal("0.5")

        try:
            # Get bias adjustment from funding rate
            adjustment = await self.bias_calculator.calculate_bias_adjustment(symbol, open_interest)

            logger.debug(
                f"Bias adjustment for {symbol}: "
                f"long={adjustment.long_ratio:.4f}, "
                f"short={adjustment.short_ratio:.4f}, "
                f"confidence={adjustment.confidence_score:.2f}"
            )

            return adjustment.long_ratio, adjustment.short_ratio

        except Exception as e:
            logger.error(f"Failed to get bias adjustment: {e}")
            # Fallback to neutral 50/50
            return Decimal("0.5"), Decimal("0.5")

    def calculate_liquidations(
        self,
        current_price: Decimal,
        open_interest: Decimal,
        symbol: str = "BTCUSDT",
        leverage_tiers: List[int] = None,
        num_bins: int = 50,
        large_trades=None,  # Optional DataFrame with real aggTrades data
    ) -> List[LiquidationLevel]:
        """Calculate liquidations with bias-adjusted position distribution.

        This method extends the parent class to apply funding rate bias
        adjustment to the long/short position distribution in MODE 2
        (synthetic binning).

        Args:
            current_price: Current market price
            open_interest: Total open interest
            symbol: Trading symbol
            leverage_tiers: List of leverage levels to calculate
            num_bins: Number of price bins for distribution
            large_trades: Optional real trade data

        Returns:
            List of LiquidationLevel objects with bias-adjusted volumes
        """
        # For MODE 1 (real trades), use parent implementation directly
        # Real trades already reflect market sentiment
        if large_trades is not None and not large_trades.empty:
            return super().calculate_liquidations(
                current_price, open_interest, symbol, leverage_tiers, num_bins, large_trades
            )

        # MODE 2: Synthetic binning with bias adjustment
        logger.info("MODE 2: Synthetic binning with funding rate bias adjustment")

        # Get bias-adjusted ratios synchronously (blocking call)
        import asyncio

        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

        long_ratio, short_ratio = loop.run_until_complete(
            self.get_bias_adjustment(symbol, open_interest)
        )

        # Prepare liquidation levels
        liquidations = []
        timestamp = datetime.now()
        leverage_tiers = leverage_tiers or [5, 10, 25, 50, 100]
        mmr = self._get_mmr(open_interest)

        import numpy as np

        for leverage in leverage_tiers:
            # Volume per leverage tier with BIAS ADJUSTMENT
            # Instead of /2 for 50/50, use actual ratios
            long_volume_per_tier = open_interest / len(leverage_tiers) * long_ratio
            short_volume_per_tier = open_interest / len(leverage_tiers) * short_ratio

            # LONG positions: Entry prices distributed ABOVE current price
            entry_range_long = np.linspace(
                float(current_price) * 1.01,  # 1% above
                float(current_price) * 1.15,  # 15% above
                num_bins,
            )

            # Gaussian distribution for realistic volume clustering
            entry_weights_long = np.exp(
                -0.5
                * ((entry_range_long - float(current_price) * 1.05) / (float(current_price) * 0.03))
                ** 2
            )
            entry_weights_long = entry_weights_long / entry_weights_long.sum()

            for entry_price, weight in zip(entry_range_long, entry_weights_long):
                liq_price = self._calculate_long_liquidation(
                    Decimal(str(entry_price)), leverage, mmr
                )
                liquidations.append(
                    LiquidationLevel(
                        timestamp=timestamp,
                        symbol=symbol,
                        price_level=liq_price,
                        liquidation_volume=long_volume_per_tier * Decimal(str(weight)),
                        leverage_tier=f"{leverage}x",
                        side="long",
                        confidence=self.confidence_score(),
                    )
                )

            # SHORT positions: Entry prices distributed slightly BELOW current price
            entry_range_short = np.linspace(
                float(current_price) * 0.953,  # 4.7% below
                float(current_price) * 0.99,  # 1% below
                num_bins,
            )

            # Gaussian distribution
            entry_weights_short = np.exp(
                -0.5
                * (
                    (entry_range_short - float(current_price) * 0.95)
                    / (float(current_price) * 0.03)
                )
                ** 2
            )
            entry_weights_short = entry_weights_short / entry_weights_short.sum()

            for entry_price, weight in zip(entry_range_short, entry_weights_short):
                liq_price = self._calculate_short_liquidation(
                    Decimal(str(entry_price)), leverage, mmr
                )
                liquidations.append(
                    LiquidationLevel(
                        timestamp=timestamp,
                        symbol=symbol,
                        price_level=liq_price,
                        liquidation_volume=short_volume_per_tier * Decimal(str(weight)),
                        leverage_tier=f"{leverage}x",
                        side="short",
                        confidence=self.confidence_score(),
                    )
                )

        logger.info(
            f"MODE 2 complete with bias adjustment: "
            f"{len(liquidations)} liquidations "
            f"(long: {len([liq for liq in liquidations if liq.side == 'long'])}, "
            f"short: {len([liq for liq in liquidations if liq.side == 'short'])})"
        )

        return liquidations
