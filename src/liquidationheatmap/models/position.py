"""Position models for time-evolving liquidation heatmap.

Contains data structures for tracking individual liquidation levels,
heatmap cells, and snapshots over time.
"""

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Literal, Optional


@dataclass
class LiquidationLevel:
    """Represents a single liquidation level created from a position.

    Tracks the lifecycle of a position from creation to consumption (liquidation).
    """

    entry_price: Decimal  # Price where position was opened
    liq_price: Decimal  # Calculated liquidation price
    volume: Decimal  # Position size in quote currency (USDT)
    side: Literal["long", "short"]
    leverage: int  # Leverage tier (5, 10, 25, 50, 100)
    created_at: datetime  # When position was opened
    consumed_at: Optional[datetime] = None  # When liquidated (None if active)

    def is_active(self) -> bool:
        """Check if position is still open."""
        return self.consumed_at is None

    def __post_init__(self):
        """Validate fields after initialization."""
        if self.leverage not in (5, 10, 25, 50, 100):
            raise ValueError(f"Invalid leverage: {self.leverage}")
        if self.side not in ("long", "short"):
            raise ValueError(f"Invalid side: {self.side}")
        if self.volume <= 0:
            raise ValueError(f"Volume must be positive: {self.volume}")


@dataclass
class HeatmapCell:
    """Single cell in the heatmap matrix.

    Represents liquidation density at a specific price bucket.
    """

    price_bucket: Decimal  # Price level (rounded to bin size)
    long_density: Decimal = Decimal("0")  # Volume of active long liquidations
    short_density: Decimal = Decimal("0")  # Volume of active short liquidations

    @property
    def total_density(self) -> Decimal:
        """Total liquidation density at this price level."""
        return self.long_density + self.short_density


@dataclass
class HeatmapSnapshot:
    """Complete heatmap state at a single timestamp.

    Contains all liquidation levels active at this point in time,
    organized by price bucket.
    """

    timestamp: datetime
    symbol: str
    cells: dict[Decimal, HeatmapCell] = field(default_factory=dict)

    # Metadata
    total_long_volume: Decimal = Decimal("0")
    total_short_volume: Decimal = Decimal("0")
    positions_created: int = 0
    positions_consumed: int = 0

    def get_cell(self, price_bucket: Decimal) -> HeatmapCell:
        """Get or create cell for price bucket."""
        if price_bucket not in self.cells:
            self.cells[price_bucket] = HeatmapCell(price_bucket=price_bucket)
        return self.cells[price_bucket]

    def to_dict(self) -> dict:
        """Convert to API response format."""
        return {
            "timestamp": self.timestamp.isoformat(),
            "symbol": self.symbol,
            "levels": [
                {
                    "price": float(cell.price_bucket),
                    "long_density": float(cell.long_density),
                    "short_density": float(cell.short_density),
                }
                for cell in sorted(self.cells.values(), key=lambda c: c.price_bucket)
            ],
            "meta": {
                "total_long_volume": float(self.total_long_volume),
                "total_short_volume": float(self.total_short_volume),
                "positions_created": self.positions_created,
                "positions_consumed": self.positions_consumed,
            },
        }


def calculate_liq_price(
    entry_price: Decimal,
    leverage: int,
    side: str,
    mmr: Decimal = Decimal("0.004"),
) -> Decimal:
    """Calculate liquidation price using Binance formula.

    Long:  liq_price = entry_price * (1 - 1/leverage + mmr)
    Short: liq_price = entry_price * (1 + 1/leverage - mmr)

    Args:
        entry_price: Position entry price
        leverage: Leverage multiplier (e.g., 10 for 10x)
        side: "long" or "short"
        mmr: Maintenance margin rate (default 0.4%)

    Returns:
        Liquidation price
    """
    lev = Decimal(leverage)
    if side == "long":
        return entry_price * (1 - 1 / lev + mmr)
    else:  # short
        return entry_price * (1 + 1 / lev - mmr)
