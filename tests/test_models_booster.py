"""Extra tests for models to increase coverage."""

from decimal import Decimal
from datetime import datetime, timezone, timedelta
import pytest
import pandas as pd

from src.liquidationheatmap.models.binance_standard import BinanceStandardModel
from src.liquidationheatmap.models.binance_standard_bias import BinanceStandardBiasModel
from src.liquidationheatmap.models.ensemble import EnsembleModel
from src.liquidationheatmap.models.position import HeatmapSnapshot, HeatmapCell, LiquidationLevel, calculate_liq_price
from src.liquidationheatmap.models.time_evolving_heatmap import (
    calculate_time_evolving_heatmap, should_liquidate, process_candle, remove_proportionally
)

def test_binance_standard_bias_model():
    model = BinanceStandardBiasModel()
    # Should work like the base model but with bias
    liqs = model.calculate_liquidations(Decimal("50000"), Decimal("1000000"))
    assert len(liqs) > 0
    assert model.model_name == "binance_standard_bias"

def test_binance_standard_model_full():
    model = BinanceStandardModel()
    trades = pd.DataFrame({
        "price": [50000.0, 51000.0],
        "gross_value": [1000000.0, 500000.0],
        "side": ["buy", "sell"]
    })
    liqs = model.calculate_liquidations(Decimal("50500.0"), Decimal("10000000"), large_trades=trades)
    assert len(liqs) > 0

def test_ensemble_model_full():
    model = EnsembleModel()
    liqs = model.calculate_liquidations(Decimal("50000"), Decimal("1000000"))
    assert len(liqs) > 0

def test_heatmap_snapshot_details():
    now = datetime.now(timezone.utc)
    snap = HeatmapSnapshot(timestamp=now, symbol="BTCUSDT")
    cell = snap.get_cell(Decimal("50000"))
    cell.long_density += Decimal("100")
    snap.total_long_volume += Decimal("100")
    assert snap.to_dict()["symbol"] == "BTCUSDT"

def test_time_evolving_heatmap_full_cycle():
    from dataclasses import dataclass
    @dataclass
    class MockCandle:
        open_time: datetime
        open: Decimal
        high: Decimal
        low: Decimal
        close: Decimal
        volume: Decimal

    now = datetime.now(timezone.utc)
    weights = [(100, Decimal("1.0"))]
    candles = [
        MockCandle(now, Decimal("50000"), Decimal("50100"), Decimal("49900"), Decimal("50050"), Decimal("100")),
        MockCandle(now + timedelta(minutes=15), Decimal("50050"), Decimal("50100"), Decimal("49000"), Decimal("49500"), Decimal("100")),
        MockCandle(now + timedelta(minutes=30), Decimal("49500"), Decimal("50500"), Decimal("49400"), Decimal("50100"), Decimal("100")),
        MockCandle(now + timedelta(minutes=45), Decimal("50100"), Decimal("50200"), Decimal("50000"), Decimal("50150"), Decimal("100")),
    ]
    oi_deltas = [Decimal("1000000"), Decimal("0"), Decimal("500000"), Decimal("-100000")]
    snapshots = calculate_time_evolving_heatmap(candles=candles, oi_deltas=oi_deltas, symbol="BTCUSDT", leverage_weights=weights)
    assert len(snapshots) == 4

def test_process_candle_direct_extended():
    from dataclasses import dataclass
    @dataclass
    class MockCandle:
        open_time: datetime
        open: Decimal
        high: Decimal
        low: Decimal
        close: Decimal
        volume: Decimal
    
    now = datetime.now(timezone.utc)
    active = {}
    
    candle_short = MockCandle(now, Decimal("50000"), Decimal("50100"), Decimal("49000"), Decimal("49500"), Decimal("100"))
    consumed, created = process_candle(candle_short, Decimal("1000000"), active)
    assert any(p.side == "short" for p in created)
    
    spike_candle = MockCandle(now + timedelta(minutes=15), Decimal("49500"), Decimal("60000"), Decimal("49000"), Decimal("55000"), Decimal("100"))
    consumed, created = process_candle(spike_candle, Decimal("0"), active)
    assert len(consumed) > 0
    assert len(active) == 0

def test_remove_proportionally_edge_cases():
    active = {}
    remove_proportionally(active, Decimal("1000"))
    assert active == {}
    
    now = datetime.now(timezone.utc)
    pos = LiquidationLevel(Decimal("50000"), Decimal("45000"), Decimal("10"), "long", 10, now)
    active[Decimal("45000")] = [pos]
    
    remove_proportionally(active, Decimal("100"))
    assert Decimal("45000") not in active
