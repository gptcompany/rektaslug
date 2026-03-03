#!/usr/bin/env python3
"""Quantify the residual gap between provider liquidation maps.

This script builds on top of `scripts/compare_provider_liquidations.py`,
reuses the same capture parsing/Coinglass decode path, and persists
normalization scenarios into the validation DuckDB so the remaining gap
between CoinAnk and CoinGlass becomes queryable over time.

Examples:
    python3 scripts/provider_gap_analysis.py

    python3 scripts/provider_gap_analysis.py \
        --manifest data/validation/raw_provider_api/20260303T174430Z/manifest.json \
        --persist-db
"""

from __future__ import annotations

import argparse
import json
import math
import sys
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts import compare_provider_liquidations as provider_compare
from src.liquidationheatmap.settings import get_settings

RAW_CAPTURE_ROOT = provider_compare.RAW_CAPTURE_ROOT
DEFAULT_OUTPUT_DIR = provider_compare.DEFAULT_OUTPUT_DIR
DEFAULT_ANALYSIS_DB_PATH = provider_compare.DEFAULT_COMPARISON_DB_PATH


@dataclass
class ProviderMapState:
    """Detailed price-map state for one provider."""

    provider: str
    source_url: str
    saved_file: str
    symbol: str | None
    exchange: str | None
    timeframe: str | None
    current_price: float | None
    total_map: dict[float, float]
    long_map: dict[float, float]
    short_map: dict[float, float]
    price_by_leverage: dict[int, dict[float, float]]
    leverage_totals: dict[int, float]
    notes: list[str]

    @property
    def bucket_count(self) -> int:
        return len(self.total_map)

    @property
    def total_value(self) -> float:
        return sum(self.total_map.values())

    @property
    def total_long(self) -> float:
        return sum(self.long_map.values())

    @property
    def total_short(self) -> float:
        return sum(self.short_map.values())

    @property
    def peak_long(self) -> float:
        return max(self.long_map.values(), default=0.0)

    @property
    def peak_short(self) -> float:
        return max(self.short_map.values(), default=0.0)

    @property
    def price_step_median(self) -> float | None:
        return provider_compare.median_step(list(self.total_map))

    def leverage_share_map(self) -> dict[int, float]:
        total = sum(self.leverage_totals.values())
        if total <= 0:
            return {}
        return {
            leverage: value / total
            for leverage, value in sorted(self.leverage_totals.items())
            if value > 0
        }


@dataclass
class ScenarioMetrics:
    """Comparison metrics for one normalization scenario."""

    scenario_name: str
    description: str
    left_provider: str
    right_provider: str
    left_bucket_count: int
    right_bucket_count: int
    left_total: float
    right_total: float
    total_ratio: float | None
    left_long_total: float
    right_long_total: float
    long_ratio: float | None
    left_short_total: float
    right_short_total: float
    short_ratio: float | None
    left_peak_long: float
    right_peak_long: float
    left_peak_short: float
    right_peak_short: float
    long_peak_ratio: float | None
    short_peak_ratio: float | None
    left_price_step: float | None
    right_price_step: float | None
    comparison_step: float | None
    shape_pearson: float | None
    shape_cosine: float | None
    distribution_overlap: float | None
    matched_bucket_ratio: float | None
    common_tiers: list[int]
    notes: list[str]

    def to_public_dict(self) -> dict[str, Any]:
        return {
            "scenario_name": self.scenario_name,
            "description": self.description,
            "providers": [self.left_provider, self.right_provider],
            "left_bucket_count": self.left_bucket_count,
            "right_bucket_count": self.right_bucket_count,
            "left_total": self.left_total,
            "right_total": self.right_total,
            "total_ratio": self.total_ratio,
            "left_long_total": self.left_long_total,
            "right_long_total": self.right_long_total,
            "long_ratio": self.long_ratio,
            "left_short_total": self.left_short_total,
            "right_short_total": self.right_short_total,
            "short_ratio": self.short_ratio,
            "left_peak_long": self.left_peak_long,
            "right_peak_long": self.right_peak_long,
            "left_peak_short": self.left_peak_short,
            "right_peak_short": self.right_peak_short,
            "long_peak_ratio": self.long_peak_ratio,
            "short_peak_ratio": self.short_peak_ratio,
            "left_price_step": self.left_price_step,
            "right_price_step": self.right_price_step,
            "comparison_step": self.comparison_step,
            "shape_pearson": self.shape_pearson,
            "shape_cosine": self.shape_cosine,
            "distribution_overlap": self.distribution_overlap,
            "matched_bucket_ratio": self.matched_bucket_ratio,
            "common_tiers": self.common_tiers,
            "notes": self.notes,
        }


def utc_timestamp_slug() -> str:
    """Return a filesystem-safe UTC timestamp."""
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--manifest",
        action="append",
        help="Manifest file or run directory. Can be passed multiple times.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Optional explicit JSON output path.",
    )
    parser.add_argument(
        "--persist-db",
        action="store_true",
        help="Persist scenario metrics into the validation DuckDB.",
    )
    parser.add_argument(
        "--db-path",
        type=Path,
        help="Override the DuckDB path. Defaults to the validation DuckDB.",
    )
    parser.add_argument(
        "--internal-api-base",
        help="Override the local API base used to fetch /liquidations/heatmap-timeseries.",
    )
    parser.add_argument(
        "--internal-time-window",
        help="Override the internal heatmap time_window (e.g. 48h, 7d, 30d, 90d, 180d, 1y).",
    )
    return parser.parse_args()


def default_output_path() -> Path:
    """Return the default output path under provider comparisons."""
    DEFAULT_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    return DEFAULT_OUTPUT_DIR / f"{utc_timestamp_slug()}_provider_gap_analysis.json"


def ratio(numerator: float, denominator: float) -> float | None:
    """Safe ratio helper."""
    if denominator == 0:
        return None
    return numerator / denominator


def rounded_map(values: dict[float, float]) -> dict[float, float]:
    """Drop tiny zero-ish values after arithmetic transforms."""
    result: dict[float, float] = {}
    for price, value in values.items():
        if value <= 0:
            continue
        rounded_price = round(price, 8)
        result[rounded_price] = result.get(rounded_price, 0.0) + value
    return result


def choose_capture(
    captures: list[provider_compare.CaptureFile],
    provider: str,
    url_paths: tuple[str, ...],
) -> provider_compare.CaptureFile:
    """Choose the latest preferred capture for one provider."""
    for capture in reversed(captures):
        if capture.provider != provider:
            continue
        lowered_path = urlparse(capture.source_url).path.lower()
        if any(lowered_path == candidate for candidate in url_paths):
            return capture
    raise SystemExit(
        f"Could not find {provider} capture matching any of: {', '.join(url_paths)}"
    )


def choose_internal_time_window(
    coinank_timeframe: str | None,
    coinglass_timeframe: str | None,
    explicit: str | None,
) -> str:
    """Choose the closest internal time_window for the vendor timeframe."""
    if explicit:
        return explicit

    mapping = {
        "1d": "48h",
        "1 day": "48h",
        "48h": "48h",
        "1w": "7d",
        "7 day": "7d",
        "7d": "7d",
        "1m": "30d",
        "1 month": "30d",
        "30d": "30d",
        "3m": "90d",
        "3 month": "90d",
        "90d": "90d",
        "6m": "180d",
        "6 month": "180d",
        "180d": "180d",
        "1y": "1y",
        "1 year": "1y",
        "365d": "1y",
    }
    for candidate in (coinglass_timeframe, coinank_timeframe):
        if not candidate:
            continue
        resolved = mapping.get(candidate.strip().lower())
        if resolved:
            return resolved
    return "48h"


def choose_internal_price_bin_size(
    coinank_state: ProviderMapState,
    coinglass_state: ProviderMapState,
) -> float:
    """Pick a practical internal price_bin_size anchored to vendor granularity."""
    candidates = [
        step
        for step in (coinank_state.price_step_median, coinglass_state.price_step_median)
        if step and step > 0
    ]
    if not candidates:
        return 100.0
    return max(1.0, min(1000.0, round(max(candidates), 2)))


def fetch_internal_heatmap_payload(
    *,
    api_base: str,
    symbol: str,
    time_window: str,
    price_bin_size: float,
) -> tuple[dict[str, Any], str]:
    """Fetch the current internal heatmap-timeseries payload from the local API."""
    params = urllib.parse.urlencode(
        {
            "symbol": symbol,
            "time_window": time_window,
            "price_bin_size": f"{price_bin_size:.8g}",
        }
    )
    source_url = f"{api_base.rstrip('/')}/liquidations/heatmap-timeseries?{params}"
    try:
        with urllib.request.urlopen(source_url, timeout=120) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except Exception as exc:
        raise SystemExit(f"Failed to fetch internal heatmap data: {exc}") from exc

    if not isinstance(payload, dict):
        raise SystemExit("Internal heatmap endpoint returned a non-object payload.")
    if "data" not in payload or "meta" not in payload:
        raise SystemExit("Internal heatmap payload is missing `data` or `meta`.")
    return payload, source_url


def extract_coinank_state(
    capture: provider_compare.CaptureFile,
) -> ProviderMapState:
    """Extract raw CoinAnk bucket/leverage state from getLiqMap."""
    root = capture.payload
    data = root.get("data")
    if not isinstance(data, dict):
        raise SystemExit("CoinAnk capture payload has no `data` object.")

    prices_raw = data.get("prices")
    if not isinstance(prices_raw, list) or not prices_raw:
        raise SystemExit("CoinAnk capture has no `prices` array.")

    leverage_keys = sorted(
        [
            key
            for key, value in data.items()
            if key.startswith("x")
            and key[1:].isdigit()
            and isinstance(value, list)
            and len(value) == len(prices_raw)
        ],
        key=lambda key: int(key[1:]),
    )
    if not leverage_keys:
        raise SystemExit("CoinAnk getLiqMap capture does not expose leverage ladders.")

    current_price = provider_compare.safe_float(data.get("lastPrice"))
    last_index_raw = data.get("lastIndex")
    last_index: int | None = None
    if isinstance(last_index_raw, (int, float)):
        candidate = float(last_index_raw)
        if math.isfinite(candidate) and candidate.is_integer():
            last_index = int(candidate)

    total_map: dict[float, float] = {}
    long_map: dict[float, float] = {}
    short_map: dict[float, float] = {}
    price_by_leverage: dict[int, dict[float, float]] = {}
    leverage_totals: dict[int, float] = {}

    for idx, raw_price in enumerate(prices_raw):
        price = provider_compare.safe_float(raw_price)
        if price is None:
            continue

        bucket_total = 0.0
        for leverage_key in leverage_keys:
            leverage = int(leverage_key[1:])
            value = provider_compare.safe_float(data[leverage_key][idx]) or 0.0
            if value <= 0:
                continue

            leverage_totals[leverage] = leverage_totals.get(leverage, 0.0) + value
            price_by_leverage.setdefault(leverage, {})
            price_by_leverage[leverage][price] = price_by_leverage[leverage].get(price, 0.0) + value
            bucket_total += value

        if bucket_total <= 0:
            continue

        total_map[price] = total_map.get(price, 0.0) + bucket_total
        is_long = False
        if last_index is not None:
            is_long = idx <= last_index
        elif current_price is not None:
            is_long = price <= current_price
        else:
            is_long = True

        target = long_map if is_long else short_map
        target[price] = target.get(price, 0.0) + bucket_total

    params = provider_compare.parse_query_params(capture.source_url)
    notes = [
        (
            "CoinAnk getLiqMap exposes price buckets plus leverage ladders "
            f"{', '.join(leverage_keys)}."
        ),
        "Long/short buckets are split with CoinAnk `lastIndex` when present.",
    ]
    if last_index is None:
        notes[-1] = "Long/short buckets are split around `lastPrice` because `lastIndex` was absent."
    notes.append("The public CoinAnk payload starts at x25 and omits x5/x10.")

    return ProviderMapState(
        provider=capture.provider,
        source_url=capture.source_url,
        saved_file=str(capture.saved_file),
        symbol=data.get("symbol") or params.get("symbol"),
        exchange=params.get("exchange"),
        timeframe=params.get("interval"),
        current_price=current_price,
        total_map=rounded_map(total_map),
        long_map=rounded_map(long_map),
        short_map=rounded_map(short_map),
        price_by_leverage={
            leverage: rounded_map(level_map)
            for leverage, level_map in sorted(price_by_leverage.items())
        },
        leverage_totals=dict(sorted(leverage_totals.items())),
        notes=notes,
    )


def extract_coinglass_state(
    capture: provider_compare.CaptureFile,
) -> ProviderMapState:
    """Extract raw Coinglass bucket/leverage state from decoded liqMapV2."""
    decoded_payload, decode_notes = provider_compare.decode_coinglass_json_payload(capture)
    if not isinstance(decoded_payload, dict):
        hint = decode_notes[0] if decode_notes else "Decoded payload unavailable."
        raise SystemExit(f"Could not decode Coinglass liqMap capture. {hint}")

    liq_map_v2 = decoded_payload.get("liqMapV2")
    if not isinstance(liq_map_v2, dict) or not liq_map_v2:
        raise SystemExit("Decoded Coinglass liqMap capture has no `liqMapV2` object.")

    params = provider_compare.parse_query_params(capture.source_url)
    symbol, exchange = provider_compare.normalize_coinglass_symbol(params.get("symbol"))
    instrument = decoded_payload.get("instrument")
    if isinstance(instrument, dict):
        symbol = (
            instrument.get("instrumentId")
            or instrument.get("baseAsset")
            or symbol
        )
        exchange = instrument.get("exName") or exchange

    current_price = provider_compare.safe_float(decoded_payload.get("lastPrice"))

    total_map: dict[float, float] = {}
    long_map: dict[float, float] = {}
    short_map: dict[float, float] = {}
    price_by_leverage: dict[int, dict[float, float]] = {}
    leverage_totals: dict[int, float] = {}
    cluster_count = 0

    for raw_bucket_price, raw_rows in liq_map_v2.items():
        bucket_price = provider_compare.safe_float(raw_bucket_price)
        if bucket_price is None or not isinstance(raw_rows, list):
            continue

        bucket_total = 0.0
        for row in raw_rows:
            if not isinstance(row, list) or len(row) < 4:
                continue
            value = provider_compare.safe_float(row[1]) or 0.0
            if value <= 0:
                continue

            leverage = provider_compare.safe_float(row[2])
            leverage_int = int(leverage) if leverage is not None and leverage.is_integer() else None
            if leverage_int is not None:
                leverage_totals[leverage_int] = leverage_totals.get(leverage_int, 0.0) + value
                price_by_leverage.setdefault(leverage_int, {})
                price_by_leverage[leverage_int][bucket_price] = (
                    price_by_leverage[leverage_int].get(bucket_price, 0.0) + value
                )

            bucket_total += value
            cluster_count += 1

        if bucket_total <= 0:
            continue

        total_map[bucket_price] = total_map.get(bucket_price, 0.0) + bucket_total
        is_long = (
            current_price is None or bucket_price <= current_price
        )
        target = long_map if is_long else short_map
        target[bucket_price] = target.get(bucket_price, 0.0) + bucket_total

    interval = params.get("interval")
    limit = params.get("limit")
    timeframe = None
    if interval and limit:
        liqmap_window_labels = {
            ("1", "1500"): "1 day",
            ("5", "2000"): "7 day",
            ("30", "1440"): "1 month",
            ("90d", "1440"): "3 month",
            ("180d", "1440"): "6 month",
            ("365d", "1440"): "1 year",
        }
        timeframe = liqmap_window_labels.get((interval, limit), f"{interval}_x{limit}")
    elif interval:
        timeframe = interval

    notes = list(decode_notes)
    notes.append(
        (
            "Coinglass liqMapV2 groups clusters under top-level price buckets; "
            f"this capture contains {len(total_map)} active buckets and {cluster_count} clusters."
        )
    )
    notes.append("Long/short buckets are split around decoded `lastPrice`.")

    return ProviderMapState(
        provider=capture.provider,
        source_url=capture.source_url,
        saved_file=str(capture.saved_file),
        symbol=symbol,
        exchange=exchange,
        timeframe=timeframe,
        current_price=current_price,
        total_map=rounded_map(total_map),
        long_map=rounded_map(long_map),
        short_map=rounded_map(short_map),
        price_by_leverage={
            leverage: rounded_map(level_map)
            for leverage, level_map in sorted(price_by_leverage.items())
        },
        leverage_totals=dict(sorted(leverage_totals.items())),
        notes=notes,
    )


def extract_internal_state(
    payload: dict[str, Any],
    *,
    source_url: str,
) -> ProviderMapState:
    """Extract the latest internal heatmap snapshot into the provider map schema."""
    meta = payload.get("meta")
    snapshots = payload.get("data")
    if not isinstance(meta, dict) or not isinstance(snapshots, list):
        raise SystemExit("Internal heatmap payload is missing usable `meta`/`data`.")
    if not snapshots:
        raise SystemExit("Internal heatmap payload has no snapshots.")

    latest_snapshot = snapshots[-1]
    if not isinstance(latest_snapshot, dict):
        raise SystemExit("Internal heatmap latest snapshot is invalid.")
    levels = latest_snapshot.get("levels")
    if not isinstance(levels, list):
        raise SystemExit("Internal heatmap latest snapshot has no `levels` array.")

    total_map: dict[float, float] = {}
    long_map: dict[float, float] = {}
    short_map: dict[float, float] = {}

    for level in levels:
        if not isinstance(level, dict):
            continue
        price = provider_compare.safe_float(level.get("price"))
        if price is None:
            continue
        long_value = provider_compare.safe_float(level.get("long_density")) or 0.0
        short_value = provider_compare.safe_float(level.get("short_density")) or 0.0
        if long_value > 0:
            long_map[price] = long_map.get(price, 0.0) + long_value
        if short_value > 0:
            short_map[price] = short_map.get(price, 0.0) + short_value
        total_value = long_value + short_value
        if total_value > 0:
            total_map[price] = total_map.get(price, 0.0) + total_value

    if not total_map:
        raise SystemExit("Internal heatmap latest snapshot has no active price levels.")

    notes = [
        "Internal state uses the latest /liquidations/heatmap-timeseries snapshot.",
        "long_density and short_density are active predicted liquidation densities, not vendor-reported executed liquidations.",
    ]

    return ProviderMapState(
        provider="internal",
        source_url=source_url,
        saved_file="live_api",
        symbol=meta.get("symbol"),
        exchange="internal",
        timeframe=meta.get("interval"),
        current_price=None,
        total_map=rounded_map(total_map),
        long_map=rounded_map(long_map),
        short_map=rounded_map(short_map),
        price_by_leverage={},
        leverage_totals={},
        notes=notes,
    )


def clone_state(
    state: ProviderMapState,
    *,
    total_map: dict[float, float],
    long_map: dict[float, float],
    short_map: dict[float, float],
    price_by_leverage: dict[int, dict[float, float]] | None = None,
    leverage_totals: dict[int, float] | None = None,
    note: str,
) -> ProviderMapState:
    """Create a derived state while preserving metadata."""
    new_notes = list(state.notes)
    new_notes.append(note)
    return ProviderMapState(
        provider=state.provider,
        source_url=state.source_url,
        saved_file=state.saved_file,
        symbol=state.symbol,
        exchange=state.exchange,
        timeframe=state.timeframe,
        current_price=state.current_price,
        total_map=rounded_map(total_map),
        long_map=rounded_map(long_map),
        short_map=rounded_map(short_map),
        price_by_leverage=(
            {
                leverage: rounded_map(level_map)
                for leverage, level_map in sorted(price_by_leverage.items())
            }
            if price_by_leverage is not None
            else {
                leverage: dict(level_map)
                for leverage, level_map in state.price_by_leverage.items()
            }
        ),
        leverage_totals=(
            dict(sorted(leverage_totals.items()))
            if leverage_totals is not None
            else dict(sorted(state.leverage_totals.items()))
        ),
        notes=new_notes,
    )


def filter_state_to_common_tiers(
    state: ProviderMapState,
    allowed_tiers: set[int],
) -> ProviderMapState:
    """Restrict a provider state to a subset of leverage tiers."""
    if not allowed_tiers:
        raise SystemExit("Common-tier normalization was requested with no overlapping tiers.")

    long_prices = set(state.long_map)
    total_map: dict[float, float] = {}
    long_map: dict[float, float] = {}
    short_map: dict[float, float] = {}
    price_by_leverage: dict[int, dict[float, float]] = {}
    leverage_totals: dict[int, float] = {}

    for leverage, level_map in state.price_by_leverage.items():
        if leverage not in allowed_tiers:
            continue
        for price, value in level_map.items():
            if value <= 0:
                continue
            total_map[price] = total_map.get(price, 0.0) + value
            if price in long_prices:
                long_map[price] = long_map.get(price, 0.0) + value
            else:
                short_map[price] = short_map.get(price, 0.0) + value
            price_by_leverage.setdefault(leverage, {})
            price_by_leverage[leverage][price] = price_by_leverage[leverage].get(price, 0.0) + value
            leverage_totals[leverage] = leverage_totals.get(leverage, 0.0) + value

    return clone_state(
        state,
        total_map=total_map,
        long_map=long_map,
        short_map=short_map,
        price_by_leverage=price_by_leverage,
        leverage_totals=leverage_totals,
        note=(
            "Restricted to the leverage tiers common to CoinAnk and Coinglass: "
            f"{', '.join(str(tier) for tier in sorted(allowed_tiers))}."
        ),
    )


def align_bin(price: float, step: float, anchor: float) -> float:
    """Map one price into a stable anchored bin."""
    if step <= 0:
        return round(price, 8)
    position = math.floor(((price - anchor) / step) + 1e-9)
    return round(anchor + (position * step), 8)


def rebin_map(values: dict[float, float], step: float, anchor: float) -> dict[float, float]:
    """Re-bin a price map onto a coarser anchored grid."""
    if step <= 0:
        return dict(values)
    rebinned: dict[float, float] = {}
    for price, value in values.items():
        if value <= 0:
            continue
        bucket = align_bin(price, step, anchor)
        rebinned[bucket] = rebinned.get(bucket, 0.0) + value
    return rounded_map(rebinned)


def rebin_state(
    state: ProviderMapState,
    step: float,
    anchor: float,
) -> ProviderMapState:
    """Re-bin total/side/leverage maps onto a shared grid."""
    price_by_leverage = {
        leverage: rebin_map(level_map, step, anchor)
        for leverage, level_map in state.price_by_leverage.items()
    }
    leverage_totals = {
        leverage: sum(level_map.values())
        for leverage, level_map in price_by_leverage.items()
    }
    return clone_state(
        state,
        total_map=rebin_map(state.total_map, step, anchor),
        long_map=rebin_map(state.long_map, step, anchor),
        short_map=rebin_map(state.short_map, step, anchor),
        price_by_leverage=price_by_leverage,
        leverage_totals=leverage_totals,
        note=f"Re-binned onto a shared anchored grid with step {step:.8g}.",
    )


def aligned_maps(
    left_map: dict[float, float],
    right_map: dict[float, float],
    *,
    step: float | None = None,
) -> tuple[dict[float, float], dict[float, float], float | None]:
    """Align two price maps onto a shared grid."""
    if not left_map and not right_map:
        return {}, {}, step

    left_step = provider_compare.median_step(list(left_map))
    right_step = provider_compare.median_step(list(right_map))
    if step is None:
        candidate_steps = [candidate for candidate in (left_step, right_step) if candidate]
        step = max(candidate_steps) if candidate_steps else None

    all_prices = list(left_map) + list(right_map)
    anchor = min(all_prices) if all_prices else 0.0
    normalized_left = rebin_map(left_map, step, anchor) if step else dict(left_map)
    normalized_right = rebin_map(right_map, step, anchor) if step else dict(right_map)

    return normalized_left, normalized_right, step


def aligned_vectors(
    left_map: dict[float, float],
    right_map: dict[float, float],
    *,
    step: float | None = None,
) -> tuple[list[float], list[float], float | None]:
    """Align two price maps onto a shared grid and return comparable vectors."""
    normalized_left, normalized_right, used_step = aligned_maps(
        left_map,
        right_map,
        step=step,
    )
    keys = sorted(set(normalized_left) | set(normalized_right))
    return (
        [normalized_left.get(price, 0.0) for price in keys],
        [normalized_right.get(price, 0.0) for price in keys],
        used_step,
    )


def pearson_correlation(left: list[float], right: list[float]) -> float | None:
    """Compute Pearson correlation without NumPy."""
    if len(left) != len(right) or len(left) < 2:
        return None

    left_mean = sum(left) / len(left)
    right_mean = sum(right) / len(right)
    left_var = sum((value - left_mean) ** 2 for value in left)
    right_var = sum((value - right_mean) ** 2 for value in right)
    if left_var <= 0 or right_var <= 0:
        return None

    covariance = sum(
        (left_value - left_mean) * (right_value - right_mean)
        for left_value, right_value in zip(left, right)
    )
    return covariance / math.sqrt(left_var * right_var)


def cosine_similarity(left: list[float], right: list[float]) -> float | None:
    """Compute cosine similarity for two vectors."""
    if len(left) != len(right) or not left:
        return None

    numerator = sum(left_value * right_value for left_value, right_value in zip(left, right))
    left_norm = math.sqrt(sum(value * value for value in left))
    right_norm = math.sqrt(sum(value * value for value in right))
    if left_norm <= 0 or right_norm <= 0:
        return None
    return numerator / (left_norm * right_norm)


def distribution_overlap(left: list[float], right: list[float]) -> float | None:
    """Return overlap of two normalized distributions, in [0, 1]."""
    if len(left) != len(right) or not left:
        return None

    left_total = sum(left)
    right_total = sum(right)
    if left_total <= 0 or right_total <= 0:
        return None

    return sum(
        min(left_value / left_total, right_value / right_total)
        for left_value, right_value in zip(left, right)
    )


def matched_bucket_ratio(
    left_map: dict[float, float],
    right_map: dict[float, float],
    step: float | None,
) -> float | None:
    """Return the left/right ratio on buckets that remain active on both sides."""
    normalized_left, normalized_right, _ = aligned_maps(left_map, right_map, step=step)
    if not normalized_left or not normalized_right:
        return None

    shared_keys = [
        price
        for price in sorted(set(normalized_left) & set(normalized_right))
        if normalized_left.get(price, 0.0) > 0 and normalized_right.get(price, 0.0) > 0
    ]
    if not shared_keys:
        return None

    return ratio(
        sum(normalized_left[price] for price in shared_keys),
        sum(normalized_right[price] for price in shared_keys),
    )


def build_scenario_metrics(
    scenario_name: str,
    description: str,
    left: ProviderMapState,
    right: ProviderMapState,
    *,
    common_tiers: list[int] | None = None,
    notes: list[str] | None = None,
) -> ScenarioMetrics:
    """Build one scenario result from two provider states."""
    left_vector, right_vector, comparison_step = aligned_vectors(left.total_map, right.total_map)
    return ScenarioMetrics(
        scenario_name=scenario_name,
        description=description,
        left_provider=left.provider,
        right_provider=right.provider,
        left_bucket_count=left.bucket_count,
        right_bucket_count=right.bucket_count,
        left_total=left.total_value,
        right_total=right.total_value,
        total_ratio=ratio(left.total_value, right.total_value),
        left_long_total=left.total_long,
        right_long_total=right.total_long,
        long_ratio=ratio(left.total_long, right.total_long),
        left_short_total=left.total_short,
        right_short_total=right.total_short,
        short_ratio=ratio(left.total_short, right.total_short),
        left_peak_long=left.peak_long,
        right_peak_long=right.peak_long,
        left_peak_short=left.peak_short,
        right_peak_short=right.peak_short,
        long_peak_ratio=ratio(left.peak_long, right.peak_long),
        short_peak_ratio=ratio(left.peak_short, right.peak_short),
        left_price_step=left.price_step_median,
        right_price_step=right.price_step_median,
        comparison_step=comparison_step,
        shape_pearson=pearson_correlation(left_vector, right_vector),
        shape_cosine=cosine_similarity(left_vector, right_vector),
        distribution_overlap=distribution_overlap(left_vector, right_vector),
        matched_bucket_ratio=matched_bucket_ratio(left.total_map, right.total_map, comparison_step),
        common_tiers=sorted(common_tiers or []),
        notes=notes or [],
    )


def build_summary_findings(
    raw_metrics: ScenarioMetrics,
    common_tier_metrics: ScenarioMetrics,
    rebinned_metrics: ScenarioMetrics,
    combined_metrics: ScenarioMetrics,
) -> list[str]:
    """Generate a concise narrative from scenario deltas."""
    findings: list[str] = []
    if raw_metrics.total_ratio is not None and common_tier_metrics.total_ratio is not None:
        reduction = 1 - (common_tier_metrics.total_ratio / raw_metrics.total_ratio)
        findings.append(
            (
                "Restricting CoinAnk to the leverage tiers shared with Coinglass reduces the "
                f"left/right total ratio from {raw_metrics.total_ratio:.4f}x to "
                f"{common_tier_metrics.total_ratio:.4f}x "
                f"({reduction * 100:.1f}% of the original gap removed)."
            )
        )
    if raw_metrics.total_ratio is not None and rebinned_metrics.total_ratio is not None:
        findings.append(
            (
                "Re-binning CoinAnk onto the Coinglass price step changes shape but not scale: "
                f"raw ratio {raw_metrics.total_ratio:.4f}x vs rebinned "
                f"{rebinned_metrics.total_ratio:.4f}x."
            )
        )
    if combined_metrics.total_ratio is not None:
        findings.append(
            (
                "After both common-tier filtering and shared-grid rebinning, the residual "
                f"total gap is still {combined_metrics.total_ratio:.4f}x, which points to "
                "provider-side scaling/persistence differences beyond leverage coverage and bin size."
            )
        )
    if combined_metrics.shape_cosine is not None and combined_metrics.distribution_overlap is not None:
        findings.append(
            (
                "On a shared coarse grid the maps are directionally similar but not identical "
                f"(cosine {combined_metrics.shape_cosine:.4f}, overlap "
                f"{combined_metrics.distribution_overlap:.4f})."
            )
        )
    return findings


def ensure_gap_tables(conn) -> None:
    """Create gap-analysis tables if they do not exist."""
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS provider_gap_analysis_runs (
            run_id VARCHAR PRIMARY KEY,
            created_at TIMESTAMP,
            report_path VARCHAR,
            manifests_json VARCHAR,
            notes_json VARCHAR
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS provider_gap_analysis_scenarios (
            run_id VARCHAR,
            scenario_name VARCHAR,
            left_provider VARCHAR,
            right_provider VARCHAR,
            left_bucket_count INTEGER,
            right_bucket_count INTEGER,
            left_total DOUBLE,
            right_total DOUBLE,
            total_ratio DOUBLE,
            left_long_total DOUBLE,
            right_long_total DOUBLE,
            long_ratio DOUBLE,
            left_short_total DOUBLE,
            right_short_total DOUBLE,
            short_ratio DOUBLE,
            left_peak_long DOUBLE,
            right_peak_long DOUBLE,
            left_peak_short DOUBLE,
            right_peak_short DOUBLE,
            long_peak_ratio DOUBLE,
            short_peak_ratio DOUBLE,
            left_price_step DOUBLE,
            right_price_step DOUBLE,
            comparison_step DOUBLE,
            shape_pearson DOUBLE,
            shape_cosine DOUBLE,
            distribution_overlap DOUBLE,
            matched_bucket_ratio DOUBLE,
            common_tiers_json VARCHAR,
            notes_json VARCHAR,
            PRIMARY KEY (run_id, scenario_name)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS provider_gap_analysis_leverage (
            run_id VARCHAR,
            provider VARCHAR,
            leverage INTEGER,
            total_value DOUBLE,
            share_ratio DOUBLE,
            PRIMARY KEY (run_id, provider, leverage)
        )
        """
    )


def persist_report_to_duckdb(
    report: dict[str, Any],
    output_path: Path,
    db_path: Path,
) -> None:
    """Persist gap analysis into the validation DuckDB."""
    try:
        import duckdb
    except ImportError as exc:
        raise RuntimeError("duckdb is required for --persist-db") from exc

    db_path.parent.mkdir(parents=True, exist_ok=True)
    run_id = output_path.stem

    conn = duckdb.connect(str(db_path))
    try:
        ensure_gap_tables(conn)

        conn.execute(
            """
            INSERT OR REPLACE INTO provider_gap_analysis_runs
            (run_id, created_at, report_path, manifests_json, notes_json)
            VALUES (?, ?, ?, ?, ?)
            """,
            [
                run_id,
                report["timestamp_utc"],
                str(output_path),
                json.dumps(report.get("manifests", []), ensure_ascii=True),
                json.dumps(report.get("findings", []), ensure_ascii=True),
            ],
        )

        conn.execute("DELETE FROM provider_gap_analysis_scenarios WHERE run_id = ?", [run_id])
        for scenario in report.get("scenarios", []):
            providers = scenario.get("providers", [])
            if len(providers) != 2:
                continue
            conn.execute(
                """
                INSERT INTO provider_gap_analysis_scenarios
                (
                    run_id, scenario_name, left_provider, right_provider,
                    left_bucket_count, right_bucket_count, left_total, right_total,
                    total_ratio, left_long_total, right_long_total, long_ratio,
                    left_short_total, right_short_total, short_ratio, left_peak_long,
                    right_peak_long, left_peak_short, right_peak_short, long_peak_ratio,
                    short_peak_ratio, left_price_step, right_price_step, comparison_step,
                    shape_pearson, shape_cosine, distribution_overlap, matched_bucket_ratio,
                    common_tiers_json, notes_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    run_id,
                    scenario.get("scenario_name"),
                    providers[0],
                    providers[1],
                    scenario.get("left_bucket_count"),
                    scenario.get("right_bucket_count"),
                    scenario.get("left_total"),
                    scenario.get("right_total"),
                    scenario.get("total_ratio"),
                    scenario.get("left_long_total"),
                    scenario.get("right_long_total"),
                    scenario.get("long_ratio"),
                    scenario.get("left_short_total"),
                    scenario.get("right_short_total"),
                    scenario.get("short_ratio"),
                    scenario.get("left_peak_long"),
                    scenario.get("right_peak_long"),
                    scenario.get("left_peak_short"),
                    scenario.get("right_peak_short"),
                    scenario.get("long_peak_ratio"),
                    scenario.get("short_peak_ratio"),
                    scenario.get("left_price_step"),
                    scenario.get("right_price_step"),
                    scenario.get("comparison_step"),
                    scenario.get("shape_pearson"),
                    scenario.get("shape_cosine"),
                    scenario.get("distribution_overlap"),
                    scenario.get("matched_bucket_ratio"),
                    json.dumps(scenario.get("common_tiers", []), ensure_ascii=True),
                    json.dumps(scenario.get("notes", []), ensure_ascii=True),
                ],
            )

        conn.execute("DELETE FROM provider_gap_analysis_leverage WHERE run_id = ?", [run_id])
        for provider, leverage_rows in report.get("leverage_composition", {}).items():
            for row in leverage_rows:
                conn.execute(
                    """
                    INSERT INTO provider_gap_analysis_leverage
                    (run_id, provider, leverage, total_value, share_ratio)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    [
                        run_id,
                        provider,
                        row.get("leverage"),
                        row.get("total_value"),
                        row.get("share_ratio"),
                    ],
                )
    finally:
        conn.close()


def build_report(
    manifest_paths: list[Path],
    coinank_state: ProviderMapState,
    coinglass_state: ProviderMapState,
    internal_state: ProviderMapState | None,
    scenarios: list[ScenarioMetrics],
    common_tiers: list[int],
) -> dict[str, Any]:
    """Build the final JSON report."""
    leverage_composition = {
        state.provider: [
            {
                "leverage": leverage,
                "total_value": state.leverage_totals[leverage],
                "share_ratio": state.leverage_share_map().get(leverage),
            }
            for leverage in sorted(state.leverage_totals)
        ]
        for state in (coinank_state, coinglass_state)
    }
    if internal_state is not None:
        leverage_composition[internal_state.provider] = []

    raw_metrics = scenarios[0]
    common_tier_metrics = scenarios[1]
    rebinned_metrics = scenarios[2]
    combined_metrics = scenarios[3]

    providers = {
        coinank_state.provider: {
            "source_url": coinank_state.source_url,
            "saved_file": coinank_state.saved_file,
            "symbol": coinank_state.symbol,
            "exchange": coinank_state.exchange,
            "timeframe": coinank_state.timeframe,
            "bucket_count": coinank_state.bucket_count,
            "total_value": coinank_state.total_value,
            "total_long": coinank_state.total_long,
            "total_short": coinank_state.total_short,
            "peak_long": coinank_state.peak_long,
            "peak_short": coinank_state.peak_short,
            "current_price": coinank_state.current_price,
            "price_step_median": coinank_state.price_step_median,
            "notes": coinank_state.notes,
        },
        coinglass_state.provider: {
            "source_url": coinglass_state.source_url,
            "saved_file": coinglass_state.saved_file,
            "symbol": coinglass_state.symbol,
            "exchange": coinglass_state.exchange,
            "timeframe": coinglass_state.timeframe,
            "bucket_count": coinglass_state.bucket_count,
            "total_value": coinglass_state.total_value,
            "total_long": coinglass_state.total_long,
            "total_short": coinglass_state.total_short,
            "peak_long": coinglass_state.peak_long,
            "peak_short": coinglass_state.peak_short,
            "current_price": coinglass_state.current_price,
            "price_step_median": coinglass_state.price_step_median,
            "notes": coinglass_state.notes,
        },
    }
    if internal_state is not None:
        providers[internal_state.provider] = {
            "source_url": internal_state.source_url,
            "saved_file": internal_state.saved_file,
            "symbol": internal_state.symbol,
            "exchange": internal_state.exchange,
            "timeframe": internal_state.timeframe,
            "bucket_count": internal_state.bucket_count,
            "total_value": internal_state.total_value,
            "total_long": internal_state.total_long,
            "total_short": internal_state.total_short,
            "peak_long": internal_state.peak_long,
            "peak_short": internal_state.peak_short,
            "current_price": internal_state.current_price,
            "price_step_median": internal_state.price_step_median,
            "notes": internal_state.notes,
        }

    return {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "manifests": [str(path.resolve()) for path in manifest_paths],
        "providers": providers,
        "common_leverage_tiers": common_tiers,
        "leverage_composition": leverage_composition,
        "scenarios": [scenario.to_public_dict() for scenario in scenarios],
        "findings": build_summary_findings(
            raw_metrics=raw_metrics,
            common_tier_metrics=common_tier_metrics,
            rebinned_metrics=rebinned_metrics,
            combined_metrics=combined_metrics,
        ),
        "recommendation": {
            "most_defensible_basis": "coinank_common_tiers_rebinned",
            "why": (
                "It aligns leverage coverage first, then aligns price granularity, which "
                "removes the two largest structural mismatches without inventing a new scale factor."
            ),
            "residual_ratio": combined_metrics.total_ratio,
            "residual_shape_cosine": combined_metrics.shape_cosine,
            "residual_overlap": combined_metrics.distribution_overlap,
        },
        "notes": [
            "This analysis uses the exact CoinAnk getLiqMap and Coinglass liqMap captures referenced by the input manifest.",
            "The residual gap after common-tier filtering and shared-grid rebinning is the strongest estimate of provider-side scaling/persistence differences.",
            "CoinAnk and Coinglass do not expose the same leverage ladder coverage in their public/private map payloads, so raw totals are not directly comparable.",
        ],
    }


def resolve_db_path(explicit_path: Path | None) -> Path:
    """Resolve the DuckDB path used for gap-analysis persistence."""
    if explicit_path is not None:
        return explicit_path
    return DEFAULT_ANALYSIS_DB_PATH


def main() -> int:
    """CLI entrypoint."""
    args = parse_args()
    manifest_paths = provider_compare.resolve_manifest_paths(args.manifest)
    captures = provider_compare.load_capture_files(manifest_paths)

    coinank_capture = choose_capture(
        captures,
        provider="coinank",
        url_paths=("/api/liqmap/getliqmap",),
    )
    coinglass_capture = choose_capture(
        captures,
        provider="coinglass",
        url_paths=("/api/index/v5/liqmap", "/api/index/5/liqmap"),
    )

    coinank_state = extract_coinank_state(coinank_capture)
    coinglass_state = extract_coinglass_state(coinglass_capture)
    settings = get_settings()
    internal_time_window = choose_internal_time_window(
        coinank_state.timeframe,
        coinglass_state.timeframe,
        args.internal_time_window,
    )
    internal_payload, internal_source_url = fetch_internal_heatmap_payload(
        api_base=args.internal_api_base or settings.api_url,
        symbol=coinank_state.symbol or coinglass_state.symbol or "BTCUSDT",
        time_window=internal_time_window,
        price_bin_size=choose_internal_price_bin_size(coinank_state, coinglass_state),
    )
    internal_state = extract_internal_state(
        internal_payload,
        source_url=internal_source_url,
    )

    common_tiers = sorted(
        set(coinank_state.leverage_totals) & set(coinglass_state.leverage_totals)
    )
    if not common_tiers:
        raise SystemExit("CoinAnk and Coinglass have no overlapping leverage tiers in these captures.")

    comparison_step = coinglass_state.price_step_median
    if comparison_step is None:
        raise SystemExit("Coinglass comparison step is unavailable; cannot build rebinned scenarios.")
    all_prices = list(coinank_state.total_map) + list(coinglass_state.total_map)
    anchor = min(all_prices) if all_prices else 0.0

    coinank_common = filter_state_to_common_tiers(coinank_state, set(common_tiers))
    coinank_rebinned = rebin_state(coinank_state, comparison_step, anchor)
    coinank_common_rebinned = rebin_state(coinank_common, comparison_step, anchor)

    scenarios = [
        build_scenario_metrics(
            "raw",
            "Direct comparison of the captured provider maps without normalization.",
            coinank_state,
            coinglass_state,
            common_tiers=common_tiers,
            notes=[
                "This preserves each provider's native leverage coverage and native price grid.",
            ],
        ),
        build_scenario_metrics(
            "coinank_common_tiers",
            "CoinAnk restricted to the leverage tiers shared with Coinglass.",
            coinank_common,
            coinglass_state,
            common_tiers=common_tiers,
            notes=[
                "This isolates how much of the raw gap is explained by leverage-tier coverage alone.",
            ],
        ),
        build_scenario_metrics(
            "coinank_rebinned_to_coinglass_step",
            "CoinAnk re-binned onto the Coinglass active price step.",
            coinank_rebinned,
            coinglass_state,
            common_tiers=common_tiers,
            notes=[
                "This isolates shape changes caused by price granularity without changing leverage coverage.",
            ],
        ),
        build_scenario_metrics(
            "coinank_common_tiers_rebinned",
            "CoinAnk restricted to common tiers, then re-binned onto the Coinglass active price step.",
            coinank_common_rebinned,
            coinglass_state,
            common_tiers=common_tiers,
            notes=[
                "This is the best apples-to-apples comparison available from the current payloads.",
            ],
        ),
        build_scenario_metrics(
            "internal_vs_coinank",
            "Internal latest heatmap snapshot versus CoinAnk price-map.",
            internal_state,
            coinank_state,
            notes=[
                (
                    "Internal uses the latest heatmap-timeseries snapshot. "
                    "Shape metrics are aligned on a shared coarse grid; totals stay native."
                ),
            ],
        ),
        build_scenario_metrics(
            "internal_vs_coinglass",
            "Internal latest heatmap snapshot versus Coinglass price-map.",
            internal_state,
            coinglass_state,
            common_tiers=common_tiers,
            notes=[
                (
                    "Internal uses the latest heatmap-timeseries snapshot. "
                    "This is the direct benchmark path for our model vs Coinglass."
                ),
            ],
        ),
    ]

    report = build_report(
        manifest_paths=manifest_paths,
        coinank_state=coinank_state,
        coinglass_state=coinglass_state,
        internal_state=internal_state,
        scenarios=scenarios,
        common_tiers=common_tiers,
    )

    output_path = args.output.resolve() if args.output else default_output_path()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")

    if args.persist_db:
        persist_report_to_duckdb(
            report=report,
            output_path=output_path,
            db_path=resolve_db_path(args.db_path),
        )

    print(output_path)
    if args.persist_db:
        print(f"duckdb: {resolve_db_path(args.db_path)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
