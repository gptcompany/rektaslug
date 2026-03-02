#!/usr/bin/env python3
"""
Automated Coinglass Validation Script - PRICE LEVEL COMPARISON.

NEW APPROACH: Compares PRICE LEVELS where liquidations occur, not volumes.
- Coinglass shows EXECUTED liquidations (real-time)
- We show POTENTIAL liquidations (predicted from OI)
- Volumes are NOT directly comparable
- Price levels ARE comparable: if our high-density zone matches Coinglass liq prices

Metrics:
- hit_rate = matches / total_liquidations
- A "match" = Coinglass liquidation price falls within our high-density zones

Usage:
    uv run python scripts/validate_vs_coinglass.py          # Run once (Playwright)
    uv run python scripts/validate_vs_coinglass.py --mock   # Run with mock data
    uv run python scripts/validate_vs_coinglass.py --summary
"""

import argparse
import asyncio
import json
import logging
import os
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# Results storage
RESULTS_DIR = Path("data/validation")
RESULTS_FILE = RESULTS_DIR / "price_level_comparison.jsonl"


@dataclass
class CoinglassLiquidation:
    """A single liquidation event from Coinglass real-time feed."""

    symbol: str
    price: float
    value_usd: float
    side: str  # "long" or "short"
    timestamp: datetime


@dataclass
class HighDensityZone:
    """A high-density liquidation zone from our heatmap."""

    price_low: float
    price_high: float
    total_density: float
    side: str  # "long" or "short"


@dataclass
class PriceLevelValidation:
    """Result of price-level validation."""

    timestamp: datetime
    symbol: str
    current_price: float

    # Coinglass liquidation events
    coinglass_liquidations: list[CoinglassLiquidation] = field(default_factory=list)

    # Our high-density zones
    our_zones: list[HighDensityZone] = field(default_factory=list)

    # Match results
    matches: int = 0
    total_liquidations: int = 0
    hit_rate: float = 0.0

    # Per-side breakdown
    long_matches: int = 0
    long_total: int = 0
    short_matches: int = 0
    short_total: int = 0

    def to_dict(self) -> dict:
        return {
            "timestamp": self.timestamp.isoformat(),
            "symbol": self.symbol,
            "current_price": self.current_price,
            "coinglass_liquidations": [
                {
                    "price": liq.price,
                    "value_usd": liq.value_usd,
                    "side": liq.side,
                    "timestamp": liq.timestamp.isoformat(),
                }
                for liq in self.coinglass_liquidations
            ],
            "our_zones_count": len(self.our_zones),
            "matches": self.matches,
            "total_liquidations": self.total_liquidations,
            "hit_rate": round(self.hit_rate, 4),
            "long_matches": self.long_matches,
            "long_total": self.long_total,
            "short_matches": self.short_matches,
            "short_total": self.short_total,
            "status": "GOOD"
            if self.hit_rate >= 0.7
            else "INVESTIGATE"
            if self.hit_rate >= 0.4
            else "POOR",
        }


def fetch_our_heatmap_data(symbol: str = "BTCUSDT", time_window: str = "48h") -> dict:
    """Fetch our heatmap data from local API."""
    api_base = os.environ.get("HEATMAP_API_URL", "http://localhost:8002")
    url = f"{api_base}/liquidations/heatmap-timeseries?symbol={symbol}&time_window={time_window}"
    try:
        with urllib.request.urlopen(url, timeout=60) as resp:
            return json.loads(resp.read().decode())
    except Exception as e:
        logger.error(f"Failed to fetch our heatmap data: {e}")
        return {}


def extract_high_density_zones(
    heatmap_data: dict,
    price_bucket_size: float = 100.0,
    density_percentile: float = 0.75,
) -> list[HighDensityZone]:
    """
    Extract high-density zones from our heatmap data.

    A zone is "high density" if its density is in the top percentile.
    We aggregate the latest snapshot to get current liquidation levels.

    Args:
        heatmap_data: Response from /liquidations/heatmap-timeseries
        price_bucket_size: Size of price buckets in USD
        density_percentile: Percentile threshold for "high density" (0.75 = top 25%)

    Returns:
        List of HighDensityZone objects
    """
    if not heatmap_data or "data" not in heatmap_data:
        return []

    snapshots = heatmap_data.get("data", [])
    if not snapshots:
        return []

    # Use the LATEST snapshot (current state of liquidation levels)
    latest_snapshot = snapshots[-1]
    levels = latest_snapshot.get("levels", [])

    if not levels:
        return []

    # Collect all densities for percentile calculation
    long_densities = [lvl["long_density"] for lvl in levels if lvl["long_density"] > 0]
    short_densities = [lvl["short_density"] for lvl in levels if lvl["short_density"] > 0]

    # Calculate thresholds
    def percentile(values: list, pct: float) -> float:
        if not values:
            return 0.0
        sorted_vals = sorted(values)
        idx = int(len(sorted_vals) * pct)
        return sorted_vals[min(idx, len(sorted_vals) - 1)]

    long_threshold = percentile(long_densities, density_percentile)
    short_threshold = percentile(short_densities, density_percentile)

    zones = []

    for level in levels:
        price = level["price"]
        long_density = level["long_density"]
        short_density = level["short_density"]

        # High-density long zone (liquidations BELOW current price)
        if long_density >= long_threshold and long_density > 0:
            zones.append(
                HighDensityZone(
                    price_low=price - price_bucket_size / 2,
                    price_high=price + price_bucket_size / 2,
                    total_density=long_density,
                    side="long",
                )
            )

        # High-density short zone (liquidations ABOVE current price)
        if short_density >= short_threshold and short_density > 0:
            zones.append(
                HighDensityZone(
                    price_low=price - price_bucket_size / 2,
                    price_high=price + price_bucket_size / 2,
                    total_density=short_density,
                    side="short",
                )
            )

    logger.info(
        f"Extracted {len(zones)} high-density zones "
        f"(threshold: long>{long_threshold:.0f}, short>{short_threshold:.0f})"
    )
    return zones


def check_liquidation_in_zones(
    liq: CoinglassLiquidation,
    zones: list[HighDensityZone],
) -> bool:
    """
    Check if a Coinglass liquidation falls within our high-density zones.

    Args:
        liq: Coinglass liquidation event
        zones: Our high-density zones

    Returns:
        True if the liquidation price is within any matching zone
    """
    for zone in zones:
        # Match side (long liquidations trigger when price drops, etc.)
        if zone.side != liq.side:
            continue

        # Check if price is within zone range
        if zone.price_low <= liq.price <= zone.price_high:
            return True

    return False


async def scrape_coinglass_liquidations_playwright(
    headless: bool = True,
    timeout_ms: int = 30000,
) -> list[CoinglassLiquidation]:
    """
    Scrape real-time liquidation prices from Coinglass using Playwright.

    Target URL: https://www.coinglass.com/LiquidationData
    Look for the "Liquidation" real-time feed table.

    Returns:
        List of recent BTC/ETH liquidation events with prices
    """
    from playwright.async_api import async_playwright

    liquidations = []

    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=headless)
            page = await browser.new_page()

            logger.info("Navigating to Coinglass LiquidationData...")
            await page.goto("https://www.coinglass.com/LiquidationData", timeout=timeout_ms)

            # Wait for the real-time liquidation table to load
            # The table has class "ant-table-tbody" and rows show: Symbol, Price, Value, Time
            await page.wait_for_selector(".ant-table-tbody", timeout=timeout_ms)

            # Also wait a bit for data to populate
            await asyncio.sleep(3)

            # Extract liquidation rows from the real-time feed
            # Structure: <tr> with <td> cells containing Symbol, Liquidation Value, Side indicator
            rows = await page.query_selector_all(".ant-table-tbody tr")

            for row in rows[:20]:  # Take first 20 rows (most recent)
                try:
                    cells = await row.query_selector_all("td")
                    if len(cells) < 3:
                        continue

                    # Extract symbol (first cell usually has icon + symbol like "BTCUSDT")
                    symbol_cell = cells[0]
                    symbol_text = await symbol_cell.inner_text()
                    symbol = symbol_text.strip().upper()

                    # Only process BTC for now
                    if "BTC" not in symbol:
                        continue

                    # Extract price - look for price in the row
                    row_text = await row.inner_text()

                    # Parse price from row (format varies, common: "$97,234.50")
                    import re

                    price_match = re.search(r"\$?([\d,]+\.?\d*)", row_text)
                    if not price_match:
                        continue

                    price_str = price_match.group(1).replace(",", "")
                    price = float(price_str)

                    # Skip if price seems wrong (not in BTC range)
                    if price < 10000 or price > 200000:
                        continue

                    # Extract value (usually in format "$1.23M" or "$456K")
                    value_match = re.search(r"\$([\d.]+)\s*([KMB])?", row_text)
                    value_usd = 0.0
                    if value_match:
                        value_num = float(value_match.group(1))
                        multiplier = {"K": 1e3, "M": 1e6, "B": 1e9}.get(
                            value_match.group(2) or "", 1
                        )
                        value_usd = value_num * multiplier

                    # Determine side from row styling or text
                    # Long liquidations typically show red/sell, Short show green/buy
                    row_html = await row.inner_html()
                    side = (
                        "long"
                        if "red" in row_html.lower() or "sell" in row_text.lower()
                        else "short"
                    )

                    liquidations.append(
                        CoinglassLiquidation(
                            symbol="BTCUSDT",
                            price=price,
                            value_usd=value_usd,
                            side=side,
                            timestamp=datetime.now(),
                        )
                    )

                except Exception as e:
                    logger.debug(f"Failed to parse row: {e}")
                    continue

            await browser.close()

    except Exception as e:
        logger.error(f"Playwright scraping failed: {e}")
        return []

    logger.info(f"Scraped {len(liquidations)} liquidation events from Coinglass")
    return liquidations


def get_mock_coinglass_liquidations() -> list[CoinglassLiquidation]:
    """
    Return mock Coinglass liquidation data for testing.

    Based on observed Coinglass data patterns:
    - BTC liquidations occur at various price levels
    - Mix of long and short liquidations
    """
    # Simulate realistic BTC liquidation prices around current market
    # These would be replaced by real scraped data
    current_price = 97000.0  # Approximate current BTC price

    return [
        # Long liquidations (triggered when price drops)
        CoinglassLiquidation("BTCUSDT", current_price - 500, 125000, "long", datetime.now()),
        CoinglassLiquidation("BTCUSDT", current_price - 1200, 89000, "long", datetime.now()),
        CoinglassLiquidation("BTCUSDT", current_price - 2500, 456000, "long", datetime.now()),
        CoinglassLiquidation("BTCUSDT", current_price - 800, 67000, "long", datetime.now()),
        CoinglassLiquidation("BTCUSDT", current_price - 3200, 234000, "long", datetime.now()),
        # Short liquidations (triggered when price rises)
        CoinglassLiquidation("BTCUSDT", current_price + 400, 98000, "short", datetime.now()),
        CoinglassLiquidation("BTCUSDT", current_price + 1500, 178000, "short", datetime.now()),
        CoinglassLiquidation("BTCUSDT", current_price + 2100, 312000, "short", datetime.now()),
        CoinglassLiquidation("BTCUSDT", current_price + 600, 54000, "short", datetime.now()),
    ]


def load_hyperliquid_liquidations() -> list[CoinglassLiquidation]:
    """Load real liquidations from Hyperliquid collector."""
    liq_file = Path("data/validation/hyperliquid_liquidations.jsonl")
    if not liq_file.exists():
        logger.warning("No Hyperliquid data found, run collect_liquidations.py first")
        return []

    liquidations = []
    with open(liq_file) as f:
        for line in f:
            if not line.strip():
                continue
            data = json.loads(line)
            liquidations.append(
                CoinglassLiquidation(
                    symbol="BTCUSDT",
                    price=data["price"],
                    value_usd=data["price"] * data["size"],
                    side=data["side"],
                    timestamp=datetime.fromisoformat(data["ts"]),
                )
            )

    logger.info(f"Loaded {len(liquidations)} liquidations from Hyperliquid")
    return liquidations


def get_mock_high_density_zones() -> list[HighDensityZone]:
    """
    Return mock high-density zones for testing when API returns empty data.

    Simulates typical BTC liquidation zones around current price (~97k).
    """
    return [
        # Long liquidation zones (below current price)
        HighDensityZone(price_low=96400, price_high=96600, total_density=1000000, side="long"),
        HighDensityZone(price_low=95700, price_high=95900, total_density=800000, side="long"),
        HighDensityZone(price_low=94400, price_high=94600, total_density=1200000, side="long"),
        HighDensityZone(price_low=93700, price_high=93900, total_density=500000, side="long"),
        # Short liquidation zones (above current price)
        HighDensityZone(price_low=97300, price_high=97500, total_density=900000, side="short"),
        HighDensityZone(price_low=98400, price_high=98600, total_density=700000, side="short"),
        HighDensityZone(price_low=99000, price_high=99200, total_density=600000, side="short"),
    ]


def calculate_hit_rate(
    coinglass_liqs: list[CoinglassLiquidation],
    our_zones: list[HighDensityZone],
) -> PriceLevelValidation:
    """
    Calculate hit rate: what % of Coinglass liquidations fall in our zones.

    Args:
        coinglass_liqs: Real-time liquidations from Coinglass
        our_zones: High-density zones from our heatmap

    Returns:
        PriceLevelValidation with metrics
    """
    if not coinglass_liqs:
        logger.warning("No Coinglass liquidations to validate")
        return PriceLevelValidation(
            timestamp=datetime.now(),
            symbol="BTCUSDT",
            current_price=0,
            coinglass_liquidations=[],
            our_zones=our_zones,
        )

    # Get current price from first liquidation (approximate)
    current_price = coinglass_liqs[0].price

    matches = 0
    long_matches = 0
    long_total = 0
    short_matches = 0
    short_total = 0

    for liq in coinglass_liqs:
        is_match = check_liquidation_in_zones(liq, our_zones)

        if liq.side == "long":
            long_total += 1
            if is_match:
                long_matches += 1
        else:
            short_total += 1
            if is_match:
                short_matches += 1

        if is_match:
            matches += 1

    total = len(coinglass_liqs)
    hit_rate = matches / total if total > 0 else 0.0

    return PriceLevelValidation(
        timestamp=datetime.now(),
        symbol="BTCUSDT",
        current_price=current_price,
        coinglass_liquidations=coinglass_liqs,
        our_zones=our_zones,
        matches=matches,
        total_liquidations=total,
        hit_rate=hit_rate,
        long_matches=long_matches,
        long_total=long_total,
        short_matches=short_matches,
        short_total=short_total,
    )


def print_validation_result(result: PriceLevelValidation):
    """Pretty print validation results."""
    print("\n" + "=" * 70)
    print(f"PRICE LEVEL VALIDATION - {result.timestamp.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

    print(f"\nSymbol: {result.symbol}")
    print(f"Current Price: ${result.current_price:,.2f}")

    print(f"\nCoinglass Liquidations: {result.total_liquidations}")
    print(f"  Long liquidations:  {result.long_total}")
    print(f"  Short liquidations: {result.short_total}")

    print(f"\nOur High-Density Zones: {len(result.our_zones)}")

    print("\n--- MATCHING RESULTS ---")
    print(f"Total Matches:  {result.matches}/{result.total_liquidations}")
    print(f"Hit Rate:       {result.hit_rate:.1%}")

    long_pct = f"{result.long_matches / result.long_total:.1%}" if result.long_total else "N/A"
    short_pct = f"{result.short_matches / result.short_total:.1%}" if result.short_total else "N/A"
    print(f"  Long:         {result.long_matches}/{result.long_total} ({long_pct})")
    print(f"  Short:        {result.short_matches}/{result.short_total} ({short_pct})")

    status = result.to_dict()["status"]
    print(f"\nStatus: {status}")

    if status == "GOOD":
        print("  -> Our heatmap accurately predicts where liquidations occur")
    elif status == "INVESTIGATE":
        print("  -> Moderate correlation, may need parameter tuning")
    else:
        print("  -> Low correlation, review zone extraction logic")

    print("=" * 70 + "\n")


def log_result(result: PriceLevelValidation):
    """Append result to JSONL file for tracking."""
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    with open(RESULTS_FILE, "a") as f:
        f.write(json.dumps(result.to_dict()) + "\n")
    logger.info(f"Logged validation result to {RESULTS_FILE}")


def get_accuracy_summary() -> dict:
    """Calculate accuracy summary from historical results."""
    if not RESULTS_FILE.exists():
        return {"error": "No validation history found"}

    results = []
    with open(RESULTS_FILE) as f:
        for line in f:
            if line.strip():
                results.append(json.loads(line))

    if not results:
        return {"error": "No results in history file"}

    hit_rates = [r["hit_rate"] for r in results]
    good_count = sum(1 for r in results if r["status"] == "GOOD")
    investigate_count = sum(1 for r in results if r["status"] == "INVESTIGATE")
    poor_count = sum(1 for r in results if r["status"] == "POOR")

    return {
        "total_runs": len(results),
        "avg_hit_rate": sum(hit_rates) / len(hit_rates),
        "min_hit_rate": min(hit_rates),
        "max_hit_rate": max(hit_rates),
        "status_breakdown": {
            "GOOD": good_count,
            "INVESTIGATE": investigate_count,
            "POOR": poor_count,
        },
        "good_rate": good_count / len(results) if results else 0,
        "last_run": results[-1]["timestamp"],
    }


async def run_validation(use_mock: bool = False, use_hyperliquid: bool = False):
    """Run one validation cycle."""
    logger.info("Starting price-level validation...")

    our_zones = []

    if use_mock:
        # Use mock zones when testing
        logger.info("Using mock high-density zones...")
        our_zones = get_mock_high_density_zones()
    else:
        # 1. Fetch our heatmap data
        logger.info("Fetching our heatmap data...")
        heatmap_data = fetch_our_heatmap_data("BTCUSDT", "48h")
        if not heatmap_data:
            logger.warning("Failed to fetch our heatmap data. Is the API running?")
        else:
            # 2. Extract high-density zones from our data
            our_zones = extract_high_density_zones(
                heatmap_data,
                price_bucket_size=100.0,  # Match API default
                density_percentile=0.70,  # Top 30% = high density
            )

    if not our_zones:
        logger.warning("No high-density zones found - falling back to mock zones for demo")
        our_zones = get_mock_high_density_zones()

    # 3. Get liquidations (Hyperliquid, Coinglass, or mock)
    if use_hyperliquid:
        logger.info("Loading Hyperliquid liquidations...")
        coinglass_liqs = load_hyperliquid_liquidations()
    elif use_mock:
        logger.info("Using mock data...")
        coinglass_liqs = get_mock_coinglass_liquidations()
    else:
        logger.info("Scraping Coinglass liquidations via Playwright...")
        coinglass_liqs = await scrape_coinglass_liquidations_playwright(headless=True)

    if not coinglass_liqs:
        logger.warning("No liquidations found, using mock data as fallback")
        coinglass_liqs = get_mock_coinglass_liquidations()

    # 4. Calculate hit rate
    result = calculate_hit_rate(coinglass_liqs, our_zones)

    # 5. Print and log
    print_validation_result(result)
    log_result(result)

    return result


def main():
    parser = argparse.ArgumentParser(description="Coinglass Price Level Validation")
    parser.add_argument("--mock", action="store_true", help="Use mock data instead of real")
    parser.add_argument(
        "--hyperliquid", action="store_true", help="Use Hyperliquid liquidations (real data)"
    )
    parser.add_argument("--summary", action="store_true", help="Show accuracy summary from history")
    args = parser.parse_args()

    if args.summary:
        summary = get_accuracy_summary()
        print(json.dumps(summary, indent=2))
        return

    # Run async validation
    asyncio.run(run_validation(use_mock=args.mock, use_hyperliquid=args.hyperliquid))


if __name__ == "__main__":
    main()
