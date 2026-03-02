#!/usr/bin/env python3
"""Capture side-by-side validation screenshots for our canonical liq-map vs Coinank.

Pipeline:
1. Ensure local FastAPI is running
2. Screenshot the local Coinank-style liq-map route
3. Screenshot Coinank ``liq-map/binance/btcusdt/1w``
4. Save both files to ``data/validation/liqmap/`` with a shared timestamp
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import signal
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.error import URLError
from urllib.request import urlopen

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.liquidationheatmap.settings import get_settings

sys.path.insert(0, str(Path(__file__).resolve().parent))
from coinank_screenshot import capture_coinank_liqmap

_SETTINGS = get_settings()

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = _SETTINGS.port
DEFAULT_SYMBOL = "BTCUSDT"
DEFAULT_MODEL = "openinterest"
DEFAULT_TIMEFRAME = 7

ROUTE_TIMEFRAME_BY_DAYS = {
    1: "1d",
    7: "1w",
}


def http_get_json(url: str, timeout: float = 3.0) -> dict[str, Any]:
    with urlopen(url, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def is_server_healthy(api_base: str) -> bool:
    try:
        with urlopen(f"{api_base}/health", timeout=1.5) as resp:
            return resp.status == 200
    except Exception:
        return False


def preflight_liqmap_api(
    api_base: str,
    symbol: str,
    model: str,
    timeframe: int,
) -> dict[str, Any]:
    url = f"{api_base}/liquidations/levels?symbol={symbol}&model={model}&timeframe={timeframe}"
    try:
        payload = http_get_json(url, timeout=15.0)
    except Exception as exc:
        return {"ok": False, "error": str(exc), "url": url}

    long_liqs = payload.get("long_liquidations", [])
    short_liqs = payload.get("short_liquidations", [])
    return {
        "ok": True,
        "url": url,
        "long_count": len(long_liqs),
        "short_count": len(short_liqs),
        "current_price": payload.get("current_price"),
    }


def fetch_liqmap_payload(
    api_base: str,
    symbol: str,
    model: str,
    timeframe: int,
) -> dict[str, Any] | None:
    """Fetch the complete /liquidations/levels JSON payload.

    Returns the full response dict, or None on error.
    """
    url = f"{api_base}/liquidations/levels?symbol={symbol}&model={model}&timeframe={timeframe}"
    try:
        return http_get_json(url, timeout=15.0)
    except Exception:
        return None


def compute_validation_metrics(payload: dict[str, Any] | None) -> dict[str, Any]:
    """Compute style-independent validation metrics from a levels payload.

    Returns a dict with: price_range, volume_totals, leverage_distribution,
    top_levels, and cumulative_shape.  Returns {"error": ...} if payload is None.
    """
    if payload is None:
        return {"error": "payload is None"}

    current_price = float(payload.get("current_price", 0))
    longs = payload.get("long_liquidations", [])
    shorts = payload.get("short_liquidations", [])

    all_prices = [float(entry["price_level"]) for entry in longs] + [
        float(entry["price_level"]) for entry in shorts
    ]
    long_total = sum(float(entry["volume"]) for entry in longs)
    short_total = sum(float(entry["volume"]) for entry in shorts)

    # --- price_range ---
    if all_prices:
        min_price = min(all_prices)
        max_price = max(all_prices)
        below = sum(1 for p in all_prices if p < current_price)
        pct_below = below / len(all_prices) * 100.0
        pct_above = 100.0 - pct_below
    else:
        min_price = max_price = current_price
        pct_below = pct_above = 0.0

    # --- volume_totals ---
    if short_total > 0:
        ls_ratio = long_total / short_total
    else:
        ls_ratio = 0.0

    # --- leverage_distribution (% of total volume per tier) ---
    tier_volumes: dict[str, float] = {}
    for entry in longs + shorts:
        lev = entry.get("leverage", "unknown")
        tier_volumes[lev] = tier_volumes.get(lev, 0.0) + float(entry["volume"])
    grand_total = long_total + short_total
    leverage_distribution: dict[str, float] = {}
    if grand_total > 0:
        for tier, vol in sorted(tier_volumes.items()):
            leverage_distribution[tier] = round(vol / grand_total * 100.0, 2)
    # --- top_levels (top-5 by volume, long and short separate) ---

    def _top5(entries: list[dict]) -> list[dict[str, float]]:
        parsed = [
            {"price_level": float(e["price_level"]), "volume": float(e["volume"])} for e in entries
        ]
        parsed.sort(key=lambda x: x["volume"], reverse=True)
        return parsed[:5]

    top_long = _top5(longs)
    top_short = _top5(shorts)

    return {
        "price_range": {
            "min_price": min_price,
            "max_price": max_price,
            "current_price": current_price,
            "pct_below": round(pct_below, 2),
            "pct_above": round(pct_above, 2),
        },
        "volume_totals": {
            "long_total": long_total,
            "short_total": short_total,
            "long_short_ratio": round(ls_ratio, 6),
        },
        "leverage_distribution": leverage_distribution,
        "top_levels": {
            "long": top_long,
            "short": top_short,
        },
    }


def fetch_data_freshness(
    api_base: str,
    symbol: str = "BTCUSDT",
    max_age_minutes: int = 5,
) -> dict[str, Any]:
    """Check data freshness via /data/date-range endpoint.

    Returns dict with end_date, age_hours, age_minutes, and optional warning.
    """
    url = f"{api_base}/data/date-range?symbol={symbol}"
    try:
        data = http_get_json(url, timeout=10.0)
    except Exception as exc:
        return {"error": str(exc)}

    end_date_str = data.get("end_date", "")
    if not end_date_str:
        return {"error": "no end_date in response"}

    try:
        end_dt = datetime.fromisoformat(end_date_str.replace("Z", "+00:00"))
        if end_dt.tzinfo is None:
            end_dt = end_dt.replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        age_seconds = (now - end_dt).total_seconds()
        age_hours = age_seconds / 3600.0
        age_minutes = age_seconds / 60.0
    except Exception as exc:
        return {"error": f"failed to parse end_date: {exc}", "end_date": end_date_str}

    result: dict[str, Any] = {
        "end_date": end_date_str,
        "age_hours": round(age_hours, 2),
        "age_minutes": round(age_minutes, 2),
        "max_age_minutes": max_age_minutes,
    }
    if age_minutes > max_age_minutes:
        result["stale"] = True
        result["warning"] = (
            f"Data is {age_minutes:.1f}m old (>{max_age_minutes}m threshold)"
        )
    else:
        result["stale"] = False
    return result


def start_local_server_if_needed(
    repo_root: Path, host: str, port: int
) -> tuple[subprocess.Popen | None, str]:
    api_base = f"http://{host}:{port}"
    if is_server_healthy(api_base):
        return None, api_base

    cmd = [
        "uv",
        "run",
        "uvicorn",
        "src.liquidationheatmap.api.main:app",
        "--host",
        host,
        "--port",
        str(port),
    ]
    proc = subprocess.Popen(
        cmd,
        cwd=str(repo_root),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        start_new_session=True,
    )

    deadline = time.time() + 45
    while time.time() < deadline:
        if proc.poll() is not None:
            output = ""
            if proc.stdout:
                output = proc.stdout.read()
            raise RuntimeError(f"uvicorn exited early with code {proc.returncode}\n{output}")
        if is_server_healthy(api_base):
            return proc, api_base
        time.sleep(0.5)

    try:
        proc.terminate()
        proc.wait(timeout=3)
    except Exception:
        try:
            os.killpg(proc.pid, signal.SIGTERM)
        except Exception:
            pass
    raise RuntimeError("Timed out waiting for local FastAPI server on /health")


def stop_local_server(proc: subprocess.Popen | None) -> None:
    if proc is None:
        return
    if proc.poll() is not None:
        return
    try:
        proc.terminate()
        proc.wait(timeout=10)
    except Exception:
        try:
            os.killpg(proc.pid, signal.SIGTERM)
            proc.wait(timeout=5)
        except Exception:
            try:
                proc.kill()
            except Exception:
                pass


def build_liqmap_page_url(
    api_base: str,
    exchange: str,
    symbol: str,
    timeframe: int,
    chart_mode: str,
) -> str:
    route_timeframe = ROUTE_TIMEFRAME_BY_DAYS.get(timeframe)
    if route_timeframe is None:
        raise ValueError(f"Unsupported liq-map timeframe '{timeframe}'. Use 1 or 7 days only.")
    page_url = (
        f"{api_base}/chart/derivatives/liq-map/"
        f"{exchange.lower()}/{symbol.lower()}/{route_timeframe}"
    )
    if chart_mode != "bar":
        page_url = f"{page_url}?chart={chart_mode}"
    return page_url


async def wait_for_local_liqmap_ready(page) -> dict[str, Any]:
    """Wait for the Plotly liq-map chart to render."""
    await page.wait_for_selector("#liquidation-map", state="visible", timeout=30000)

    for _ in range(45):
        state = await page.evaluate(
            """
            () => {
                const container = document.getElementById('liquidation-map');
                const plot = container
                    ? container.querySelector('.plot-container')
                    : null;
                const priceEl = document.getElementById('currentPrice');
                return {
                    hasPlot: Boolean(plot),
                    priceText: priceEl ? priceEl.textContent : '',
                };
            }
            """
        )
        if state.get("hasPlot"):
            return {"ready": True, **state}
        await page.wait_for_timeout(1000)
    return {"ready": False, "hasPlot": False, "priceText": ""}


async def capture_local_liqmap_page(
    page_url: str, output_path: Path, headless: bool = True
) -> dict[str, Any]:
    try:
        from playwright.async_api import async_playwright
    except ImportError as exc:
        raise RuntimeError(
            "playwright is not installed. Run `uv add --dev playwright` "
            "and `playwright install chromium`."
        ) from exc

    output_path.parent.mkdir(parents=True, exist_ok=True)
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=headless,
            args=["--no-sandbox", "--disable-setuid-sandbox"],
        )
        page = await browser.new_page(viewport={"width": 1920, "height": 1400})
        try:
            await page.goto(page_url, timeout=120000)
            await page.wait_for_load_state("load", timeout=30000)
            ready_state = await wait_for_local_liqmap_ready(page)

            chart = page.locator("#liquidation-map")
            try:
                await chart.screenshot(path=str(output_path))
            except Exception:
                await page.screenshot(path=str(output_path), full_page=False)
            return ready_state
        finally:
            await browser.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default=DEFAULT_HOST, help="Local FastAPI host")
    parser.add_argument(
        "--port",
        type=int,
        default=DEFAULT_PORT,
        help="Local FastAPI port",
    )
    parser.add_argument(
        "--symbol",
        default=DEFAULT_SYMBOL,
        help="Symbol for API preflight (default BTCUSDT)",
    )
    parser.add_argument(
        "--model",
        default=DEFAULT_MODEL,
        help="Liquidation model (default openinterest)",
    )
    parser.add_argument(
        "--timeframe",
        type=int,
        default=DEFAULT_TIMEFRAME,
        help="Timeframe in days for API (supported: 1 or 7, default 7)",
    )
    parser.add_argument(
        "--coin",
        default="BTC",
        help="Coin symbol for Coinank screenshot",
    )
    parser.add_argument(
        "--exchange",
        default="binance",
        help="Exchange for Coinank liq-map",
    )
    parser.add_argument(
        "--coinank-timeframe",
        default="1w",
        help="Coinank timeframe path segment",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/validation/liqmap"),
        help="Directory for screenshot outputs",
    )
    parser.add_argument(
        "--headed",
        action="store_true",
        help="Run browsers with UI for debugging",
    )
    parser.add_argument(
        "--chart-mode",
        default="bar",
        choices=["area", "bar"],
        help="Chart rendering mode for local screenshot (default: bar)",
    )
    parser.add_argument(
        "--max-freshness-minutes",
        type=int,
        default=5,
        help="Maximum allowed data staleness before validation aborts (default: 5)",
    )
    parser.add_argument(
        "--allow-stale-data",
        action="store_true",
        help="Allow validation to continue even when the freshness gate fails",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    repo_root = Path(__file__).resolve().parents[1]
    output_dir = (
        (repo_root / args.output_dir).resolve()
        if not args.output_dir.is_absolute()
        else args.output_dir
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    manifest_dir = (repo_root / "data/validation/manifests").resolve()
    manifest_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    coin_tag = args.coin.strip().lower() or "coin"
    ex_tag = args.exchange.strip().lower() or "exchange"
    tf_tag = args.coinank_timeframe.strip().replace("/", "_") or "tf"

    ours_path = output_dir / (f"ours_{ex_tag}_{coin_tag}usdt_{tf_tag}_{timestamp}.png")
    coinank_path = output_dir / (f"coinank_{ex_tag}_{coin_tag}usdt_{tf_tag}_{timestamp}.png")
    manifest_path = manifest_dir / (f"liqmap_{ex_tag}_{coin_tag}usdt_{tf_tag}_{timestamp}.json")

    email = os.environ.get("COINANK_USER")
    password = os.environ.get("COINANK_PASSWORD")

    proc: subprocess.Popen | None = None
    api_base = f"http://{args.host}:{args.port}"
    page_url = build_liqmap_page_url(
        api_base=api_base,
        exchange=args.exchange,
        symbol=args.symbol,
        timeframe=args.timeframe,
        chart_mode=args.chart_mode,
    )

    try:
        proc, api_base = start_local_server_if_needed(
            repo_root=repo_root, host=args.host, port=args.port
        )
        page_url = build_liqmap_page_url(
            api_base=api_base,
            exchange=args.exchange,
            symbol=args.symbol,
            timeframe=args.timeframe,
            chart_mode=args.chart_mode,
        )

        api_preflight = preflight_liqmap_api(
            api_base=api_base,
            symbol=args.symbol,
            model=args.model,
            timeframe=args.timeframe,
        )

        # Fetch full payload for numerical metrics
        payload = fetch_liqmap_payload(
            api_base=api_base,
            symbol=args.symbol,
            model=args.model,
            timeframe=args.timeframe,
        )
        numerical_metrics = compute_validation_metrics(payload)

        # Fetch data freshness
        data_freshness = fetch_data_freshness(
            api_base=api_base,
            symbol=args.symbol,
            max_age_minutes=args.max_freshness_minutes,
        )
        if data_freshness.get("stale") and not args.allow_stale_data:
            raise RuntimeError(
                f"data freshness gate failed: {data_freshness['warning']}. "
                "Run scripts/fill_gap_from_ccxt.py (or the ccxt gap-fill timer) before validating, "
                "or pass --allow-stale-data to bypass."
            )

        local_state = asyncio.run(
            capture_local_liqmap_page(
                page_url=page_url,
                output_path=ours_path,
                headless=not args.headed,
            )
        )
        coinank_capture_info: dict = {}
        asyncio.run(
            capture_coinank_liqmap(
                coin=args.coin,
                timeframe=args.coinank_timeframe,
                exchange=args.exchange,
                output_path=coinank_path,
                headless=not args.headed,
                email=email,
                password=password,
                capture_info=coinank_capture_info,
            )
        )

        # Visual element checklist for 1:1 Coinank validation (>= 95% target)
        visual_element_checklist = {
            "tier1_blockers": [
                {
                    "id": "T1-01",
                    "element": "Chart renders",
                    "description": "No blank page, no loading, no JS errors",
                },
                {
                    "id": "T1-02",
                    "element": "Bars both sides",
                    "description": "Stacked bars left (long) AND right (short) of current price",
                },
                {
                    "id": "T1-03",
                    "element": "Current price marker",
                    "description": "Red dashed vertical line at correct price",
                },
                {
                    "id": "T1-04",
                    "element": "Cumulative lines",
                    "description": "Red/pink (long, descending) AND green/cyan (short, ascending)",
                },
                {
                    "id": "T1-05",
                    "element": "Price range",
                    "description": "X-axis range includes current price with margin",
                },
            ],
            "tier2_structure": [
                {
                    "id": "T2-01",
                    "element": "Bar chart type",
                    "description": "Stacked vertical bars, NOT area chart",
                },
                {
                    "id": "T2-02",
                    "element": "3 leverage groups",
                    "description": "Low (blue), Medium (purple), High (orange/salmon)",
                },
                {
                    "id": "T2-03",
                    "element": "Leverage colors",
                    "description": "Blue/purple/orange color families matching Coinank",
                },
                {
                    "id": "T2-04",
                    "element": "Stacking order",
                    "description": "Low (bottom) -> Medium -> High (top)",
                },
                {
                    "id": "T2-05",
                    "element": "Cumulative Long fill",
                    "description": "Filled area below red line (light pink, semi-transparent)",
                },
                {
                    "id": "T2-06",
                    "element": "Cumulative Short fill",
                    "description": "Filled area below green line (light green, semi-transparent)",
                },
                {
                    "id": "T2-07",
                    "element": "Cumulative Long direction",
                    "description": "Starts high at left, descends toward current price",
                },
                {
                    "id": "T2-08",
                    "element": "Cumulative Short direction",
                    "description": "Starts near zero at current price, ascends right",
                },
                {
                    "id": "T2-09",
                    "element": "Current price label",
                    "description": "Text 'Current Price: XXXXX' above chart area",
                },
                {
                    "id": "T2-10",
                    "element": "Current price arrow",
                    "description": "Red upward arrow/triangle at top of dashed line",
                },
                {
                    "id": "T2-11",
                    "element": "Current price dot",
                    "description": "Red circle/dot at bottom of dashed line",
                },
                {
                    "id": "T2-12",
                    "element": "Y-axis left",
                    "description": "Volume scale with M suffix, no axis title",
                },
                {
                    "id": "T2-13",
                    "element": "Y-axis right",
                    "description": "Cumulative scale with M/B suffix, no axis title",
                },
                {
                    "id": "T2-14",
                    "element": "X-axis format",
                    "description": "Plain numbers, no comma separator, no $ sign",
                },
                {
                    "id": "T2-15",
                    "element": "No axis titles",
                    "description": "Only tick numbers, no descriptive labels",
                },
                {
                    "id": "T2-16",
                    "element": "No chart title",
                    "description": "No title text above chart, legend only",
                },
                {
                    "id": "T2-17",
                    "element": "Legend position",
                    "description": "Horizontal, centered, above chart area",
                },
                {
                    "id": "T2-18",
                    "element": "Legend content",
                    "description": "Only 3 leverage group entries",
                },
                {
                    "id": "T2-19",
                    "element": "White background",
                    "description": "White/light background, NOT dark",
                },
                {
                    "id": "T2-20",
                    "element": "Range slider",
                    "description": "Horizontal zoom/pan slider at bottom",
                },
                {
                    "id": "T2-21",
                    "element": "Grid lines",
                    "description": "Light horizontal grid lines",
                },
            ],
            "tier3_magnitude": [
                {
                    "id": "T3-01",
                    "element": "Volume scale",
                    "description": "Y-left same order of magnitude as Coinank",
                },
                {
                    "id": "T3-02",
                    "element": "Cumulative scale",
                    "description": "Y-right same order of magnitude",
                },
                {
                    "id": "T3-03",
                    "element": "Long/Short ratio",
                    "description": "Both sides present, ratio within 2x",
                },
                {
                    "id": "T3-04",
                    "element": "Top volume zones",
                    "description": "Highest bars at similar price levels (+-2%)",
                },
                {
                    "id": "T3-05",
                    "element": "Price range coverage",
                    "description": "Similar min/max prices (+-10%)",
                },
                {
                    "id": "T3-06",
                    "element": "Leverage dominance",
                    "description": "Same tier ordering (typically Low > Med > High)",
                },
            ],
            "scoring": {
                "tier1": "pass/fail gate (any fail = score 0)",
                "tier2": "21 elements x ~4 points = 84 points max",
                "tier3": "6 elements x ~2.7 points = 16 points max",
                "threshold": 95,
            },
        }

        manifest = {
            "timestamp": timestamp,
            "api_base": api_base,
            "page_url": page_url,
            "ours_screenshot": str(ours_path),
            "coinank_screenshot": str(coinank_path),
            "api_preflight": api_preflight,
            "numerical_metrics": numerical_metrics,
            "data_freshness": data_freshness,
            "local_page_state": local_state,
            "coinank_capture_info": coinank_capture_info,
            "visual_element_checklist": visual_element_checklist,
        }
        manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

        print(f"ours_screenshot={ours_path}")
        print(f"coinank_screenshot={coinank_path}")
        print(f"manifest={manifest_path}")

        if api_preflight.get("ok"):
            lc = api_preflight.get("long_count", 0)
            sc = api_preflight.get("short_count", 0)
            if lc == 0 and sc == 0:
                print("warning: API returned zero liquidation levels for the selected parameters")
        if not local_state.get("ready"):
            print(f"warning: local page not fully ready (hasPlot={local_state.get('hasPlot')})")
        if data_freshness.get("warning"):
            print(f"warning: {data_freshness['warning']}")

        # Coinank capture method reporting
        capture_method = coinank_capture_info.get("method", "unknown")
        login_ok = coinank_capture_info.get("login_success", False)
        print(f"coinank_capture_method={capture_method}")
        if coinank_capture_info.get("login_attempted") and not login_ok:
            print("warning: coinank login failed - native download unavailable, used crop fallback")
        if capture_method == "screenshot_crop":
            print(
                "warning: coinank reference is a screenshot crop, NOT native download. "
                "For best 1:1 validation, ensure COINANK_USER and COINANK_PASSWORD are set."
            )

        return 0
    except URLError as exc:
        print(f"error: network/local HTTP failure: {exc}")
        return 1
    except Exception as exc:
        print(f"error: {exc}")
        return 1
    finally:
        stop_local_server(proc)


if __name__ == "__main__":
    sys.exit(main())
