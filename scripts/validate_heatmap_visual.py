#!/usr/bin/env python3
"""Capture side-by-side validation screenshots for our 30d heatmap vs Coinank.

Pipeline:
1. Ensure local FastAPI is running
2. Screenshot the local heatmap_30d.html page
3. Screenshot Coinank `btcusdt/1M`
4. Save both files to `data/validation/` with a shared timestamp
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
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.error import URLError
from urllib.request import urlopen

sys.path.insert(0, str(Path(__file__).resolve().parent))
from coinank_screenshot import capture_coinank_heatmap

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = int(os.environ.get("HEATMAP_PORT", 8002))
DEFAULT_SYMBOL = "BTCUSDT"
DEFAULT_TIME_WINDOW = "30d"
DEFAULT_PRICE_BIN_SIZE = 500


def http_get_json(url: str, timeout: float = 3.0) -> dict[str, Any]:
    with urlopen(url, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def is_server_healthy(api_base: str) -> bool:
    try:
        with urlopen(f"{api_base}/health", timeout=1.5) as resp:
            return resp.status == 200
    except Exception:
        return False


def preflight_heatmap_api(api_base: str, symbol: str, time_window: str, price_bin_size: int) -> dict[str, Any]:
    url = (
        f"{api_base}/liquidations/heatmap-timeseries?"
        f"symbol={symbol}&time_window={time_window}&price_bin_size={price_bin_size}"
    )
    try:
        payload = http_get_json(url, timeout=15.0)
    except Exception as exc:
        return {"ok": False, "error": str(exc), "url": url}

    meta = payload.get("meta", {})
    data = payload.get("data", [])
    return {
        "ok": True,
        "url": url,
        "total_snapshots": meta.get("total_snapshots", 0),
        "interval": meta.get("interval"),
        "start_time": meta.get("start_time"),
        "end_time": meta.get("end_time"),
        "empty": len(data) == 0,
    }


def start_local_server_if_needed(repo_root: Path, host: str, port: int) -> tuple[subprocess.Popen | None, str]:
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


async def wait_for_local_page_ready(page) -> dict[str, Any]:
    await page.wait_for_selector("#chart-container", state="visible", timeout=30000)

    # Wait until Plotly container exists or status enters a terminal state.
    for _ in range(45):
        state = await page.evaluate(
            """
            () => {
                const statusEl = document.getElementById('status');
                const plot = document.querySelector('#heatmap .plot-container');
                return {
                    statusText: statusEl ? statusEl.textContent : '',
                    statusClass: statusEl ? statusEl.className : '',
                    hasPlot: Boolean(plot),
                };
            }
            """
        )
        status_text = (state.get("statusText") or "").strip()
        if state.get("hasPlot"):
            return {"ready": True, **state}
        if status_text.startswith("No heatmap data") or status_text.startswith("Error:"):
            return {"ready": False, **state}
        await page.wait_for_timeout(1000)
    return {"ready": False, "statusText": "timeout", "statusClass": "", "hasPlot": False}


async def capture_local_heatmap_page(page_url: str, output_path: Path, headless: bool = True) -> dict[str, Any]:
    try:
        from playwright.async_api import async_playwright
    except ImportError as exc:
        raise RuntimeError(
            "playwright is not installed. Run `uv add --dev playwright` (or install it) and "
            "`playwright install chromium`."
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
            ready_state = await wait_for_local_page_ready(page)

            # Prefer chart-only screenshot for easier side-by-side comparison.
            chart = page.locator("#chart-container")
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
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help="Local FastAPI port")
    parser.add_argument("--symbol", default=DEFAULT_SYMBOL, help="Symbol for API preflight (default BTCUSDT)")
    parser.add_argument("--time-window", default=DEFAULT_TIME_WINDOW, help="API time_window (default 30d)")
    parser.add_argument("--price-bin-size", type=int, default=DEFAULT_PRICE_BIN_SIZE, help="API price_bin_size")
    parser.add_argument("--coin", default="BTC", help="Coin symbol for Coinank screenshot")
    parser.add_argument("--coinank-timeframe", default="1M", help="Coinank timeframe path segment")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/validation"),
        help="Directory for screenshot outputs",
    )
    parser.add_argument("--headed", action="store_true", help="Run browsers with UI for debugging")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    repo_root = Path(__file__).resolve().parents[1]
    output_dir = (repo_root / args.output_dir).resolve() if not args.output_dir.is_absolute() else args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    ours_path = output_dir / f"ours_heatmap_{args.time_window}_{timestamp}.png"
    coin_tag = args.coin.strip().lower() or "coin"
    timeframe_tag = args.coinank_timeframe.strip().replace("/", "_") or "tf"
    coinank_path = output_dir / f"coinank_{coin_tag}_{timeframe_tag}_{timestamp}.png"
    manifest_path = output_dir / f"validation_manifest_{timestamp}.json"

    proc: subprocess.Popen | None = None
    api_base = f"http://{args.host}:{args.port}"
    page_url = f"{api_base}/heatmap_30d.html"

    try:
        proc, api_base = start_local_server_if_needed(repo_root=repo_root, host=args.host, port=args.port)
        page_url = f"{api_base}/heatmap_30d.html"

        api_preflight = preflight_heatmap_api(
            api_base=api_base,
            symbol=args.symbol,
            time_window=args.time_window,
            price_bin_size=args.price_bin_size,
        )

        local_state = asyncio.run(
            capture_local_heatmap_page(page_url=page_url, output_path=ours_path, headless=not args.headed)
        )
        asyncio.run(
            capture_coinank_heatmap(
                coin=args.coin,
                timeframe=args.coinank_timeframe,
                output_path=coinank_path,
                headless=not args.headed,
            )
        )

        manifest = {
            "timestamp": timestamp,
            "api_base": api_base,
            "page_url": page_url,
            "ours_screenshot": str(ours_path),
            "coinank_screenshot": str(coinank_path),
            "api_preflight": api_preflight,
            "local_page_state": local_state,
        }
        manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

        print(f"ours_screenshot={ours_path}")
        print(f"coinank_screenshot={coinank_path}")
        print(f"manifest={manifest_path}")

        if api_preflight.get("ok") and api_preflight.get("empty"):
            print("warning: local API returned zero snapshots for the selected window; screenshot may show empty/error state")
        if not local_state.get("ready"):
            print(f"warning: local page not fully ready ({local_state.get('statusText', 'unknown state')})")

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
