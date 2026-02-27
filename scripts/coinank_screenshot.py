#!/usr/bin/env python3
"""Capture a cropped Coinank liquidation heatmap screenshot.

Example:
    uv run python scripts/coinank_screenshot.py \
        --coin BTC --timeframe 1M --output /tmp/reference.png

Prerequisites:
    - `playwright` Python package
    - `playwright install chromium`
"""

from __future__ import annotations

import argparse
import asyncio
import os
from pathlib import Path

COINANK_URL_TEMPLATE = "https://coinank.com/chart/derivatives/liq-heat-map/{pair}/{timeframe}"
COINANK_LIQMAP_URL_TEMPLATE = (
    "https://coinank.com/chart/derivatives/liq-map/{exchange}/{pair}/{timeframe}"
)
VIEWPORT_WIDTH = 1920
VIEWPORT_HEIGHT = 1400

# Crop tuned for Coinank layout to isolate the heatmap area.
CROP_X = 360
CROP_Y = 300
CROP_WIDTH = 1550
CROP_HEIGHT = 1050

# 20% of 100 samples must look like heatmap colors.
CANVAS_SAMPLE_POINTS = 100
CANVAS_HEATMAP_THRESHOLD = 20


def normalize_coin(coin: str) -> str:
    coin = coin.strip().upper()
    if coin.endswith("USDT"):
        coin = coin[:-4]
    if not coin:
        raise ValueError("coin cannot be empty")
    return coin


def normalize_timeframe(timeframe: str) -> str:
    timeframe = timeframe.strip()
    if not timeframe:
        raise ValueError("timeframe cannot be empty")
    return timeframe


def build_coinank_url(coin: str, timeframe: str) -> str:
    symbol = normalize_coin(coin).lower()
    tf = normalize_timeframe(timeframe)
    return COINANK_URL_TEMPLATE.format(pair=f"{symbol}usdt", timeframe=tf)


def build_coinank_liqmap_url(coin: str, timeframe: str, exchange: str = "binance") -> str:
    symbol = normalize_coin(coin).lower()
    tf = normalize_timeframe(timeframe)
    return COINANK_LIQMAP_URL_TEMPLATE.format(exchange=exchange, pair=f"{symbol}usdt", timeframe=tf)


async def dismiss_common_popups(page) -> None:
    selectors = [
        'button:has-text("Do not consent")',
        'button:has-text("Consent")',
        'button:has-text("Accept")',
        '[aria-label="Close"]',
        ".close-button",
    ]
    for selector in selectors:
        try:
            await page.click(selector, timeout=1000)
            await page.wait_for_timeout(500)
            return
        except Exception:
            continue


async def coinank_login(page, email: str, password: str) -> bool:
    """Login to Coinank via the modal form. Returns True on success."""
    try:
        login_btn = page.locator('button:has-text("Login")').first
        await login_btn.click(timeout=5000)
        await page.wait_for_timeout(1500)

        await page.locator('input[placeholder*="E-Mail"], input[placeholder*="mail"]').first.fill(
            email, timeout=5000
        )
        await page.locator('input[type="password"]').first.fill(password, timeout=5000)

        # Click the Login button inside the modal form
        modal_login = page.locator('.ant-modal-content button:has-text("Login")').first
        await modal_login.click(timeout=5000)
        await page.wait_for_timeout(3000)

        # Verify login: modal should close
        is_visible = await page.locator(".ant-modal-content").is_visible()
        return not is_visible
    except Exception as exc:
        print(f"warning: coinank login failed: {exc}")
        return False


async def download_chart(page, output_path: Path, timeout_ms: int = 15000) -> bool:
    """Download chart via Coinank camera button. Returns True on success."""
    try:
        camera = page.locator(".anticon-camera").first
        await camera.wait_for(state="visible", timeout=10000)

        async with page.expect_download(timeout=timeout_ms) as download_info:
            await camera.click()
        download = await download_info.value
        await download.save_as(str(output_path))
        return True
    except Exception as exc:
        print(f"warning: chart download failed, will fallback to crop: {exc}")
        return False


async def wait_for_canvas_heatmap_colors(page, timeout_seconds: int = 30) -> bool:
    """Wait until the page canvas contains enough viridis-like heatmap colors."""
    try:
        await page.wait_for_selector("canvas", state="visible", timeout=timeout_seconds * 1000)
    except Exception:
        return False

    for _ in range(timeout_seconds):
        has_colors = await page.evaluate(
            f"""
            () => {{
                const canvas = document.querySelector('canvas');
                if (!canvas) return false;
                const ctx = canvas.getContext('2d');
                if (!ctx) return false;
                const width = canvas.width;
                const height = canvas.height;
                if (!width || !height) return false;

                let heatmapPixels = 0;
                for (let i = 0; i < {CANVAS_SAMPLE_POINTS}; i++) {{
                    const x = Math.floor(((i % 10) + 0.5) * width / 10);
                    const y = Math.floor((Math.floor(i / 10) + 0.5) * height / 10);
                    const pixel = ctx.getImageData(x, y, 1, 1).data;

                    const isPurple = pixel[0] > 80 && pixel[2] > 80 && pixel[1] < 100;
                    const isGreen = pixel[1] > 100 && pixel[0] < 180 && pixel[2] < 180;
                    const isCyan = pixel[1] > 100 && pixel[2] > 100;
                    const isYellow = pixel[0] > 150 && pixel[1] > 150 && pixel[2] < 100;

                    if (isPurple || isGreen || isCyan || isYellow) {{
                        heatmapPixels++;
                    }}
                }}

                return heatmapPixels >= {CANVAS_HEATMAP_THRESHOLD};
            }}
            """
        )
        if has_colors:
            await page.wait_for_timeout(1500)
            return True
        await page.wait_for_timeout(1000)
    return False


async def capture_coinank_heatmap(
    coin: str,
    timeframe: str,
    output_path: Path,
    headless: bool = True,
    email: str | None = None,
    password: str | None = None,
) -> Path:
    """Capture a Coinank heatmap screenshot and return the saved path.

    If email/password are provided, logs in first and attempts native chart
    download via camera button. Falls back to screenshot crop on failure.
    """
    try:
        from playwright.async_api import async_playwright
    except ImportError as exc:
        raise RuntimeError(
            "playwright is not installed. Run `uv add --dev playwright` (or install it) and "
            "`playwright install chromium`."
        ) from exc

    url = build_coinank_url(coin, timeframe)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=headless,
            args=["--no-sandbox", "--disable-setuid-sandbox"],
        )
        page = await browser.new_page(
            viewport={"width": VIEWPORT_WIDTH, "height": VIEWPORT_HEIGHT},
            user_agent=(
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
            ),
        )
        try:
            await page.goto(url, timeout=120000)
            await page.wait_for_load_state("load", timeout=30000)
            await page.wait_for_timeout(2000)
            await dismiss_common_popups(page)

            if email and password:
                await coinank_login(page, email, password)

            rendered = await wait_for_canvas_heatmap_colors(page, timeout_seconds=30)
            if not rendered:
                print(
                    "warning: canvas heatmap colors not detected within timeout, capturing anyway"
                )

            # Try native download first, fallback to crop
            if email and password:
                downloaded = await download_chart(page, output_path)
                if downloaded:
                    return output_path

            await page.screenshot(
                path=str(output_path),
                clip={
                    "x": CROP_X,
                    "y": CROP_Y,
                    "width": CROP_WIDTH,
                    "height": CROP_HEIGHT,
                },
            )
            return output_path
        finally:
            await browser.close()


async def wait_for_canvas_liqmap_colors(page, timeout_seconds: int = 30) -> bool:
    """Wait until the page canvas contains liq-map bar chart colors."""
    try:
        await page.wait_for_selector("canvas", state="visible", timeout=timeout_seconds * 1000)
    except Exception:
        return False

    for _ in range(timeout_seconds):
        has_colors = await page.evaluate(
            f"""
            () => {{
                const canvas = document.querySelector('canvas');
                if (!canvas) return false;
                const ctx = canvas.getContext('2d');
                if (!ctx) return false;
                const width = canvas.width;
                const height = canvas.height;
                if (!width || !height) return false;

                let chartPixels = 0;
                for (let i = 0; i < {CANVAS_SAMPLE_POINTS}; i++) {{
                    const x = Math.floor(((i % 10) + 0.5) * width / 10);
                    const y = Math.floor((Math.floor(i / 10) + 0.5) * height / 10);
                    const pixel = ctx.getImageData(x, y, 1, 1).data;

                    // Detect bar chart colors: cyan, blue, orange, pink/red
                    const isCyan = pixel[1] > 150 && pixel[2] > 150 && pixel[0] < 150;
                    const isBlue = pixel[2] > 150 && pixel[0] < 150 && pixel[1] < 200;
                    const isOrange = pixel[0] > 200 && pixel[1] > 100 && pixel[2] < 100;
                    const isPink = pixel[0] > 200 && pixel[1] < 120 && pixel[2] > 80;

                    if (isCyan || isBlue || isOrange || isPink) {{
                        chartPixels++;
                    }}
                }}

                return chartPixels >= {CANVAS_HEATMAP_THRESHOLD};
            }}
            """
        )
        if has_colors:
            await page.wait_for_timeout(1500)
            return True
        await page.wait_for_timeout(1000)
    return False


async def capture_coinank_liqmap(
    coin: str,
    timeframe: str,
    exchange: str,
    output_path: Path,
    headless: bool = True,
    email: str | None = None,
    password: str | None = None,
) -> Path:
    """Capture a Coinank liq-map screenshot and return the saved path.

    If email/password are provided, logs in first and attempts native chart
    download via camera button. Falls back to screenshot crop on failure.
    """
    try:
        from playwright.async_api import async_playwright
    except ImportError as exc:
        raise RuntimeError(
            "playwright is not installed. Run `uv add --dev playwright` and "
            "`playwright install chromium`."
        ) from exc

    url = build_coinank_liqmap_url(coin, timeframe, exchange)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=headless,
            args=["--no-sandbox", "--disable-setuid-sandbox"],
        )
        page = await browser.new_page(
            viewport={"width": VIEWPORT_WIDTH, "height": VIEWPORT_HEIGHT},
            user_agent=(
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
            ),
        )
        try:
            await page.goto(url, timeout=120000)
            await page.wait_for_load_state("load", timeout=30000)
            await page.wait_for_timeout(2000)
            await dismiss_common_popups(page)

            if email and password:
                await coinank_login(page, email, password)

            rendered = await wait_for_canvas_liqmap_colors(page, timeout_seconds=30)
            if not rendered:
                print("warning: liq-map chart colors not detected within timeout, capturing anyway")

            # Try native download first, fallback to crop
            if email and password:
                downloaded = await download_chart(page, output_path)
                if downloaded:
                    return output_path

            await page.screenshot(
                path=str(output_path),
                clip={
                    "x": CROP_X,
                    "y": 250,
                    "width": CROP_WIDTH,
                    "height": 1100,
                },
            )
            return output_path
        finally:
            await browser.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--coin", default="BTC", help="Coin symbol, e.g. BTC or ETH")
    parser.add_argument(
        "--timeframe",
        default="1M",
        help="Coinank timeframe segment (e.g. 1w, 1M, 3M, 1y)",
    )
    parser.add_argument(
        "--output",
        required=True,
        type=Path,
        help="Output PNG path",
    )
    parser.add_argument(
        "--product",
        choices=["heatmap", "map"],
        default="heatmap",
        help="Coinank product type: heatmap (liq-heat-map) or map (liq-map)",
    )
    parser.add_argument(
        "--exchange",
        choices=["binance", "bybit", "hyperliquid"],
        default="binance",
        help="Exchange for liq-map product (ignored for heatmap)",
    )
    parser.add_argument(
        "--headed",
        action="store_true",
        help="Run browser with UI for debugging",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    email = os.environ.get("COINANK_USER")
    password = os.environ.get("COINANK_PASSWORD")
    if email and password:
        print("info: COINANK credentials found, will attempt native download")
    else:
        print("info: no COINANK credentials, using screenshot crop fallback")

    try:
        if args.product == "map":
            saved = asyncio.run(
                capture_coinank_liqmap(
                    coin=args.coin,
                    timeframe=args.timeframe,
                    exchange=args.exchange,
                    output_path=args.output,
                    headless=not args.headed,
                    email=email,
                    password=password,
                )
            )
            url = build_coinank_liqmap_url(args.coin, args.timeframe, args.exchange)
        else:
            saved = asyncio.run(
                capture_coinank_heatmap(
                    coin=args.coin,
                    timeframe=args.timeframe,
                    output_path=args.output,
                    headless=not args.headed,
                    email=email,
                    password=password,
                )
            )
            url = build_coinank_url(args.coin, args.timeframe)
    except Exception as exc:
        print(f"error: {exc}")
        return 1

    print(f"saved: {saved}")
    print(f"url: {url}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
