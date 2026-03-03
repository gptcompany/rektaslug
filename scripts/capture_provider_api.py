#!/usr/bin/env python3
"""Capture raw JSON API responses from provider page loads.

This script opens one or more provider pages with Playwright, listens for
network responses, saves the JSON payloads that look relevant, and writes a
manifest for later comparison.

Examples:
    uv run python scripts/capture_provider_api.py \
        --provider coinank \
        --coin BTC --timeframe 1w

    uv run python scripts/capture_provider_api.py \
        --provider both \
        --coin BTC --timeframe 1w \
        --coinglass-url "https://www.coinglass.com/..."

    uv run python scripts/capture_provider_api.py \
        --provider bitcoincounterflow
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from coinank_screenshot import build_coinank_liqmap_url, coinank_login, dismiss_common_popups

VIEWPORT_WIDTH = 1920
VIEWPORT_HEIGHT = 1400
DEFAULT_OUTPUT_DIR = Path("data/validation/raw_provider_api")
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)
DEFAULT_COINGLASS_URL = "https://www.coinglass.com/LiquidationData"
BITCOINCOUNTERFLOW_DEFAULT_URL = "https://bitcoincounterflow.com/liquidation-heatmap/"
BITCOINCOUNTERFLOW_LIQUIDATIONS_URL = (
    "https://api.bitcoincounterflow.com/api/liquidations"
    "?exchange=BinanceUSDM&symbol=BTCUSDT&timeframe=15m&days=7"
)

PROVIDER_DOMAINS = {
    "coinank": ("coinank.com",),
    "coinglass": ("coinglass.com",),
    "bitcoincounterflow": (
        "bitcoincounterflow.com",
        "api.bitcoincounterflow.com",
        "proxy.bitcoincounterflow.com",
        "serverless-vercel-nine.vercel.app",
    ),
}

RELEVANT_RESPONSE_HEADERS = {
    "content-type",
    "encryption",
    "language",
    "time",
    "user",
    "v",
}
RELEVANT_REQUEST_HEADERS = {
    "cache-ts-v2",
}


@dataclass
class CaptureTarget:
    """Single provider page to visit and capture from."""

    provider: str
    url: str
    email: str | None = None
    password: str | None = None


def utc_timestamp_slug() -> str:
    """Return a stable UTC timestamp for filenames."""
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def safe_slug(value: str, fallback: str = "payload") -> str:
    """Convert arbitrary strings into filesystem-safe slugs."""
    slug = re.sub(r"[^a-zA-Z0-9]+", "_", value).strip("_").lower()
    return slug or fallback


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--provider",
        choices=["coinank", "coinglass", "bitcoincounterflow", "both", "all"],
        default="both",
        help="Which provider(s) to capture. 'both' = CoinAnk + Coinglass; 'all' adds Bitcoin CounterFlow.",
    )
    parser.add_argument("--coin", default="BTC", help="Base coin, e.g. BTC or ETH.")
    parser.add_argument(
        "--timeframe",
        default="1w",
        help="CoinAnk liq-map timeframe segment, e.g. 1d, 1w, 1M.",
    )
    parser.add_argument(
        "--exchange",
        default="binance",
        choices=["binance", "bybit", "hyperliquid"],
        help="Exchange for CoinAnk liq-map URL generation.",
    )
    parser.add_argument(
        "--coinank-url",
        help="Override CoinAnk page URL. Defaults to generated liq-map URL.",
    )
    parser.add_argument(
        "--coinglass-url",
        default=DEFAULT_COINGLASS_URL,
        help="Coinglass page URL to open. Defaults to the public LiquidationData page.",
    )
    parser.add_argument(
        "--bitcoincounterflow-url",
        default=BITCOINCOUNTERFLOW_DEFAULT_URL,
        help="Bitcoin CounterFlow page URL to open.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Base directory for raw captures and manifests.",
    )
    parser.add_argument(
        "--max-responses",
        type=int,
        default=25,
        help="Maximum JSON responses to persist per provider.",
    )
    parser.add_argument(
        "--post-load-wait-ms",
        type=int,
        default=8000,
        help="How long to keep listening after the page reaches load/network-idle.",
    )
    parser.add_argument(
        "--headed",
        action="store_true",
        help="Run Chromium with UI for debugging.",
    )
    return parser.parse_args()


def build_targets(args: argparse.Namespace) -> list[CaptureTarget]:
    """Build the provider targets requested by the user."""
    targets: list[CaptureTarget] = []

    if args.provider in {"coinank", "both", "all"}:
        coinank_url = args.coinank_url or build_coinank_liqmap_url(
            args.coin, args.timeframe, args.exchange
        )
        targets.append(
            CaptureTarget(
                provider="coinank",
                url=coinank_url,
                email=os.environ.get("COINANK_USER"),
                password=os.environ.get("COINANK_PASSWORD"),
            )
        )

    if args.provider in {"coinglass", "both", "all"}:
        coinglass_url = args.coinglass_url or DEFAULT_COINGLASS_URL
        targets.append(
            CaptureTarget(
                provider="coinglass",
                url=coinglass_url,
                email=os.environ.get("COINGLASS_USER_LOGIN"),
                password=os.environ.get("COINGLASS_USER_PASSWORD"),
            )
        )

    if args.provider in {"bitcoincounterflow", "all"}:
        targets.append(
            CaptureTarget(
                provider="bitcoincounterflow",
                url=args.bitcoincounterflow_url,
            )
        )

    return targets


def looks_like_json_body(content_type: str, url: str, body: str) -> bool:
    """Decide whether a response body looks like JSON worth saving."""
    lowered_type = (content_type or "").lower()
    lowered_url = url.lower()
    if re.search(r"\.(css|js|mjs|map|svg|png|jpe?g|gif|webp|woff2?|ttf)(?:\?|$)", lowered_url):
        return False
    stripped = body.lstrip()
    if "json" in lowered_type:
        return True
    if stripped.startswith("{") or stripped.startswith("["):
        return True
    return "/api/" in lowered_url or "graphql" in lowered_url


def summarize_json_payload(payload: Any) -> dict[str, Any]:
    """Return a lightweight description of a parsed JSON payload."""
    if isinstance(payload, dict):
        return {
            "kind": "object",
            "top_level_keys": sorted(str(key) for key in payload.keys())[:50],
        }

    if isinstance(payload, list):
        summary: dict[str, Any] = {
            "kind": "array",
            "length": len(payload),
        }
        if payload and isinstance(payload[0], dict):
            summary["first_item_keys"] = sorted(str(key) for key in payload[0].keys())[:50]
        return summary

    return {
        "kind": type(payload).__name__,
        "repr": str(payload)[:200],
    }


def select_relevant_headers(headers: dict[str, str], allowed_keys: set[str]) -> dict[str, str]:
    """Keep only the small header subset needed for later decoding/debugging."""
    selected: dict[str, str] = {}
    for key, value in headers.items():
        lowered_key = key.lower()
        if lowered_key in allowed_keys:
            selected[lowered_key] = value
    return selected


async def maybe_log_in(target: CaptureTarget, page) -> bool:
    """Perform best-effort login for providers that need it."""
    if not target.email or not target.password:
        return False

    if target.provider == "coinank":
        await dismiss_common_popups(page)
        return await coinank_login(page, target.email, target.password)

    if target.provider == "coinglass":
        return await coinglass_login(page, target.email, target.password)

    return False


async def coinglass_login(page, email: str, password: str) -> bool:
    """Best-effort Coinglass login based on generic visible form fields."""
    login_selectors = [
        'button:has-text("Log In")',
        'button:has-text("Login")',
        'button:has-text("Sign In")',
        'a:has-text("Log In")',
        'a:has-text("Login")',
        'a:has-text("Sign In")',
    ]
    for selector in login_selectors:
        try:
            await page.locator(selector).first.click(timeout=2000)
            await page.wait_for_timeout(1500)
            break
        except Exception:
            continue

    email_input = None
    for selector in (
        'input[type="email"]',
        'input[name*="email" i]',
        'input[placeholder*="email" i]',
        'input[id*="email" i]',
    ):
        locator = page.locator(selector).first
        try:
            if await locator.is_visible(timeout=1000):
                email_input = locator
                break
        except Exception:
            continue

    password_input = None
    for selector in (
        'input[type="password"]',
        'input[name*="password" i]',
        'input[placeholder*="password" i]',
        'input[id*="password" i]',
    ):
        locator = page.locator(selector).first
        try:
            if await locator.is_visible(timeout=1000):
                password_input = locator
                break
        except Exception:
            continue

    if email_input is None or password_input is None:
        return False

    try:
        await email_input.fill(email, timeout=5000)
        await password_input.fill(password, timeout=5000)
    except Exception:
        return False

    for selector in (
        'button:has-text("Log In")',
        'button:has-text("Login")',
        'button:has-text("Sign In")',
        'button[type="submit"]',
    ):
        try:
            await page.locator(selector).last.click(timeout=2000)
            break
        except Exception:
            continue
    else:
        try:
            await password_input.press("Enter")
        except Exception:
            return False

    try:
        await page.wait_for_load_state("networkidle", timeout=10000)
    except Exception:
        pass
    await page.wait_for_timeout(3000)

    try:
        return not await password_input.is_visible()
    except Exception:
        return True


def response_matches_target(target: CaptureTarget, url: str) -> bool:
    """Return True when the response URL belongs to the provider's network surface."""
    lowered = url.lower()
    return any(domain in lowered for domain in PROVIDER_DOMAINS[target.provider])


async def prime_bitcoincounterflow_capture(page) -> None:
    """Trigger a direct fetch for the raw liquidations payload from the site origin."""
    try:
        await page.evaluate(
            """
            async (url) => {
                const response = await fetch(url, { credentials: 'omit' });
                await response.text();
                return { ok: response.ok, status: response.status };
            }
            """,
            BITCOINCOUNTERFLOW_LIQUIDATIONS_URL,
        )
    except Exception:
        # The passive page capture is still useful even if the direct probe fails.
        return


async def capture_target(
    target: CaptureTarget,
    run_dir: Path,
    max_responses: int,
    post_load_wait_ms: int,
    headless: bool,
) -> dict[str, Any]:
    """Capture JSON responses for a single provider."""
    try:
        from playwright.async_api import async_playwright
    except ImportError as exc:
        raise RuntimeError(
            "playwright is not installed. Run `uv add --dev playwright` and "
            "`playwright install chromium`."
        ) from exc

    provider_dir = run_dir / target.provider
    provider_dir.mkdir(parents=True, exist_ok=True)
    captured: list[dict[str, Any]] = []
    seen_urls: set[str] = set()
    capture_tasks: set[asyncio.Task[Any]] = set()
    capture_lock = asyncio.Lock()
    counter = 0

    async def handle_response(response) -> None:
        nonlocal counter

        async with capture_lock:
            if len(captured) >= max_responses:
                return

            url = response.url
            if not response_matches_target(target, url):
                return

            if response.status >= 400:
                return

            request_post_data = response.request.post_data or ""
            request_fingerprint = hashlib.sha1(request_post_data.encode("utf-8")).hexdigest()[:12]
            key = f"{response.request.method}:{url}:{request_fingerprint}"
            if key in seen_urls:
                return

            try:
                body = await response.text()
            except Exception:
                return

            content_type = response.headers.get("content-type", "")
            if not looks_like_json_body(content_type, url, body):
                return

            seen_urls.add(key)
            counter += 1
            response_headers = select_relevant_headers(response.headers, RELEVANT_RESPONSE_HEADERS)
            raw_request_headers = getattr(response.request, "headers", {}) or {}
            if not isinstance(raw_request_headers, dict):
                raw_request_headers = {}
            request_headers = select_relevant_headers(
                raw_request_headers,
                RELEVANT_REQUEST_HEADERS,
            )

            parsed_payload: Any = None
            file_ext = "txt"
            body_to_write = body
            summary: dict[str, Any]
            try:
                parsed_payload = json.loads(body)
                file_ext = "json"
                body_to_write = json.dumps(parsed_payload, indent=2, ensure_ascii=True)
                summary = summarize_json_payload(parsed_payload)
            except Exception:
                summary = {"kind": "text", "preview": body[:200]}

            parsed_url = urlparse(url)
            stem = safe_slug(Path(parsed_url.path).name or parsed_url.path or "response")
            file_name = f"{counter:02d}_{stem}.{file_ext}"
            output_path = provider_dir / file_name
            output_path.write_text(body_to_write, encoding="utf-8")

            captured.append(
                {
                    "provider": target.provider,
                    "source_url": url,
                    "status": response.status,
                    "method": response.request.method,
                    "content_type": content_type,
                    "response_headers": response_headers,
                    "request_headers": request_headers,
                    "request_post_data": request_post_data[:5000],
                    "saved_file": str(output_path),
                    "summary": summary,
                }
            )

    def schedule_response(response) -> None:
        task = asyncio.create_task(handle_response(response))
        capture_tasks.add(task)
        task.add_done_callback(capture_tasks.discard)

    login_success = False

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=headless,
            args=["--no-sandbox", "--disable-setuid-sandbox"],
        )
        context = await browser.new_context(
            viewport={"width": VIEWPORT_WIDTH, "height": VIEWPORT_HEIGHT},
            user_agent=DEFAULT_USER_AGENT,
        )
        page = await context.new_page()
        page.on("response", schedule_response)

        try:
            await page.goto(target.url, timeout=120000)
            await page.wait_for_load_state("load", timeout=30000)
            login_success = await maybe_log_in(target, page)

            if target.provider == "bitcoincounterflow":
                await prime_bitcoincounterflow_capture(page)

            try:
                await page.wait_for_load_state("networkidle", timeout=15000)
            except Exception:
                pass

            await page.wait_for_timeout(post_load_wait_ms)
            if capture_tasks:
                await asyncio.gather(*capture_tasks, return_exceptions=True)
        finally:
            await browser.close()

    summary = {
        "provider": target.provider,
        "page_url": target.url,
        "login_attempted": bool(target.email and target.password),
        "login_success": login_success,
        "capture_count": len(captured),
        "captures": captured,
    }

    summary_path = provider_dir / "summary.json"
    summary_path.write_text(json.dumps(summary, indent=2, ensure_ascii=True), encoding="utf-8")
    return summary


def build_run_comparison(provider_summaries: list[dict[str, Any]]) -> dict[str, Any]:
    """Build a lightweight cross-provider summary for quick inspection."""
    providers: dict[str, dict[str, Any]] = {}

    for summary in provider_summaries:
        providers[summary["provider"]] = {
            "page_url": summary["page_url"],
            "capture_count": summary["capture_count"],
            "login_success": summary["login_success"],
            "endpoints": [capture["source_url"] for capture in summary["captures"]],
        }

    return {
        "providers": providers,
        "notes": [
            "This is a raw capture manifest, not a schema-normalized comparison.",
            "Use the saved JSON bodies to map matching endpoints and units before numerical diffs.",
        ],
    }


async def run_capture(args: argparse.Namespace, emit_progress: bool = True) -> Path:
    """Execute the capture workflow and return the manifest path."""
    targets = build_targets(args)
    run_dir = args.output_dir / utc_timestamp_slug()
    run_dir.mkdir(parents=True, exist_ok=True)

    provider_summaries: list[dict[str, Any]] = []
    for target in targets:
        if emit_progress:
            print(f"capturing {target.provider}: {target.url}")
        summary = await capture_target(
            target=target,
            run_dir=run_dir,
            max_responses=args.max_responses,
            post_load_wait_ms=args.post_load_wait_ms,
            headless=not args.headed,
        )
        provider_summaries.append(summary)
        if emit_progress:
            print(f"saved {summary['capture_count']} JSON responses for {target.provider}")

    manifest = {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "run_dir": str(run_dir),
        "args": {
            "provider": args.provider,
            "coin": args.coin,
            "timeframe": args.timeframe,
            "exchange": args.exchange,
            "max_responses": args.max_responses,
            "post_load_wait_ms": args.post_load_wait_ms,
        },
        "providers": provider_summaries,
        "comparison": build_run_comparison(provider_summaries),
    }
    manifest_path = run_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=True), encoding="utf-8")
    if emit_progress:
        print(f"manifest: {manifest_path}")
    return manifest_path


async def async_main(args: argparse.Namespace) -> int:
    """Async CLI entry point."""
    await run_capture(args, emit_progress=True)

    return 0


def main() -> int:
    """CLI entry point."""
    args = parse_args()
    try:
        return asyncio.run(async_main(args))
    except KeyboardInterrupt:
        print("cancelled")
        return 130
    except Exception as exc:
        print(f"error: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
