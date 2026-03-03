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
import base64
import hashlib
import json
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlparse

import pyotp
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad

from coinank_screenshot import build_coinank_liqmap_url, coinank_login, dismiss_common_popups

VIEWPORT_WIDTH = 1920
VIEWPORT_HEIGHT = 1400
DEFAULT_OUTPUT_DIR = Path("data/validation/raw_provider_api")
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)
DEFAULT_COINGLASS_URL = "https://www.coinglass.com/LiquidationData"
COINGLASS_LOGIN_URL = "https://www.coinglass.com/login?act=liqmap"
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

# --- CoinGlass TOTP+AES data parameter (reverse-engineered from _app bundle) ---
# These are public client-side constants embedded in the CoinGlass frontend JS.
_CG_TOTP_SECRET = "I65VU7K5ZQL7WB4E"
_CG_AES_KEY = "1f68efd73f8d4921acc0dead41dd39bc"

# Mapping from UI dropdown value (days) to API (interval, limit).
# Source: LiquidationMap page chunk onChangeTime handler.
COINGLASS_TIMEFRAME_MAP: dict[str, tuple[str, int]] = {
    "1d": ("1", 1500),
    "1w": ("5", 2000),
    "1m": ("30", 1440),
    "3m": ("90d", 1440),
    "6m": ("180d", 1440),
    "1y": ("365d", 1440),
}
# Aliases that normalize to the canonical (lowercase) keys above.
_CG_TIMEFRAME_ALIASES: dict[str, str] = {
    "1 day": "1d",
    "7 day": "1w",
    "7d": "1w",
    "30 day": "1m",
    "30d": "1m",
    "90 day": "3m",
    "90d": "3m",
    "180 day": "6m",
    "180d": "6m",
    "365 day": "1y",
    "365d": "1y",
    "1 year": "1y",
}


def generate_coinglass_data_param() -> str:
    """Generate the ``data`` query parameter for CoinGlass liqMap/liqHeatMap.

    The frontend builds this by creating a TOTP token, concatenating it with the
    current Unix timestamp, and encrypting the result with AES-128-ECB.
    """
    ts = int(time.time())
    totp = pyotp.TOTP(_CG_TOTP_SECRET, interval=30)
    otp_code = totp.at(ts)
    plaintext = f"{ts},{otp_code}"
    cipher = AES.new(_CG_AES_KEY.encode("utf-8"), AES.MODE_ECB)
    encrypted = cipher.encrypt(pad(plaintext.encode("utf-8"), AES.block_size))
    return base64.b64encode(encrypted).decode("utf-8")


def resolve_coinglass_interval_limit(
    timeframe: str,
) -> tuple[str, int]:
    """Return ``(interval, limit)`` for a given CLI/UI timeframe string.

    Raises ``ValueError`` if the timeframe is not recognized.
    """
    key = timeframe.strip().lower()
    key = _CG_TIMEFRAME_ALIASES.get(key, key)
    if key in COINGLASS_TIMEFRAME_MAP:
        return COINGLASS_TIMEFRAME_MAP[key]
    supported = sorted(COINGLASS_TIMEFRAME_MAP.keys())
    raise ValueError(
        f"Unsupported CoinGlass timeframe: {timeframe!r}. "
        f"Supported: {', '.join(supported)}"
    )


# ---------------------------------------------------------------------------
# Browserless CoinGlass API (no Playwright needed)
# ---------------------------------------------------------------------------

_CG_LOGIN_URL = "https://capi.coinglass.com/coin-community/api/user/login"
_CG_LIQMAP_URL = "https://capi.coinglass.com/api/index/5/liqMap"


def coinglass_rest_login(email: str, password: str) -> dict[str, Any]:
    """Login to CoinGlass via REST and return token info.

    Returns a dict with ``accessToken``, ``refreshToken``, and
    ``accessTokenExpireIn`` on success.  Raises ``RuntimeError`` on failure.
    """
    import urllib.request
    import urllib.parse

    body = urllib.parse.urlencode({"mailAddress": email, "password": password}).encode()
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "application/json",
        "User-Agent": DEFAULT_USER_AGENT,
        "Language": "en",
        "Encryption": "true",
        "Cache-Ts-V2": str(int(time.time() * 1000)),
    }
    req = urllib.request.Request(_CG_LOGIN_URL, data=body, headers=headers)
    with urllib.request.urlopen(req, timeout=30) as resp:
        data = json.loads(resp.read().decode())

    if data.get("code") != "0" or not data.get("data", {}).get("accessToken"):
        raise RuntimeError(f"CoinGlass login failed: {data.get('msg', data)}")
    return data["data"]


def coinglass_rest_fetch_liqmap(
    access_token: str,
    timeframe: str,
    symbol: str = "Binance_BTCUSDT",
) -> dict[str, Any]:
    """Fetch liqMap from CoinGlass API without a browser.

    Returns the raw JSON response dict (with encrypted ``data`` field).
    """
    import urllib.request

    interval, limit = resolve_coinglass_interval_limit(timeframe)
    data_param = generate_coinglass_data_param()
    data_encoded = quote(data_param, safe="")

    url = (
        f"{_CG_LIQMAP_URL}?merge=true&symbol={symbol}"
        f"&interval={interval}&limit={limit}&data={data_encoded}"
    )
    headers = {
        "Accept": "application/json",
        "User-Agent": DEFAULT_USER_AGENT,
        "Language": "en",
        "Encryption": "true",
        "Cache-Ts-V2": str(int(time.time() * 1000)),
        "Obe": access_token,
        "Referer": "https://www.coinglass.com/",
    }
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=30) as resp:
        result = json.loads(resp.read().decode())
        resp_headers = {k.lower(): v for k, v in resp.getheaders()}

    return {
        "url": url,
        "status": 200,
        "body": result,
        "response_headers": {
            k: resp_headers[k] for k in ("encryption", "user", "v", "time")
            if k in resp_headers
        },
    }


@dataclass
class CaptureTarget:
    """Single provider page to visit and capture from."""

    provider: str
    url: str
    email: str | None = None
    password: str | None = None
    ui_timeframe: str | None = None


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
        "--coinglass-timeframe",
        help=(
            "Optional Coinglass LiquidationMap UI timeframe. "
            "Examples: 1d, 1w, '1 day', '1 week'. Defaults to a mapping from --timeframe."
        ),
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
    parser.add_argument(
        "--coinglass-mode",
        choices=["browser", "rest", "auto"],
        default="browser",
        help=(
            "CoinGlass capture method. "
            "'browser': Playwright route interception (default). "
            "'rest': direct REST API replay (no browser). "
            "'auto': try REST first, fallback to browser on failure."
        ),
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
                ui_timeframe=normalize_coinglass_timeframe_label(
                    args.coinglass_timeframe or args.timeframe
                ),
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


def normalize_coinglass_timeframe_label(value: str | None) -> str | None:
    """Normalize CLI timeframe hints into Coinglass UI labels."""
    if not value:
        return None

    lowered = value.strip().lower()
    explicit = {
        "1d": "1 day",
        "3d": "3 day",
        "7d": "7 day",
        "1w": "7 day",
        "2w": "14 day",
        "1m": "30 day",
        "3m": "90 day",
        "6m": "180 day",
        "1 day": "1 day",
        "3 day": "3 day",
        "7 day": "7 day",
        "14 day": "14 day",
        "30 day": "30 day",
        "90 day": "90 day",
        "180 day": "180 day",
        "1 week": "7 day",
        "2 week": "14 day",
        "1 month": "30 day",
    }
    if lowered in explicit:
        return explicit[lowered]

    interval_match = re.fullmatch(r"(\d+)\s*([hdwm])", lowered)
    if not interval_match:
        return value

    amount = interval_match.group(1)
    unit = interval_match.group(2)
    suffix = {
        "h": "hour",
        "d": "day",
        "w": "week",
        "m": "month",
    }[unit]
    return f"{amount} {suffix}"


async def dismiss_coinglass_popups(page) -> None:
    """Dismiss consent/login overlays that block the underlying page."""
    for selector in (
        'button:has-text("Consent")',
        'button:has-text("Do not consent")',
        'button[aria-label="Close"]',
        'button:has-text("Close")',
    ):
        try:
            locator = page.locator(selector).first
            if await locator.is_visible(timeout=1000):
                await locator.click(timeout=2000)
                await page.wait_for_timeout(300)
        except Exception:
            continue


async def fill_and_commit(locator, value: str) -> None:
    """Fill a field, then blur it so client-side validation re-runs."""
    await locator.click(timeout=5000)
    await locator.fill(value, timeout=10000)
    await locator.press("Tab", timeout=5000)


async def apply_coinglass_timeframe(target: CaptureTarget, page) -> bool:
    """Set the main Coinglass LiquidationMap timeframe via the visible dropdown."""
    desired = target.ui_timeframe
    if not desired:
        return False

    lowered_url = target.url.lower()
    if "/pro/futures/liquidationmap" not in lowered_url:
        return False

    comboboxes = page.locator('[role="combobox"]')
    combobox_count = await comboboxes.count()
    timeframe_index = None
    current_value = None

    for idx in range(combobox_count):
        try:
            text = (await comboboxes.nth(idx).inner_text()).strip()
        except Exception:
            continue
        if re.fullmatch(r"\d+\s+(day|week|month|hour)s?", text, flags=re.IGNORECASE):
            timeframe_index = idx
            current_value = text
            break

    if timeframe_index is None:
        return False

    if current_value and current_value.lower() == desired.lower():
        return True

    try:
        await comboboxes.nth(timeframe_index).click(timeout=5000)
    except Exception:
        return False

    await page.wait_for_timeout(500)

    option_selectors = (
        f'[role="option"]:has-text("{desired}")',
        f'[role="menuitem"]:has-text("{desired}")',
        f'li:has-text("{desired}")',
        f'div:has-text("{desired}")',
        f'button:has-text("{desired}")',
        f'text="{desired}"',
    )
    for selector in option_selectors:
        try:
            matches = page.locator(selector)
            for idx in range(await matches.count()):
                option = matches.nth(idx)
                if not await option.is_visible(timeout=500):
                    continue
                await option.click(timeout=3000)
                await page.wait_for_timeout(1000)
                return True
        except Exception:
            continue

    return False


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
        summary: dict[str, Any] = {
            "kind": "object",
            "top_level_keys": sorted(str(key) for key in payload.keys())[:50],
        }
        # Preserve boolean success/code fields for downstream verification.
        if "success" in payload:
            summary["success"] = payload["success"]
        if "code" in payload:
            summary["code"] = payload["code"]
        return summary

    if isinstance(payload, list):
        arr_summary: dict[str, Any] = {
            "kind": "array",
            "length": len(payload),
        }
        if payload and isinstance(payload[0], dict):
            arr_summary["first_item_keys"] = sorted(str(key) for key in payload[0].keys())[:50]
        return arr_summary

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
    """Log in via CoinGlass's dedicated liqmap login page."""
    await page.goto(COINGLASS_LOGIN_URL, timeout=120000)
    await page.wait_for_load_state("load", timeout=30000)
    await dismiss_coinglass_popups(page)

    email_input = page.locator('input[name="email"]').first
    password_input = page.locator('input[name="password"]').first
    try:
        await email_input.wait_for(state="visible", timeout=10000)
        await password_input.wait_for(state="visible", timeout=10000)
    except Exception:
        return False

    try:
        await fill_and_commit(email_input, email)
        await fill_and_commit(password_input, password)
    except Exception:
        return False

    submit_button = page.locator('button:has-text("Login")').last

    for _ in range(10):
        try:
            if await submit_button.is_enabled():
                break
        except Exception:
            pass
        await page.wait_for_timeout(300)
    else:
        return False

    try:
        await submit_button.click(timeout=5000)
    except Exception:
        try:
            await password_input.press("Enter")
        except Exception:
            return False

    for _ in range(30):
        await page.wait_for_timeout(500)
        try:
            if "/login" not in page.url.lower():
                return True
            if not await password_input.is_visible():
                return True
        except Exception:
            return True

    return False


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


async def coinglass_direct_fetch(
    target: CaptureTarget,
    page,
    captured: list[dict[str, Any]],
) -> bool:
    """Rewrite the CoinGlass liqMap request via Playwright route interception.

    After browser login, intercepts the page's own ``liqMap`` API request using
    ``page.route()`` and rewrites ``interval``, ``limit``, and ``data`` params.
    A ``page.reload()`` triggers the authenticated axios call which our handler
    intercepts.

    Returns True only if:
    - At least one liqMap request was intercepted and rewritten.
    - The corresponding response contains ``success: true`` with matching
      ``interval``/``limit``.
    """
    timeframe_key = target.ui_timeframe or "1d"
    interval, limit = resolve_coinglass_interval_limit(timeframe_key)
    data_param = generate_coinglass_data_param()

    data_encoded = quote(data_param, safe="")

    # Intercept and rewrite the page's own liqMap requests via Playwright
    # route API.  This lets us change the interval/limit without clicking the
    # dropdown while reusing the page's authenticated axios instance.
    rewritten = False

    async def rewrite_liqmap(route):
        nonlocal rewritten
        url = route.request.url
        new_url = re.sub(r"interval=[^&]+", f"interval={interval}", url)
        new_url = re.sub(r"limit=\d+", f"limit={limit}", new_url)
        new_url = re.sub(r"data=[^&]+", f"data={data_encoded}", new_url)
        rewritten = True
        await route.continue_(url=new_url)

    try:
        await page.route("**/api/index/5/liqMap*", rewrite_liqmap)
    except Exception as exc:
        print(f"  coinglass route setup error: {exc}")
        return False

    # Trigger a page refresh to re-fire the liqMap request, which our route
    # handler will intercept and rewrite with the desired params.
    try:
        await page.reload(timeout=30000)
        await page.wait_for_load_state("load", timeout=30000)
        await page.wait_for_timeout(3000)
    except Exception:
        pass

    # Clean up route
    try:
        await page.unroute("**/api/index/5/liqMap*", rewrite_liqmap)
    except Exception:
        pass

    if not rewritten:
        print("  coinglass route: no liqMap request intercepted")
        return False

    # Verify that the main response handler actually captured a successful
    # liqMap response with the expected interval/limit.
    verified = False
    for cap in captured:
        url = cap.get("source_url", "")
        if "liqMap" not in url:
            continue
        if f"interval={interval}" not in url:
            continue
        if f"limit={limit}" not in url:
            continue
        summary = cap.get("summary", {})
        if summary.get("success") is True:
            verified = True
            break

    if not verified:
        print(
            f"  coinglass route: request rewritten but no verified liqMap "
            f"response with interval={interval}&limit={limit} and success=true"
        )

    return verified


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
    timeframe_applied = False

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
        await context.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined});"
        )
        page = await context.new_page()

        try:
            if target.provider == "coinglass" and target.email and target.password:
                login_success = await coinglass_login(page, target.email, target.password)

            page.on("response", schedule_response)
            await page.goto(target.url, timeout=120000)
            await page.wait_for_load_state("load", timeout=30000)
            if target.provider != "coinglass":
                login_success = await maybe_log_in(target, page)

            if target.provider == "coinglass" and login_success:
                await dismiss_coinglass_popups(page)
                timeframe_applied = await coinglass_direct_fetch(
                    target, page, captured,
                )

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
        "requested_ui_timeframe": target.ui_timeframe,
        "timeframe_applied": timeframe_applied,
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


def _redact_url(url: str) -> str:
    """Strip the ``data`` query parameter from a CoinGlass URL for safe logging."""
    return re.sub(r"data=[^&]+", "data=REDACTED", url)


def capture_coinglass_rest(
    target: CaptureTarget,
    run_dir: Path,
) -> dict[str, Any]:
    """Capture CoinGlass liqMap via REST (no browser).

    Produces the same summary shape as ``capture_target()`` so downstream
    consumers (manifest, comparator) work unchanged.

    Sensitive auth material (tokens, ``data`` param) is redacted from the
    persisted summary/manifest.
    """
    provider_dir = run_dir / target.provider
    provider_dir.mkdir(parents=True, exist_ok=True)

    if not target.email or not target.password:
        raise RuntimeError(
            "COINGLASS_USER_LOGIN / COINGLASS_USER_PASSWORD required "
            "for REST capture"
        )

    token_info = coinglass_rest_login(target.email, target.password)
    access_token = token_info["accessToken"]

    timeframe_key = target.ui_timeframe or "1d"
    result = coinglass_rest_fetch_liqmap(access_token, timeframe_key)

    body = result["body"]
    body_text = json.dumps(body, indent=2, ensure_ascii=True)
    output_path = provider_dir / "01_liqmap.json"
    output_path.write_text(body_text, encoding="utf-8")

    interval, limit = resolve_coinglass_interval_limit(timeframe_key)
    payload_summary = summarize_json_payload(body)

    # Redact the URL for persistence (strip TOTP data param).
    safe_url = _redact_url(result["url"])

    captured = [
        {
            "provider": target.provider,
            "source_url": safe_url,
            "status": result["status"],
            "method": "GET",
            "content_type": "application/json",
            "response_headers": result["response_headers"],
            "request_headers": {},
            "request_post_data": "",
            "saved_file": str(output_path),
            "summary": payload_summary,
        }
    ]

    # Validate against the original (non-redacted) URL for interval/limit.
    raw_url = result["url"]
    verified = (
        payload_summary.get("success") is True
        and f"interval={interval}" in raw_url
        and f"limit={limit}" in raw_url
    )

    if not verified:
        raise RuntimeError(
            f"CoinGlass REST capture failed verification: "
            f"interval={interval}, limit={limit}, "
            f"success={payload_summary.get('success')}"
        )

    summary = {
        "provider": target.provider,
        "page_url": target.url,
        "login_attempted": True,
        "login_success": True,
        "requested_ui_timeframe": target.ui_timeframe,
        "timeframe_applied": verified,
        "capture_count": len(captured),
        "captures": captured,
        "capture_mode": "rest",
    }

    summary_path = provider_dir / "summary.json"
    summary_path.write_text(
        json.dumps(summary, indent=2, ensure_ascii=True), encoding="utf-8"
    )
    return summary


async def run_capture(args: argparse.Namespace, emit_progress: bool = True) -> Path:
    """Execute the capture workflow and return the manifest path."""
    targets = build_targets(args)
    run_dir = args.output_dir / utc_timestamp_slug()
    run_dir.mkdir(parents=True, exist_ok=True)

    cg_mode = getattr(args, "coinglass_mode", "browser")

    provider_summaries: list[dict[str, Any]] = []
    for target in targets:
        if emit_progress:
            mode_label = f" [{cg_mode}]" if target.provider == "coinglass" else ""
            print(f"capturing {target.provider}{mode_label}: {target.url}")

        summary: dict[str, Any] | None = None

        # CoinGlass REST or auto path
        if target.provider == "coinglass" and cg_mode in ("rest", "auto"):
            try:
                summary = capture_coinglass_rest(target, run_dir)
                if emit_progress:
                    print(
                        f"saved {summary['capture_count']} JSON responses "
                        f"for {target.provider} [rest]"
                    )
            except Exception as exc:
                if cg_mode == "rest":
                    raise
                if emit_progress:
                    print(
                        f"  REST failed ({exc}), falling back to browser"
                    )
                summary = None  # fall through to browser path

        # Browser path (default, or auto fallback)
        if summary is None:
            summary = await capture_target(
                target=target,
                run_dir=run_dir,
                max_responses=args.max_responses,
                post_load_wait_ms=args.post_load_wait_ms,
                headless=not args.headed,
            )
            if "capture_mode" not in summary:
                summary["capture_mode"] = "browser"
            if emit_progress:
                print(
                    f"saved {summary['capture_count']} JSON responses "
                    f"for {target.provider} [{summary.get('capture_mode', 'browser')}]"
                )

        provider_summaries.append(summary)

    manifest = {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "run_dir": str(run_dir),
        "args": {
            "provider": args.provider,
            "coin": args.coin,
            "timeframe": args.timeframe,
            "coinglass_timeframe": args.coinglass_timeframe,
            "coinglass_mode": cg_mode,
            "exchange": args.exchange,
            "max_responses": args.max_responses,
            "post_load_wait_ms": args.post_load_wait_ms,
        },
        "providers": provider_summaries,
        "comparison": build_run_comparison(provider_summaries),
    }
    manifest_path = run_dir / "manifest.json"
    manifest_path.write_text(
        json.dumps(manifest, indent=2, ensure_ascii=True), encoding="utf-8"
    )
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
