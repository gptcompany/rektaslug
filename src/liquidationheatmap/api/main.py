"""FastAPI application for liquidation heatmap API."""

import json
import logging
import os
import time
from collections import defaultdict
from decimal import Decimal
from typing import Literal, Optional
from urllib.request import urlopen

import numpy as np
from fastapi import FastAPI, HTTPException, Query, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from starlette.middleware.base import BaseHTTPMiddleware

# =============================================================================
# CACHING LAYER (T058-T060)
# =============================================================================


class HeatmapCache:
    """In-memory cache with TTL for heatmap timeseries responses.

    T058: Add in-memory cache layer with TTL to API endpoint
    T059: Implement cache-first query strategy
    T060: Add cache metrics logging for hit/miss ratio
    """

    def __init__(self, ttl_seconds: int = 300, max_size: int = 100):
        """Initialize cache.

        Args:
            ttl_seconds: Time-to-live for cache entries (default: 5 minutes)
            max_size: Maximum number of cache entries
        """
        self.ttl_seconds = ttl_seconds
        self.max_size = max_size
        self._cache: dict[str, tuple[float, any]] = {}  # key -> (expiry_time, value)
        self._hits = 0
        self._misses = 0

    def _make_key(
        self,
        symbol: str,
        start_time: Optional[str],
        end_time: Optional[str],
        interval: str,
        price_bin_size: float,
        leverage_weights: Optional[str],
    ) -> str:
        """Create cache key from request parameters."""
        return f"{symbol}:{start_time}:{end_time}:{interval}:{price_bin_size}:{leverage_weights}"

    def get(
        self,
        symbol: str,
        start_time: Optional[str],
        end_time: Optional[str],
        interval: str,
        price_bin_size: float,
        leverage_weights: Optional[str],
    ) -> Optional[any]:
        """Get cached response if exists and not expired."""
        key = self._make_key(
            symbol, start_time, end_time, interval, price_bin_size, leverage_weights
        )
        if key in self._cache:
            expiry, value = self._cache[key]
            if time.time() < expiry:
                self._hits += 1
                return value
            else:
                # Expired, remove from cache
                del self._cache[key]

        self._misses += 1
        return None

    def set(
        self,
        symbol: str,
        start_time: Optional[str],
        end_time: Optional[str],
        interval: str,
        price_bin_size: float,
        leverage_weights: Optional[str],
        value: any,
    ) -> None:
        """Store response in cache."""
        # Evict oldest entries if at max size
        if len(self._cache) >= self.max_size:
            oldest_key = min(self._cache.keys(), key=lambda k: self._cache[k][0])
            del self._cache[oldest_key]

        key = self._make_key(
            symbol, start_time, end_time, interval, price_bin_size, leverage_weights
        )
        expiry = time.time() + self.ttl_seconds
        self._cache[key] = (expiry, value)

    def get_stats(self) -> dict:
        """Get cache statistics."""
        total = self._hits + self._misses
        hit_rate = (self._hits / total * 100) if total > 0 else 0.0
        return {
            "hits": self._hits,
            "misses": self._misses,
            "total_requests": total,
            "hit_rate_percent": round(hit_rate, 2),
            "cached_entries": len(self._cache),
            "max_size": self.max_size,
            "ttl_seconds": self.ttl_seconds,
        }

    def clear(self) -> None:
        """Clear all cached entries."""
        self._cache.clear()
        self._hits = 0
        self._misses = 0


# Global heatmap cache instance (TTL from env or default 5 minutes)
_heatmap_cache = HeatmapCache(
    ttl_seconds=int(os.getenv("LH_CACHE_TTL", "300")),
    max_size=int(os.getenv("LH_CACHE_MAX_SIZE", "100")),
)


# Simple rate limiter (in-memory, suitable for single-server deployments)
class SimpleRateLimiter:
    """Simple in-memory rate limiter using sliding window."""

    def __init__(self, requests_per_minute: int = 60):
        self.requests_per_minute = requests_per_minute
        self.window_size = 60  # 1 minute
        self.requests: dict[str, list[float]] = defaultdict(list)

    def is_allowed(self, ip: str) -> tuple[bool, int]:
        """Check if request from IP is allowed. Returns (allowed, retry_after)."""
        now = time.time()
        cutoff = now - self.window_size

        # Clean old requests for this IP
        self.requests[ip] = [t for t in self.requests[ip] if t > cutoff]

        # Check limit
        if len(self.requests[ip]) >= self.requests_per_minute:
            oldest = min(self.requests[ip]) if self.requests[ip] else now
            retry_after = int(oldest + self.window_size - now) + 1
            return False, retry_after

        # Record request
        self.requests[ip].append(now)

        # Periodic cleanup: remove stale IPs to prevent memory leak
        # Only run cleanup every ~100 requests to avoid performance impact
        if len(self.requests) > 1000:
            stale_ips = [ip_key for ip_key, timestamps in self.requests.items() if not timestamps]
            for stale_ip in stale_ips:
                del self.requests[stale_ip]

        return True, 0


# Global rate limiter instance
_rate_limiter = SimpleRateLimiter(requests_per_minute=int(os.getenv("RATE_LIMIT_RPM", "120")))


class RateLimitMiddleware(BaseHTTPMiddleware):
    """Middleware to enforce rate limiting."""

    async def dispatch(self, request: Request, call_next):
        # Skip rate limiting if disabled
        if os.getenv("RATE_LIMIT_ENABLED", "true").lower() == "false":
            return await call_next(request)

        # Skip health endpoint
        if request.url.path == "/health":
            return await call_next(request)

        client_ip = request.client.host if request.client else "unknown"
        allowed, retry_after = _rate_limiter.is_allowed(client_ip)

        if not allowed:
            return JSONResponse(
                status_code=429,
                content={
                    "error": "Too Many Requests",
                    "message": f"Rate limit exceeded. Try again in {retry_after} seconds.",
                    "retry_after": retry_after,
                },
                headers={"Retry-After": str(retry_after)},
            )

        response = await call_next(request)
        return response


def get_cors_origins() -> list[str]:
    """Get CORS allowed origins from environment.

    In production, set CORS_ALLOWED_ORIGINS to comma-separated list of origins.
    Example: CORS_ALLOWED_ORIGINS=https://app.example.com,https://admin.example.com

    Returns:
        List of allowed origins. Defaults to ["*"] for development.
    """
    origins_env = os.getenv("CORS_ALLOWED_ORIGINS", "")
    if origins_env:
        return [origin.strip() for origin in origins_env.split(",") if origin.strip()]
    # Development default - allow all origins
    return ["*"]


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

from ..ingestion.db_service import DuckDBService
from ..models.binance_standard import BinanceStandardModel
from ..models.ensemble import EnsembleModel
from ..models.funding_adjusted import FundingAdjustedModel

# Supported trading pairs (whitelist)
SUPPORTED_SYMBOLS = {
    "BTCUSDT",
    "ETHUSDT",
    "BNBUSDT",
    "ADAUSDT",
    "DOGEUSDT",
    "XRPUSDT",
    "SOLUSDT",
    "DOTUSDT",
    "MATICUSDT",
    "LINKUSDT",
}

# =============================================================================
# EXCHANGE CONFIGURATION (Feature 012 - Exchange Aggregation)
# =============================================================================

# Supported exchanges for multi-exchange aggregation
SUPPORTED_EXCHANGES = {
    "binance": {
        "name": "binance",
        "display_name": "Binance Futures",
        "status": "active",
        "features": ["liquidations", "open_interest", "funding_rate"],
        "data_source": "REST polling (5s)",
    },
    "hyperliquid": {
        "name": "hyperliquid",
        "display_name": "Hyperliquid",
        "status": "active",
        "features": ["liquidations"],
        "data_source": "WebSocket",
    },
    "bybit": {
        "name": "bybit",
        "display_name": "Bybit",
        "status": "stub",
        "features": [],
        "data_source": "Not implemented",
    },
}

# Exchange health cache (10s TTL per T066)
_exchange_health_cache: dict = {}
_exchange_health_cache_time: float = 0.0
EXCHANGE_HEALTH_CACHE_TTL = 10.0  # seconds

# =============================================================================
# TIME WINDOW CONFIGURATION - Extended Timeframes
# =============================================================================
# Each time window auto-selects optimal klines interval for ~100-365 candles
TimeWindow = Literal["48h", "3d", "7d", "14d", "30d", "60d", "90d", "180d", "1y"]

TIME_WINDOW_CONFIG: dict[str, dict] = {
    "48h": {"hours": 48, "klines_interval": "30m", "agg_minutes": 30},
    "3d": {"hours": 72, "klines_interval": "1h", "agg_minutes": 60},
    "7d": {"hours": 168, "klines_interval": "1h", "agg_minutes": 60},
    "14d": {"hours": 336, "klines_interval": "2h", "agg_minutes": 120},
    "30d": {"hours": 720, "klines_interval": "4h", "agg_minutes": 240},
    "60d": {"hours": 1440, "klines_interval": "8h", "agg_minutes": 480},
    "90d": {"hours": 2160, "klines_interval": "12h", "agg_minutes": 720},
    "180d": {"hours": 4320, "klines_interval": "1d", "agg_minutes": 1440},
    "1y": {"hours": 8760, "klines_interval": "1d", "agg_minutes": 1440},
}

app = FastAPI(
    title="Liquidation Heatmap API",
    description="Calculate and visualize cryptocurrency liquidation levels",
    version="0.1.0",
)

# Rate limiting middleware (configurable via RATE_LIMIT_RPM and RATE_LIMIT_ENABLED env vars)
app.add_middleware(RateLimitMiddleware)

# CORS middleware (configurable via CORS_ALLOWED_ORIGINS env var)
app.add_middleware(
    CORSMiddleware,
    allow_origins=get_cors_origins(),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Register API routers
from src.liquidationheatmap.api.routers import signals_router

app.include_router(signals_router)

# Mount static files for frontend dashboards
app.mount("/frontend", StaticFiles(directory="frontend"), name="frontend")


@app.get("/coinglass")
async def coinglass_redirect():
    """Redirect /coinglass to the actual heatmap page."""
    from fastapi.responses import RedirectResponse

    return RedirectResponse(url="/frontend/coinglass_heatmap.html")


@app.get("/heatmap_30d.html")
async def heatmap_30d_page():
    """Serve the single-timeframe validation page at a stable root URL."""
    return FileResponse("frontend/heatmap_30d.html")


@app.get("/liq_map_1w.html")
async def liq_map_1w_page():
    """Serve the 1-week liquidation map validation page."""
    return FileResponse("frontend/liq_map_1w.html")


class LiquidationResponse(BaseModel):
    """Response model for liquidations endpoint."""

    symbol: str
    model: str
    current_price: Decimal
    long_liquidations: list
    short_liquidations: list


@app.get("/health")
async def health_check():
    """Health check endpoint.

    Returns:
        dict: Status of the API
    """
    return {"status": "ok", "service": "liquidation-heatmap"}


# =============================================================================
# EXCHANGE ENDPOINTS (Feature 012 - T065-T067)
# =============================================================================


def validate_exchanges(exchanges_str: Optional[str]) -> list[str]:
    """Validate and parse comma-separated exchange list.

    T061: Validate exchange names against supported list

    Args:
        exchanges_str: Comma-separated exchange names or None

    Returns:
        List of validated exchange names

    Raises:
        HTTPException: If any exchange name is invalid
    """
    if not exchanges_str:
        return list(SUPPORTED_EXCHANGES.keys())

    # Filter out empty strings from splitting (handles "binance,", ",hyperliquid", "a,,b")
    exchanges = [e.strip().lower() for e in exchanges_str.split(",") if e.strip()]

    # Handle case where all entries were empty (e.g., ",,,")
    if not exchanges:
        return list(SUPPORTED_EXCHANGES.keys())

    invalid = [e for e in exchanges if e not in SUPPORTED_EXCHANGES]

    if invalid:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid exchange(s): {invalid}. Supported: {list(SUPPORTED_EXCHANGES.keys())}",
        )

    return exchanges


async def get_exchange_health() -> dict:
    """Get health status for all exchanges with 10s caching.

    T066: Add 10s caching to health endpoint

    Returns:
        Dict of exchange name to health status
    """
    global _exchange_health_cache, _exchange_health_cache_time

    # Check cache validity
    now = time.time()
    if _exchange_health_cache and (now - _exchange_health_cache_time) < EXCHANGE_HEALTH_CACHE_TTL:
        return _exchange_health_cache

    # Build health status from database or adapters
    health_status = {}
    db = DuckDBService(read_only=True)

    try:
        # Try to get health from exchange_health table
        try:
            result = db.conn.execute("""
                SELECT exchange, is_connected, last_heartbeat, message_count,
                       error_count, uptime_percent, last_error
                FROM exchange_health
            """).fetchall()

            for row in result:
                health_status[row[0]] = {
                    "exchange": row[0],
                    "is_connected": row[1],
                    "last_heartbeat": row[2].isoformat() if row[2] else None,
                    "message_count": row[3],
                    "error_count": row[4],
                    "uptime_percent": float(row[5]) if row[5] else 0.0,
                    "last_error": row[6],
                }
        except Exception as e:
            logger.debug(f"exchange_health table not available: {e}")

        # Fill in missing exchanges with default status
        for exchange_name, exchange_info in SUPPORTED_EXCHANGES.items():
            if exchange_name not in health_status:
                health_status[exchange_name] = {
                    "exchange": exchange_name,
                    "is_connected": exchange_info["status"] == "active",
                    "last_heartbeat": None,
                    "message_count": 0,
                    "error_count": 0,
                    "uptime_percent": 100.0 if exchange_info["status"] == "active" else 0.0,
                    "last_error": None if exchange_info["status"] != "stub" else "Not implemented",
                }

    finally:
        db.close()


    # Update cache
    _exchange_health_cache = health_status
    _exchange_health_cache_time = now

    return health_status


@app.get("/exchanges")
async def list_exchanges():
    """List all supported exchanges with their status.

    T067: Add GET /exchanges list endpoint

    Returns:
        List of exchanges with display names, status, and features
    """
    exchanges = []
    for name, info in SUPPORTED_EXCHANGES.items():
        exchanges.append(
            {
                "name": name,
                "display_name": info["display_name"],
                "status": info["status"],
                "features": info["features"],
                "data_source": info["data_source"],
            }
        )

    return {
        "exchanges": exchanges,
        "count": len(exchanges),
        "active_count": sum(1 for e in exchanges if e["status"] == "active"),
    }


@app.get("/exchanges/health")
async def get_exchanges_health():
    """Get health status for all exchange adapters.

    T065: Add GET /exchanges/health endpoint
    T066: Add 10s caching to health endpoint

    Returns health metrics including connection status, message counts,
    error rates, and uptime percentage.

    Returns:
        Dict of exchange name to health metrics
    """
    health = await get_exchange_health()
    return {
        "exchanges": health,
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "cache_ttl_seconds": EXCHANGE_HEALTH_CACHE_TTL,
    }


@app.post("/api/v1/prepare-for-ingestion")
async def prepare_for_ingestion():
    """Prepare database for external ingestion by closing all read connections.

    Call this endpoint before running N8N ingestion workflows to release
    any database locks held by the API.

    Returns:
        dict: Status of connection cleanup with count of closed instances
    """
    import gc

    result = {
        "status": "success",
        "connections_closed": 0,
        "details": [],
    }

    try:
        # Set ingestion lock FIRST to prevent new connections
        if DuckDBService.set_ingestion_lock():
            result["details"].append("Ingestion lock acquired")
        else:
            result["details"].append("Warning: Failed to acquire ingestion lock")

        # Close all read-only singleton instances
        closed = DuckDBService.close_all_instances()
        result["connections_closed"] = closed
        result["details"].append(f"Closed {closed} DuckDBService instances")

        # Force garbage collection to release any lingering handles
        gc.collect()
        result["details"].append("Garbage collection completed")

        logger.info(f"Prepared for ingestion: closed {closed} DB connections, lock acquired")
        return result
    except Exception as e:
        logger.error(f"Failed to prepare for ingestion: {e}")
        # Release lock on error
        DuckDBService.release_ingestion_lock()
        result["status"] = "error"
        result["error"] = str(e)
        return result


@app.post("/api/v1/refresh-connections")
async def refresh_connections():
    """Re-establish database connections after ingestion completes.

    Call this endpoint after N8N ingestion workflows finish to warm up
    connections for subsequent API requests.

    Returns:
        dict: Status of connection refresh
    """
    result = {
        "status": "success",
        "details": [],
    }

    try:
        # Release ingestion lock FIRST to allow new connections
        if DuckDBService.release_ingestion_lock():
            result["details"].append("Ingestion lock released")
        else:
            result["details"].append("Warning: Failed to release ingestion lock")

        # Create a new read-only instance to warm up connections
        db = DuckDBService(read_only=True)

        # Verify connection works
        test_result = db.conn.execute("SELECT 1 as test").fetchone()
        if test_result:
            result["details"].append("Database connection verified")

        logger.info("Database connections refreshed successfully")
        return result
    except Exception as e:
        logger.error(f"Failed to refresh connections: {e}")
        result["status"] = "error"
        result["error"] = str(e)
        return result


@app.get("/data/date-range")
async def get_data_date_range(
    symbol: str = Query(
        "BTCUSDT",
        description="Trading pair symbol",
        pattern="^[A-Z]{6,12}$",
    ),
):
    """Get the available date range for heatmap data.

    Returns the earliest and latest timestamps available in the database
    for the specified symbol. Useful for frontend to set dynamic date ranges.

    Args:
        symbol: Trading pair (e.g., BTCUSDT)

    Returns:
        dict: start_date, end_date in ISO format
    """
    if symbol not in SUPPORTED_SYMBOLS:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid symbol '{symbol}'. Supported: {sorted(SUPPORTED_SYMBOLS)}",
        )

    db = DuckDBService(read_only=True)

    try:
        # Get date range from Open Interest data (most comprehensive)
        query = """
        SELECT
            MIN(timestamp) as start_date,
            MAX(timestamp) as end_date
        FROM open_interest_history
        WHERE symbol = ?
        """
        result = db.conn.execute(query, [symbol]).fetchone()

        if result and result[0] and result[1]:
            return {
                "symbol": symbol,
                "start_date": result[0].isoformat(),
                "end_date": result[1].isoformat(),
            }
        else:
            raise HTTPException(
                status_code=404,
                detail=f"No data found for symbol '{symbol}'",
            )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching date range: {e}")
        raise HTTPException(status_code=500, detail=f"Database error: {e}")


@app.get("/cache/stats")
async def get_cache_stats():
    """Get heatmap cache statistics (T060).

    Returns cache hit/miss ratio and other metrics for monitoring.

    Returns:
        dict: Cache statistics including hit rate
    """
    return _heatmap_cache.get_stats()


@app.delete("/cache/clear")
async def clear_cache():
    """Clear the heatmap cache.

    Useful for forcing recalculation after data updates.

    Returns:
        dict: Confirmation message
    """
    _heatmap_cache.clear()
    return {"message": "Cache cleared", "status": "ok"}


@app.get(
    "/liquidations/levels",
    response_model=LiquidationResponse,
    deprecated=True,
    description="**DEPRECATED**: Use `/liquidations/heatmap-timeseries` instead for "
    "time-evolving heatmap with correct position lifecycle modeling.",
)
async def get_liquidation_levels(
    response: Response,
    symbol: str = Query(
        ...,
        description="Trading pair symbol (e.g., BTCUSDT, ETHUSDT)",
        pattern="^[A-Z]{6,12}$",
        examples=["BTCUSDT", "ETHUSDT"],
    ),
    model: str = Query(
        "openinterest",
        description="Calculation model (reserved for future extensions, currently only 'openinterest' supported)",
    ),
    timeframe: int = Query(
        ..., ge=1, le=365, description="Timeframe in days (1-365)", examples=[30]
    ),
    whale_threshold: float = Query(
        500000.0,
        description="Minimum trade size in USD (CURRENTLY FIXED AT $500K - parameter non-functional due to pre-aggregated cache limitation)",
        ge=0.0,
    ),
):
    """Calculate liquidation levels using Open Interest distribution model.

    **DEPRECATED**: This endpoint uses static liquidation levels that do not
    account for position consumption when price crosses them. Use the
    `/liquidations/heatmap-timeseries` endpoint instead for accurate
    time-evolving heatmap data.

    Returns liquidations BELOW current price (long positions) and
    ABOVE current price (short positions).

    The OI-based model distributes current Open Interest across historical
    volume profile, providing more realistic liquidation estimates than
    legacy trade-counting approaches.

    Args:
        symbol: Trading pair (default: BTCUSDT)
        model: Calculation model (currently only 'openinterest')
        timeframe: Lookback period in days (1, 7, 30, or 90)
        whale_threshold: Minimum position size filter

    Returns:
        LiquidationResponse with long and short liquidations
    """
    # Add deprecation warning header
    response.headers["Deprecation"] = "true"
    response.headers["Sunset"] = "2025-06-01"
    response.headers["Link"] = '</liquidations/heatmap-timeseries>; rel="successor-version"'
    # Validate symbol against whitelist
    if symbol not in SUPPORTED_SYMBOLS:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid symbol '{symbol}'. Supported symbols: {sorted(SUPPORTED_SYMBOLS)}",
        )

    # Get real-time current price from Binance API

    try:
        with urlopen(
            f"https://api.binance.com/api/v3/ticker/price?symbol={symbol}", timeout=5
        ) as resp:
            data = json.loads(resp.read().decode())
            current_price = float(data["price"])
    except Exception as e:
        # Fallback to historical price if API fails
        logging.warning(f"Binance API price fetch failed for {symbol}: {e}")
        with DuckDBService(read_only=True) as db_fallback:
            price_decimal, _ = db_fallback.get_latest_open_interest(symbol)
            current_price = float(price_decimal)

    # Dynamic bin size based on timeframe (Coinglass approach)
    if timeframe <= 7:
        bin_size = 200.0  # 7d: High granularity
    elif timeframe <= 30:
        bin_size = 500.0  # 30d: Medium granularity
    else:  # 90 days
        bin_size = 1500.0  # 90d: Low granularity

    # Calculate liquidations using OI-based model
    with DuckDBService(read_only=True) as db:
        # OI-based model: distributes current Open Interest based on volume profile
        bins_df = db.calculate_liquidations_oi_based(
            symbol=symbol,
            current_price=current_price,
            bin_size=bin_size,
            lookback_days=timeframe,
            whale_threshold=whale_threshold,
        )

        # Bin liquidation prices and aggregate
        if not bins_df.empty and "liq_price" in bins_df.columns:
            # CRITICAL: Use liq_price (actual liquidation levels), not price_bucket (entry prices)
            bins_df["liq_price_binned"] = (
                np.round(bins_df["liq_price"] / bin_size) * bin_size
            ).astype(int)

            # Aggregate by binned liquidation price, leverage, and side
            bins_df = (
                bins_df.groupby(["liq_price_binned", "leverage", "side"])
                .agg({"volume": "sum"})
                .reset_index()
            )
            bins_df["count"] = 1  # Placeholder count
            bins_df.rename(
                columns={"volume": "total_volume", "liq_price_binned": "price_bucket"},
                inplace=True,
            )

    logger.info(f"SQL returned {len(bins_df)} aggregated bins")

    # Convert DataFrame to API response format
    long_liqs = []
    short_liqs = []

    for _, row in bins_df.iterrows():
        liq_entry = {
            "price_level": str(row["price_bucket"]),
            "volume": str(row["total_volume"]),
            "count": int(row["count"]),
            "leverage": f"{int(row['leverage'])}x",
        }

        if row["side"] == "buy":  # Long positions
            long_liqs.append(liq_entry)
        else:  # Short positions
            short_liqs.append(liq_entry)

    # Sort: longs descending (high to low), shorts ascending (low to high)
    long_liqs = sorted(long_liqs, key=lambda x: float(x["price_level"]), reverse=True)
    short_liqs = sorted(short_liqs, key=lambda x: float(x["price_level"]))

    logger.info(f"Formatted response: {len(long_liqs)} long, {len(short_liqs)} short")

    return LiquidationResponse(
        symbol=symbol,
        model=model,
        current_price=str(current_price),
        long_liquidations=long_liqs,
        short_liquidations=short_liqs,
    )


@app.get("/liquidations/heatmap")
async def get_heatmap(
    symbol: str = Query(
        ...,
        description="Trading pair symbol (e.g., BTCUSDT, ETHUSDT)",
        pattern="^[A-Z]{6,12}$",
        examples=["BTCUSDT", "ETHUSDT"],
    ),
    model: Literal["binance_standard", "ensemble"] = Query(
        "ensemble", description="Liquidation model to use"
    ),
    timeframe: str = Query("1d", description="Time bucket size", pattern="^(1h|4h|12h|1d|7d|30d)$"),
):
    """Get pre-aggregated heatmap data for visualization.

    Returns heatmap buckets with density (count) and volume for each
    time+price bucket combination.

    Args:
        symbol: Trading pair (e.g., BTCUSDT)
        model: Liquidation model (binance_standard or ensemble)
        timeframe: Time bucket size (1h, 4h, 12h, 1d, 7d, 30d)

    Returns:
        HeatmapResponse with data points and metadata
    """
    # Validate symbol against whitelist
    if symbol not in SUPPORTED_SYMBOLS:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid symbol '{symbol}'. Supported symbols: {sorted(SUPPORTED_SYMBOLS)}",
        )

    from .heatmap_models import HeatmapDataPoint, HeatmapMetadata, HeatmapResponse

    # Connect to database
    db = DuckDBService(read_only=True)

    try:
        # Query liquidation_snapshots table (contains actual data)
        # Aggregate by timestamp and price_bucket, compute density and volume
        query = """
        SELECT
            timestamp as time_bucket,
            price_bucket,
            COUNT(*) as density,
            SUM(active_volume) as volume
        FROM liquidation_snapshots
        WHERE symbol = ?
        GROUP BY timestamp, price_bucket
        ORDER BY timestamp, price_bucket
        """

        try:
            df = db.conn.execute(query, [symbol]).df()
        except Exception:
            # Table may not exist yet (needs snapshot ingestion)
            import pandas as pd
            df = pd.DataFrame()

        if df.empty:
            # Return empty response if no data
            return HeatmapResponse(
                symbol=symbol,
                model=model,
                timeframe=timeframe,
                current_price=None,
                data=[],
                metadata=HeatmapMetadata(
                    total_volume=0.0,
                    highest_density_price=0.0,
                    num_buckets=0,
                    data_quality_score=0.0,
                    time_range_hours=0.0,
                ),
            )

        # Convert to HeatmapDataPoint objects
        data_points = [
            HeatmapDataPoint(
                time=row["time_bucket"],
                price_bucket=float(row["price_bucket"]),
                density=int(row["density"]),
                volume=float(row["volume"]),
            )
            for _, row in df.iterrows()
        ]

        # Calculate metadata
        total_volume = float(df["volume"].sum())
        highest_density_idx = df["density"].idxmax()
        highest_density_price = float(df.loc[highest_density_idx, "price_bucket"])
        num_buckets = len(df)

        # Calculate time range
        time_range = df["time_bucket"].max() - df["time_bucket"].min()
        time_range_hours = time_range.total_seconds() / 3600 if time_range else 0.0

        # Simple data quality score (1.0 = complete, lower = gaps)
        # Could be enhanced with gap detection
        data_quality_score = min(1.0, num_buckets / max(1, time_range_hours))

        # Get current price (from latest Open Interest data)
        current_price_query = """
        SELECT open_interest_value / open_interest_contracts AS price
        FROM open_interest_history
        WHERE symbol = ?
        ORDER BY timestamp DESC
        LIMIT 1
        """
        current_price_result = db.conn.execute(current_price_query, [symbol]).fetchone()
        current_price = float(current_price_result[0]) if current_price_result else None

        metadata = HeatmapMetadata(
            total_volume=total_volume,
            highest_density_price=highest_density_price,
            num_buckets=num_buckets,
            data_quality_score=data_quality_score,
            time_range_hours=time_range_hours,
        )

        return HeatmapResponse(
            symbol=symbol,
            model=model,
            timeframe=timeframe,
            current_price=current_price,
            data=data_points,
            metadata=metadata,
        )

    finally:
        db.close()




@app.get("/liquidations/history")
async def get_liquidation_history(
    symbol: str = Query("BTCUSDT", description="Trading pair symbol"),
    start: Optional[str] = Query(None, description="Start datetime (ISO format)"),
    end: Optional[str] = Query(None, description="End datetime (ISO format)"),
    aggregate: bool = Query(False, description="Aggregate by timestamp and side"),
):
    """Get historical liquidation data from database (T047).

    Query actual liquidation events stored in liquidation_history table.
    Supports date filtering and aggregation for time-series analysis.

    Args:
        symbol: Trading pair (e.g., BTCUSDT)
        start: Optional start datetime filter
        end: Optional end datetime filter
        aggregate: If true, group by timestamp and side with totals

    Returns:
        List of historical liquidation records or aggregated data
    """
    db = DuckDBService(read_only=True)

    try:
        # Check if liquidation_history table exists
        table_check = db.conn.execute("""
            SELECT COUNT(*) as count
            FROM information_schema.tables
            WHERE table_name = 'liquidation_history'
        """).fetchone()

        # If table doesn't exist, return empty list
        if table_check[0] == 0:
            return []

        if aggregate:
            # Aggregated query for time-series visualization
            query = """
            SELECT
                timestamp,
                side,
                SUM(quantity) as total_volume,
                COUNT(*) as num_levels,
                AVG(price) as avg_price
            FROM liquidation_history
            WHERE symbol = ?
            """

            params = [symbol]

            if start:
                query += " AND timestamp >= ?"
                params.append(start)

            if end:
                query += " AND timestamp <= ?"
                params.append(end)

            query += " GROUP BY timestamp, side ORDER BY timestamp, side"

            df = db.conn.execute(query, params).df()

            return [
                {
                    "timestamp": str(rec["timestamp"]),
                    "side": rec["side"],
                    "total_volume": float(rec["total_volume"]),
                    "num_levels": int(rec["num_levels"]),
                    "avg_price": float(rec["avg_price"]),
                }
                for rec in df.to_dict(orient="records")
            ]

        else:
            # Raw historical records
            query = """
            SELECT timestamp, symbol, price, quantity, side, leverage, model
            FROM liquidation_history
            WHERE symbol = ?
            """

            params = [symbol]

            if start:
                query += " AND timestamp >= ?"
                params.append(start)

            if end:
                query += " AND timestamp <= ?"
                params.append(end)

            query += " ORDER BY timestamp DESC, side, leverage"

            df = db.conn.execute(query, params).df()

            return [
                {
                    "timestamp": str(rec["timestamp"]),
                    "symbol": rec["symbol"],
                    "price": float(rec["price"]),
                    "quantity": float(rec["quantity"]),
                    "side": rec["side"],
                    "leverage": int(rec["leverage"]),
                    "model": rec["model"],
                }
                for rec in df.to_dict(orient="records")
            ]

    finally:
        db.close()


@app.get("/liquidations/compare-models")
async def compare_models(
    symbol: str = Query("BTCUSDT", description="Trading pair symbol"),
):
    """Compare predictions from all models (T036).

    Returns predictions from binance_standard, funding_adjusted, and ensemble models.
    """
    # Fetch data from DuckDB
    with DuckDBService(read_only=True) as db:
        current_price, open_interest = db.get_latest_open_interest(symbol)
        _ = db.get_latest_funding_rate(symbol)  # Reserved for future use

    # Initialize all 3 models
    binance_model = BinanceStandardModel()
    funding_model = FundingAdjustedModel()
    ensemble_model = EnsembleModel()

    models_data = []

    # Run each model and collect results
    for model in [binance_model, funding_model, ensemble_model]:
        liquidations = model.calculate_liquidations(
            current_price=current_price,
            open_interest=open_interest,
            symbol=symbol,
        )

        # Calculate average confidence
        if liquidations:
            avg_conf = sum(liq.confidence for liq in liquidations) / len(liquidations)
        else:
            avg_conf = Decimal("0")

        # Format levels for response
        levels = [
            {
                "price_level": str(liq.price_level),
                "volume": str(liq.liquidation_volume),
                "leverage": liq.leverage_tier,
                "confidence": str(liq.confidence),
                "side": liq.side,
            }
            for liq in liquidations
        ]

        models_data.append(
            {
                "name": model.model_name,
                "levels": levels,
                "avg_confidence": avg_conf,
            }
        )

    # Calculate agreement percentage (simplified)
    # Check if models agree within 5% on average liquidation prices
    if len(models_data) >= 2:
        # Get average liquidation prices from each model
        avg_prices = []
        for model_data in models_data:
            if model_data["levels"]:
                prices = [Decimal(level["price_level"]) for level in model_data["levels"]]
                avg_prices.append(sum(prices) / len(prices))

        if len(avg_prices) >= 2:
            # Calculate coefficient of variation
            mean_price = sum(avg_prices) / len(avg_prices)
            if mean_price > 0:
                std_dev = (
                    sum((p - mean_price) ** 2 for p in avg_prices) / len(avg_prices)
                ) ** Decimal("0.5")
                cv = (std_dev / mean_price) * 100
                # Agreement is high when CV is low
                agreement = max(Decimal("0"), 100 - (cv * 20))  # Scale CV to 0-100
            else:
                agreement = Decimal("0")
        else:
            agreement = Decimal("100")
    else:
        agreement = Decimal("100")

    return {
        "symbol": symbol,
        "current_price": current_price,
        "models": models_data,
        "agreement_percentage": agreement,
    }


@app.get("/prices/klines")
async def get_klines(
    symbol: str = Query(
        "BTCUSDT",
        description="Trading pair symbol",
        pattern="^[A-Z]{6,12}$",
    ),
    interval: Literal["5m", "15m", "30m", "1h", "2h", "4h", "8h", "12h", "1d"] = Query(
        "15m",
        description="Kline interval. 5m/15m direct from tables; larger intervals aggregated from 15m (or 5m for 30m).",
    ),
    limit: int = Query(100, ge=10, le=2000, description="Number of klines to return"),
    start_time: Optional[str] = Query(
        None, description="Start datetime (ISO format) - when provided, overrides limit"
    ),
    end_time: Optional[str] = Query(
        None, description="End datetime (ISO format) - when provided, overrides limit"
    ),
):
    """Get OHLC price data for visualization overlay.

    Returns candlestick/kline data from DuckDB for the specified symbol and interval.
    Used to overlay price action on the liquidation heatmap.

    Interval aggregation:
    - 5m, 15m: Direct query from respective tables
    - 30m: Aggregated from 5m data (6 candles)
    - 1h, 2h, 4h, 8h, 12h, 1d: Aggregated from 15m data

    When start_time and end_time are provided, the endpoint fetches all klines
    within that time range (ignoring limit). This allows the heatmap frontend
    to fetch klines matching exactly the heatmap data time range.

    Args:
        symbol: Trading pair (e.g., BTCUSDT)
        interval: Kline interval (5m, 15m, 30m, 1h, 2h, 4h, 8h, 12h, 1d)
        limit: Number of klines to return (10-2000, ignored when time range provided)
        start_time: Optional start datetime (ISO format)
        end_time: Optional end datetime (ISO format)

    Returns:
        List of OHLC data points with timestamp, open, high, low, close, volume
    """
    # Validate symbol
    if symbol not in SUPPORTED_SYMBOLS:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid symbol '{symbol}'. Supported: {sorted(SUPPORTED_SYMBOLS)}",
        )

    # Parse time range if provided
    from datetime import datetime

    start_dt = None
    end_dt = None
    use_time_range = start_time is not None and end_time is not None

    if use_time_range:
        try:
            start_dt = datetime.fromisoformat(start_time.replace("Z", "+00:00")).replace(
                tzinfo=None
            )
            end_dt = datetime.fromisoformat(end_time.replace("Z", "+00:00")).replace(tzinfo=None)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=f"Invalid time format: {e}")

    db = DuckDBService(read_only=True)

    try:
        # Map intervals to aggregation minutes (matches heatmap-timeseries logic)
        interval_agg_map = {
            "5m": 5,
            "15m": 15,
            "30m": 30,
            "1h": 60,
            "2h": 120,
            "4h": 240,
            "8h": 480,
            "12h": 720,
            "1d": 1440,
        }
        agg_minutes = interval_agg_map.get(interval, 15)

        # For intervals > 15m, aggregate from base tables
        # 30m uses 5m base table (6 candles), all others use 15m
        if agg_minutes > 15:
            base_table = "klines_5m_history" if interval == "30m" else "klines_15m_history"
            base_minutes = 5 if interval == "30m" else 15

            if use_time_range:
                # Time-range based query
                query = f"""
                WITH raw_klines AS (
                    SELECT
                        open_time as timestamp,
                        CAST(open AS DOUBLE) as open,
                        CAST(high AS DOUBLE) as high,
                        CAST(low AS DOUBLE) as low,
                        CAST(close AS DOUBLE) as close,
                        CAST(volume AS DOUBLE) as volume
                    FROM {base_table}
                    WHERE symbol = ? AND open_time >= ? AND open_time <= ?
                    ORDER BY open_time
                ),
                aggregated AS (
                    SELECT
                        time_bucket(INTERVAL '{agg_minutes} minutes', timestamp) as bucket,
                        FIRST(open ORDER BY timestamp) as open,
                        MAX(high) as high,
                        MIN(low) as low,
                        LAST(close ORDER BY timestamp) as close,
                        SUM(volume) as volume
                    FROM raw_klines
                    GROUP BY bucket
                    ORDER BY bucket
                )
                SELECT bucket as timestamp, open, high, low, close, volume
                FROM aggregated
                ORDER BY timestamp
                """
                df = db.conn.execute(query, [symbol, start_dt, end_dt]).df()
            else:
                # Limit-based query (original behavior)
                # Calculate raw_limit based on base table interval
                raw_limit = limit * (agg_minutes // base_minutes) + agg_minutes // base_minutes

                query = f"""
                WITH raw_klines AS (
                    SELECT
                        open_time as timestamp,
                        CAST(open AS DOUBLE) as open,
                        CAST(high AS DOUBLE) as high,
                        CAST(low AS DOUBLE) as low,
                        CAST(close AS DOUBLE) as close,
                        CAST(volume AS DOUBLE) as volume
                    FROM {base_table}
                    WHERE symbol = ?
                    ORDER BY open_time DESC
                    LIMIT ?
                ),
                aggregated AS (
                    SELECT
                        time_bucket(INTERVAL '{agg_minutes} minutes', timestamp) as bucket,
                        FIRST(open ORDER BY timestamp) as open,
                        MAX(high) as high,
                        MIN(low) as low,
                        LAST(close ORDER BY timestamp) as close,
                        SUM(volume) as volume
                    FROM raw_klines
                    GROUP BY bucket
                    ORDER BY bucket DESC
                    LIMIT ?
                )
                SELECT bucket as timestamp, open, high, low, close, volume
                FROM aggregated
                ORDER BY timestamp DESC
                """
                df = db.conn.execute(query, [symbol, raw_limit, limit]).df()
        else:
            # Direct query for 5m and 15m intervals
            table_name = f"klines_{interval}_history"

            if use_time_range:
                # Time-range based query
                query = f"""
                SELECT
                    open_time as timestamp,
                    CAST(open AS DOUBLE) as open,
                    CAST(high AS DOUBLE) as high,
                    CAST(low AS DOUBLE) as low,
                    CAST(close AS DOUBLE) as close,
                    CAST(volume AS DOUBLE) as volume
                FROM {table_name}
                WHERE symbol = ? AND open_time >= ? AND open_time <= ?
                ORDER BY open_time
                """
                df = db.conn.execute(query, [symbol, start_dt, end_dt]).df()
            else:
                # Limit-based query (original behavior)
                query = f"""
                SELECT
                    open_time as timestamp,
                    CAST(open AS DOUBLE) as open,
                    CAST(high AS DOUBLE) as high,
                    CAST(low AS DOUBLE) as low,
                    CAST(close AS DOUBLE) as close,
                    CAST(volume AS DOUBLE) as volume
                FROM {table_name}
                WHERE symbol = ?
                ORDER BY open_time DESC
                LIMIT ?
                """
                df = db.conn.execute(query, [symbol, limit]).df()

        if df.empty:
            return {"symbol": symbol, "interval": interval, "data": []}

        # Sort ascending for chart display
        df = df.sort_values("timestamp")

        klines = [
            {
                "timestamp": row["timestamp"].isoformat(),
                "open": row["open"],
                "high": row["high"],
                "low": row["low"],
                "close": row["close"],
                "volume": row["volume"],
            }
            for _, row in df.iterrows()
        ]

        return {
            "symbol": symbol,
            "interval": interval,
            "count": len(klines),
            "data": klines,
        }

    except Exception as e:
        logger.error(f"Error fetching klines: {e}")
        raise HTTPException(status_code=500, detail=f"Database error: {e}")

    finally:
        db.close()


# =============================================================================
# TIME-EVOLVING HEATMAP ENDPOINT (Feature 008)
# =============================================================================


class HeatmapLevel(BaseModel):
    """Single price level in heatmap snapshot."""

    price: float
    long_density: float
    short_density: float


class HeatmapSnapshotResponse(BaseModel):
    """Heatmap state at a single timestamp."""

    timestamp: str
    levels: list[HeatmapLevel]
    positions_created: int
    positions_consumed: int


class HeatmapTimeseriesMetadata(BaseModel):
    """Metadata for heatmap timeseries response."""

    symbol: str
    start_time: str
    end_time: str
    interval: str
    total_snapshots: int
    price_range: dict
    total_long_volume: float
    total_short_volume: float
    total_consumed: int


class HeatmapTimeseriesResponse(BaseModel):
    """Response model for time-evolving heatmap endpoint."""

    data: list[HeatmapSnapshotResponse]
    meta: HeatmapTimeseriesMetadata


class LeverageWeightsParseError(ValueError):
    """Raised when leverage_weights query param is invalid."""

    pass


# Valid leverage tiers (must match LiquidationLevel validation)
VALID_LEVERAGE_TIERS = {5, 10, 25, 50, 100}


def parse_leverage_weights(weights_str: str | None) -> list[tuple[int, Decimal]] | None:
    """Parse leverage weights from query string.

    Format: "5:15,10:30,25:25,50:20,100:10"

    Args:
        weights_str: Query string in format "leverage:weight,..."

    Returns:
        List of (leverage, weight) tuples, or None for defaults

    Raises:
        LeverageWeightsParseError: If the format is invalid or leverage values
            are not in valid tiers (5, 10, 25, 50, 100)
    """
    if not weights_str:
        return None

    try:
        weights = []
        total = Decimal("0")
        for pair in weights_str.split(","):
            parts = pair.split(":")
            if len(parts) != 2:
                raise LeverageWeightsParseError(
                    f"Invalid format: '{pair}'. Expected 'leverage:weight'"
                )
            leverage_str, weight_str = parts
            leverage = int(leverage_str)
            weight = Decimal(weight_str)

            # Validate leverage is a valid tier
            if leverage not in VALID_LEVERAGE_TIERS:
                raise LeverageWeightsParseError(
                    f"Invalid leverage '{leverage}'. Must be one of: {sorted(VALID_LEVERAGE_TIERS)}"
                )

            # Validate weight is non-negative
            if weight < 0:
                raise LeverageWeightsParseError(
                    f"Invalid weight '{weight}' for leverage {leverage}. Weights must be non-negative."
                )

            weights.append((leverage, weight))
            total += weight

        # Normalize weights to sum to 1.0
        if total > 0:
            weights = [(lev, w / total) for lev, w in weights]
        elif weights:
            # All zero weights - invalid
            raise LeverageWeightsParseError("All weights are zero. At least one must be positive.")

        return weights
    except LeverageWeightsParseError:
        raise
    except Exception as e:
        raise LeverageWeightsParseError(f"Invalid leverage_weights: {weights_str} - {e}")


@app.get("/liquidations/heatmap-timeseries", response_model=HeatmapTimeseriesResponse)
async def get_heatmap_timeseries(
    symbol: str = Query(
        ...,
        description="Trading pair symbol (e.g., BTCUSDT)",
        pattern="^[A-Z]{6,12}$",
        examples=["BTCUSDT", "ETHUSDT"],
    ),
    time_window: Optional[TimeWindow] = Query(
        None,
        description="Time window preset (auto-selects optimal klines interval). "
        "Options: 48h, 3d, 7d, 14d, 30d, 60d, 90d, 180d, 1y. "
        "Overrides start_time and interval if provided.",
    ),
    start_time: Optional[str] = Query(
        None,
        description="Start of time range (ISO 8601). Ignored if time_window is set.",
    ),
    end_time: Optional[str] = Query(
        None,
        description="End of time range (ISO 8601). Defaults to now.",
    ),
    interval: Optional[Literal["5m", "15m", "30m", "1h", "2h", "4h", "8h", "12h", "1d"]] = Query(
        None,
        description="Time interval for snapshots. Auto-selected if time_window is used.",
    ),
    price_bin_size: float = Query(
        100,
        ge=1,
        le=1000,
        description="Price bucket size in USD",
    ),
    leverage_weights: Optional[str] = Query(
        None,
        description="Custom leverage weights: '5:15,10:30,25:25,50:20,100:10'",
    ),
    exchanges: Optional[str] = Query(
        None,
        description="Comma-separated list of exchanges to include (e.g., 'binance,hyperliquid'). "
        "Default: all exchanges. T059-T061",
    ),
):
    """Get time-evolving liquidation heatmap.

    Returns a time series of liquidation density snapshots where liquidation
    levels are consumed when price crosses them. This is the new implementation
    that correctly models position lifecycle.

    **NEW**: Use `time_window` parameter for extended timeframes (48h to 1y).
    The optimal klines interval is auto-selected for each window.

    **CACHING**: Responses are cached for 5 minutes (configurable via LH_CACHE_TTL).
    Use GET /cache/stats to monitor cache performance.

    Args:
        symbol: Trading pair (e.g., BTCUSDT)
        time_window: Preset time window (48h, 3d, 7d, 14d, 30d, 60d, 90d, 180d, 1y)
        start_time: Start of time range (ISO 8601, ignored if time_window set)
        end_time: End of time range (ISO 8601, defaults to now)
        interval: Time interval for snapshots (auto-selected if time_window used)
        price_bin_size: Price bucket size in USD for aggregation
        leverage_weights: Custom leverage distribution weights
        exchanges: Comma-separated list of exchanges to filter (default: all)

    Returns:
        HeatmapTimeseriesResponse with snapshots and metadata
    """
    from dataclasses import dataclass
    from datetime import datetime, timedelta

    from ..models.time_evolving_heatmap import calculate_time_evolving_heatmap

    # T059-T061: Validate and parse exchanges parameter
    try:
        validated_exchanges = validate_exchanges(exchanges)
    except HTTPException:
        raise
    logger.debug(f"Using exchanges: {validated_exchanges}")

    # Resolve time_window to start_time and interval if provided
    effective_interval = interval
    effective_start_time = start_time
    if time_window:
        config = TIME_WINDOW_CONFIG[time_window]
        effective_interval = config["klines_interval"]
        # start_time will be calculated from hours below
        logger.info(f"Using time_window={time_window}: interval={effective_interval}")
    elif interval is None:
        # Default to 15m if neither time_window nor interval provided
        effective_interval = "15m"

    # T059: Check cache first (cache-first query strategy)
    # Use effective values for cache key
    cache_key_start = effective_start_time if not time_window else f"tw:{time_window}"
    cached_response = _heatmap_cache.get(
        symbol, cache_key_start, end_time, effective_interval, price_bin_size, leverage_weights
    )
    if cached_response is not None:
        logger.debug(f"Cache HIT for {symbol} heatmap-timeseries")
        return cached_response

    logger.debug(f"Cache MISS for {symbol} heatmap-timeseries - computing...")

    # Validate symbol against whitelist
    if symbol not in SUPPORTED_SYMBOLS:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid symbol '{symbol}'. Supported: {sorted(SUPPORTED_SYMBOLS)}",
        )

    # Parse time range
    try:
        if end_time:
            end_dt = datetime.fromisoformat(end_time.replace("Z", "+00:00")).replace(tzinfo=None)
        else:
            end_dt = datetime.now()

        if time_window:
            # Calculate start_time from time_window hours
            config = TIME_WINDOW_CONFIG[time_window]
            start_dt = end_dt - timedelta(hours=config["hours"])
        elif effective_start_time:
            start_dt = datetime.fromisoformat(effective_start_time.replace("Z", "+00:00")).replace(
                tzinfo=None
            )
        else:
            start_dt = end_dt - timedelta(days=7)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid time format: {e}")

    # Parse leverage weights
    try:
        weights = parse_leverage_weights(leverage_weights)
    except LeverageWeightsParseError as e:
        raise HTTPException(status_code=400, detail=str(e))

    # Mock candle class for algorithm
    @dataclass
    class Candle:
        open_time: datetime
        open: Decimal
        high: Decimal
        low: Decimal
        close: Decimal
        volume: Decimal

    db = DuckDBService(read_only=True)

    try:
        # Query candles - use appropriate base table and aggregate for larger intervals
        # Extended to support new time windows (30m, 2h, 8h, 12h, 1d)
        interval_agg_map = {
            "5m": 5,  # Direct from 5m table
            "15m": 15,  # Direct from 15m table
            "30m": 30,  # Aggregate from 5m (6 candles)
            "1h": 60,  # Aggregate from 15m (4 candles)
            "2h": 120,  # Aggregate from 15m (8 candles)
            "4h": 240,  # Aggregate from 15m (16 candles)
            "8h": 480,  # Aggregate from 15m (32 candles)
            "12h": 720,  # Aggregate from 15m (48 candles)
            "1d": 1440,  # Aggregate from 15m (96 candles)
        }
        agg_minutes = interval_agg_map.get(effective_interval, 15)

        if agg_minutes <= 15:
            # Direct query for 5m or 15m
            table_name = f"klines_{effective_interval}_history"
            candle_query = f"""
            SELECT
                open_time,
                CAST(open AS DECIMAL(18,8)) as open,
                CAST(high AS DECIMAL(18,8)) as high,
                CAST(low AS DECIMAL(18,8)) as low,
                CAST(close AS DECIMAL(18,8)) as close,
                CAST(volume AS DECIMAL(18,8)) as volume
            FROM {table_name}
            WHERE symbol = ? AND open_time >= ? AND open_time <= ?
            ORDER BY open_time
            """
            candles_df = db.conn.execute(candle_query, [symbol, start_dt, end_dt]).df()
        else:
            # Aggregate candles into larger intervals
            # 30m uses 5m base table, all others use 15m
            if effective_interval == "30m":
                base_table = "klines_5m_history"
            else:
                base_table = "klines_15m_history"

            candle_query = f"""
            WITH aggregated AS (
                SELECT
                    time_bucket(INTERVAL '{agg_minutes} minutes', open_time) as bucket,
                    FIRST(open ORDER BY open_time) as open,
                    MAX(high) as high,
                    MIN(low) as low,
                    LAST(close ORDER BY open_time) as close,
                    SUM(volume) as volume
                FROM {base_table}
                WHERE symbol = ? AND open_time >= ? AND open_time <= ?
                GROUP BY bucket
            )
            SELECT
                bucket as open_time,
                CAST(open AS DECIMAL(18,8)) as open,
                CAST(high AS DECIMAL(18,8)) as high,
                CAST(low AS DECIMAL(18,8)) as low,
                CAST(close AS DECIMAL(18,8)) as close,
                CAST(volume AS DECIMAL(18,8)) as volume
            FROM aggregated
            ORDER BY open_time
            """
            candles_df = db.conn.execute(candle_query, [symbol, start_dt, end_dt]).df()

        if candles_df.empty:
            return HeatmapTimeseriesResponse(
                data=[],
                meta=HeatmapTimeseriesMetadata(
                    symbol=symbol,
                    start_time=start_dt.isoformat(),
                    end_time=end_dt.isoformat(),
                    interval=effective_interval,
                    total_snapshots=0,
                    price_range={"min": 0, "max": 0},
                    total_long_volume=0.0,
                    total_short_volume=0.0,
                    total_consumed=0,
                ),
            )

        # Query OI data with delta calculation - OPTIMIZED: aggregate to interval
        # This reduces O(n*m) matching to O(n) dict lookup
        oi_query = f"""
        WITH oi_bucketed AS (
            SELECT
                time_bucket(INTERVAL '{agg_minutes} minutes', timestamp) as bucket,
                AVG(open_interest_value) as avg_oi
            FROM open_interest_history
            WHERE symbol = ? AND timestamp >= ? AND timestamp <= ?
            GROUP BY bucket
        )
        SELECT
            bucket,
            avg_oi,
            avg_oi - LAG(avg_oi) OVER (ORDER BY bucket) as oi_delta
        FROM oi_bucketed
        ORDER BY bucket
        """

        oi_df = db.conn.execute(oi_query, [symbol, start_dt, end_dt]).df()

        # Convert to candle objects
        candles = [
            Candle(
                open_time=row["open_time"].to_pydatetime()
                if hasattr(row["open_time"], "to_pydatetime")
                else row["open_time"],
                open=Decimal(str(row["open"])),
                high=Decimal(str(row["high"])),
                low=Decimal(str(row["low"])),
                close=Decimal(str(row["close"])),
                volume=Decimal(str(row["volume"])),
            )
            for _, row in candles_df.iterrows()
        ]

        # Match OI deltas to candles - OPTIMIZED: O(1) dict lookup
        # Build lookup dict from pre-aggregated OI buckets
        oi_lookup: dict[datetime, Decimal] = {}
        if not oi_df.empty:
            for _, row in oi_df.iterrows():
                bucket = row["bucket"]
                if hasattr(bucket, "to_pydatetime"):
                    bucket = bucket.to_pydatetime()
                delta_val = row["oi_delta"]
                oi_lookup[bucket] = (
                    Decimal(str(delta_val))
                    if delta_val and delta_val == delta_val
                    else Decimal("0")
                )

        oi_deltas = []
        for candle in candles:
            # Direct O(1) lookup - buckets should match exactly
            delta = oi_lookup.get(candle.open_time, Decimal("0"))
            oi_deltas.append(delta)

        # Calculate time-evolving heatmap
        snapshots = calculate_time_evolving_heatmap(
            candles=candles,
            oi_deltas=oi_deltas,
            symbol=symbol,
            leverage_weights=weights,
            price_bucket_size=Decimal(str(price_bin_size)),
        )

        # Convert to response format
        response_data = []
        total_consumed = 0
        total_long = 0.0
        total_short = 0.0
        all_prices = []

        for snapshot in snapshots:
            levels = []
            for cell in snapshot.cells.values():
                if cell.total_density > 0:
                    levels.append(
                        HeatmapLevel(
                            price=float(cell.price_bucket),
                            long_density=float(cell.long_density),
                            short_density=float(cell.short_density),
                        )
                    )
                    all_prices.append(float(cell.price_bucket))

            response_data.append(
                HeatmapSnapshotResponse(
                    timestamp=snapshot.timestamp.isoformat(),
                    levels=sorted(levels, key=lambda x: x.price),
                    positions_created=snapshot.positions_created,
                    positions_consumed=snapshot.positions_consumed,
                )
            )

            total_consumed += snapshot.positions_consumed
            total_long += float(snapshot.total_long_volume)
            total_short += float(snapshot.total_short_volume)

        price_range = {
            "min": min(all_prices) if all_prices else 0,
            "max": max(all_prices) if all_prices else 0,
        }

        response = HeatmapTimeseriesResponse(
            data=response_data,
            meta=HeatmapTimeseriesMetadata(
                symbol=symbol,
                start_time=start_dt.isoformat(),
                end_time=end_dt.isoformat(),
                interval=effective_interval,
                total_snapshots=len(snapshots),
                price_range=price_range,
                total_long_volume=total_long,
                total_short_volume=total_short,
                total_consumed=total_consumed,
            ),
        )

        # T059: Cache the response for future requests
        # Use same cache key as lookup (with time_window prefix if applicable)
        _heatmap_cache.set(
            symbol,
            cache_key_start,
            end_time,
            effective_interval,
            price_bin_size,
            leverage_weights,
            response,
        )
        logger.debug(f"Cached response for {symbol} heatmap-timeseries")

        return response

    except Exception as e:
        logger.error(f"Error calculating heatmap timeseries: {e}")
        raise HTTPException(status_code=500, detail=f"Calculation error: {e}")

    finally:
        db.close()
