"""DuckDB service for querying Open Interest and market data."""

import json
import logging
import time
from datetime import datetime, timedelta, timezone
try:
    from datetime import UTC
except ImportError:
    UTC = timezone.utc
from decimal import Decimal
from pathlib import Path
from typing import Tuple
from urllib.request import urlopen

import duckdb

from ..settings import get_settings
from .csv_loader import load_csv_glob, load_funding_rate_csv

logger = logging.getLogger(__name__)

# Lock file to prevent API from opening DB during ingestion
INGESTION_LOCK_FILE = Path("/tmp/duckdb-ingestion.lock")
# Auto-expire stale locks after this many seconds (30 minutes)
INGESTION_LOCK_MAX_AGE_SECONDS = 30 * 60


class IngestionLockError(Exception):
    """Raised when trying to connect while ingestion is in progress."""

    pass


def _fetch_binance_price(symbol: str, timeout: int = 5) -> Decimal:
    """Fetch current price from Binance API.

    Args:
        symbol: Trading pair (e.g., BTCUSDT)
        timeout: Request timeout in seconds

    Returns:
        Current market price as Decimal

    Raises:
        Exception: If API call fails
    """
    url = f"https://api.binance.com/api/v3/ticker/price?symbol={symbol}"
    with urlopen(url, timeout=timeout) as resp:
        data = json.loads(resp.read().decode())
        return Decimal(data["price"])


# Symbol-aware fallback prices (approximate, used when all other sources fail)
# These are order-of-magnitude estimates - actual price fetched from Binance when possible
_SYMBOL_FALLBACK_PRICES = {
    "BTCUSDT": Decimal("95000.00"),
    "ETHUSDT": Decimal("3500.00"),
    "BNBUSDT": Decimal("700.00"),
    "SOLUSDT": Decimal("200.00"),
    "XRPUSDT": Decimal("2.50"),
    "ADAUSDT": Decimal("1.00"),
    "DOGEUSDT": Decimal("0.40"),
    "DOTUSDT": Decimal("8.00"),
    "MATICUSDT": Decimal("0.50"),
    "LINKUSDT": Decimal("15.00"),
}


def _get_fallback_price(symbol: str) -> Decimal:
    """Get fallback price for a symbol when API and DB both fail.

    Args:
        symbol: Trading pair (e.g., BTCUSDT, ETHUSDT)

    Returns:
        Approximate fallback price (order-of-magnitude estimate)
    """
    return _SYMBOL_FALLBACK_PRICES.get(symbol, Decimal("1000.00"))


class DuckDBService:
    """Service for managing DuckDB connection and queries.

    Uses singleton pattern PER DATABASE PATH to avoid opening 185GB database file
    on every request. This dramatically reduces HDD I/O and prevents system slowdowns.

    Different db_paths get different singleton instances, allowing:
    - Production API to keep one connection to production DB
    - Tests to use separate DB files (or :memory:)

    Thread-safe: Uses lock for concurrent singleton creation.
    Health-checked: Validates connection on access, auto-reconnects if stale.
    """

    # Singletons keyed by (resolved_path, read_only)
    _instances: dict[tuple[str, bool], "DuckDBService"] = {}
    # Thread lock for safe concurrent singleton creation
    _lock = __import__("threading").Lock()

    @classmethod
    def reset_singletons(cls, db_path: str | None = None) -> None:
        """Reset singleton instances. Used for testing.

        Args:
            db_path: If provided, only reset singletons for this path.
                     If None, reset ALL singletons.
        """
        if db_path is None:
            # Reset all
            for instance in cls._instances.values():
                try:
                    instance.conn.close()
                except Exception:
                    pass
            cls._instances.clear()
        else:
            # Reset specific path
            resolved = str(Path(db_path).resolve())
            keys_to_remove = [k for k in cls._instances if k[0] == resolved]
            for key in keys_to_remove:
                try:
                    cls._instances[key].conn.close()
                except Exception:
                    pass
                del cls._instances[key]

    @classmethod
    def close_all_instances(cls) -> int:
        """Close all singleton instances. Used before external ingestion.

        This method safely closes all DuckDB connections held by the singleton
        pattern, allowing external processes (like N8N ingestion) to acquire
        write locks on the database.

        Returns:
            Number of instances closed
        """
        closed_count = 0
        with cls._lock:
            keys_to_remove = list(cls._instances.keys())
            for key in keys_to_remove:
                try:
                    instance = cls._instances[key]
                    if hasattr(instance, "conn") and instance.conn is not None:
                        instance.conn.close()
                        instance._initialized = False
                        closed_count += 1
                        logger.info(f"Closed DuckDB instance: {key}")
                except Exception as e:
                    logger.warning(f"Error closing instance {key}: {e}")
                finally:
                    del cls._instances[key]

        logger.info(f"Closed {closed_count} DuckDB instances total")
        return closed_count

    @classmethod
    def is_ingestion_locked(cls) -> bool:
        """Check if ingestion lock is active.

        Auto-expires stale locks older than INGESTION_LOCK_MAX_AGE_SECONDS
        to prevent permanent lockout from failed ingestions.

        Returns:
            True if lock file exists and is not stale
        """
        if not INGESTION_LOCK_FILE.exists():
            return False
        try:
            age = time.time() - INGESTION_LOCK_FILE.stat().st_mtime
            if age > INGESTION_LOCK_MAX_AGE_SECONDS:
                logger.warning(
                    f"Stale ingestion lock detected ({age / 60:.0f}m old, "
                    f"max {INGESTION_LOCK_MAX_AGE_SECONDS / 60:.0f}m). "
                    f"Auto-releasing."
                )
                INGESTION_LOCK_FILE.unlink(missing_ok=True)
                return False
        except OSError:
            return False
        return True

    @classmethod
    def set_ingestion_lock(cls) -> bool:
        """Create ingestion lock file.

        Returns:
            True if lock was created successfully
        """
        try:
            INGESTION_LOCK_FILE.touch()
            logger.info("Ingestion lock acquired")
            return True
        except Exception as e:
            logger.error(f"Failed to create ingestion lock: {e}")
            return False

    @classmethod
    def release_ingestion_lock(cls) -> bool:
        """Remove ingestion lock file.

        Returns:
            True if lock was released successfully
        """
        try:
            if INGESTION_LOCK_FILE.exists():
                INGESTION_LOCK_FILE.unlink()
            logger.info("Ingestion lock released")
            return True
        except Exception as e:
            logger.error(f"Failed to release ingestion lock: {e}")
            return False

    def __new__(cls, db_path: str | None = None, read_only: bool = False):
        """Singleton pattern per (db_path, read_only) - reuse existing connection.

        Thread-safe: Uses lock for concurrent singleton creation.
        Health-checked: Validates existing connections, recreates if stale.

        Args:
            db_path: Path to DuckDB database file
            read_only: If True, open in read-only mode (faster for queries)
        """
        if db_path is None:
            db_path = str(get_settings().db_path)
        resolved_path = str(Path(db_path).resolve())
        key = (resolved_path, read_only)

        with cls._lock:
            # Check ingestion lock BEFORE allowing new connections
            if cls.is_ingestion_locked():
                raise IngestionLockError(
                    "Database locked for ingestion. Try again after ingestion completes."
                )

            # Check if we have an existing instance
            if key in cls._instances:
                instance = cls._instances[key]
                # Health check: verify connection is still valid
                if instance._initialized and not instance._is_connection_healthy():
                    logger.warning(f"Stale connection detected for {db_path}, reconnecting...")
                    try:
                        instance.conn.close()
                    except Exception:
                        pass
                    instance._initialized = False
                    del cls._instances[key]
                else:
                    return instance

            # Create new instance
            instance = super().__new__(cls)
            instance._initialized = False
            cls._instances[key] = instance

        return cls._instances[key]

    def _is_connection_healthy(self) -> bool:
        """Check if the DuckDB connection is still valid.

        Returns:
            True if connection responds to queries, False otherwise.
        """
        try:
            self.conn.execute("SELECT 1").fetchone()
            return True
        except Exception:
            return False

    def __init__(
        self,
        db_path: str = None,
        read_only: bool = False,
    ):
        """Initialize DuckDB service (only once per singleton).

        Args:
            db_path: Path to DuckDB database file
            read_only: If True, open in read-only mode (faster for queries)
        """
        # Skip if already initialized (singleton)
        if getattr(self, "_initialized", False):
            return

        if db_path is None:
            db_path = get_settings().db_path

        self.db_path = Path(db_path)
        self.read_only = read_only

        if not read_only:
            self.db_path.parent.mkdir(parents=True, exist_ok=True)

        # Open connection with appropriate mode
        if read_only:
            self.conn = duckdb.connect(str(self.db_path), read_only=True)
            logger.info(f"DuckDB singleton (read-only) connected: {self.db_path}")
        else:
            self.conn = duckdb.connect(str(self.db_path))
            logger.info(f"DuckDB singleton (read-write) connected: {self.db_path}")

        self._initialized = True

    def get_latest_open_interest(self, symbol: str = "BTCUSDT") -> Tuple[Decimal, Decimal]:
        """Get latest Open Interest and current price for symbol.

        Args:
            symbol: Trading pair (default: BTCUSDT)

        Returns:
            Tuple of (current_price, open_interest_value)
        """
        # Try to query from database
        try:
            result = self.conn.execute(
                """
                SELECT
                    open_interest_value,
                    timestamp
                FROM open_interest_history
                WHERE symbol = ?
                ORDER BY timestamp DESC
                LIMIT 1
                """,
                [symbol],
            ).fetchone()

            if result:
                oi_value = Decimal(str(result[0]))
                # Fetch real-time price from Binance API
                try:
                    current_price = _fetch_binance_price(symbol)
                except Exception as e:
                    logger.warning(f"Binance API price fetch failed for {symbol}: {e}")
                    # Fallback: symbol-aware estimate when API fails
                    current_price = _get_fallback_price(symbol)
                return current_price, oi_value
        except duckdb.CatalogException as e:
            # Table doesn't exist, load from CSV
            logger.debug(f"open_interest_history table not found, loading from CSV: {e}")

        # If no data in DB, load from CSV and insert
        # Guard: read-only connections cannot write, return fallback
        if self.read_only:
            logger.warning(f"No OI data for {symbol} and connection is read-only, using fallback")
            try:
                current_price = _fetch_binance_price(symbol)
            except Exception:
                current_price = _get_fallback_price(symbol)
            return current_price, Decimal("0")

        return self._load_and_cache_data(symbol)

    def _load_and_cache_data(self, symbol: str) -> Tuple[Decimal, Decimal]:
        """Load data from CSV and cache in DuckDB.

        Args:
            symbol: Trading pair

        Returns:
            Tuple of (current_price, open_interest_value)
        """
        # Load from CSV
        csv_pattern = f"data/raw/{symbol}/metrics/{symbol}-metrics-*.csv"

        try:
            df = load_csv_glob(csv_pattern, conn=self.conn)
        except FileNotFoundError:
            # No data available, fetch real price and return defaults
            logger.warning(f"No CSV data found for {symbol}, using defaults")
            try:
                current_price = _fetch_binance_price(symbol)
            except Exception as e:
                logger.warning(f"Binance price fetch also failed for {symbol}: {e}")
                current_price = _get_fallback_price(symbol)
            return current_price, Decimal("100000000.00")

        if df.empty:
            logger.warning(f"Empty CSV data for {symbol}, using defaults")
            try:
                current_price = _fetch_binance_price(symbol)
            except Exception as e:
                logger.warning(f"Binance price fetch failed for empty CSV {symbol}: {e}")
                current_price = _get_fallback_price(symbol)
            return current_price, Decimal("100000000.00")

        # Create table if not exists with UNIQUE constraint
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS open_interest_history (
                id BIGINT PRIMARY KEY,
                timestamp TIMESTAMP NOT NULL,
                symbol VARCHAR(20) NOT NULL,
                open_interest_value DECIMAL(20, 8) NOT NULL,
                open_interest_contracts DECIMAL(20, 8),
                UNIQUE(timestamp, symbol)
            )
        """)

        # Insert data with validation (INSERT OR IGNORE for duplicates)
        # Validate: OI value > 0, symbol not empty
        self.conn.execute("""
            INSERT OR IGNORE INTO open_interest_history
            SELECT
                row_number() OVER (ORDER BY timestamp) +
                    COALESCE((SELECT MAX(id) FROM open_interest_history), 0) as id,
                timestamp,
                symbol,
                open_interest_value,
                open_interest_contracts
            FROM df
            WHERE open_interest_value > 0
              AND symbol IS NOT NULL
              AND symbol != ''
        """)

        # Get latest OI value
        latest = df.iloc[-1]
        oi_value = Decimal(str(latest["open_interest_value"]))

        # Fetch real-time price from Binance API
        try:
            current_price = _fetch_binance_price(symbol)
        except Exception as e:
            logger.warning(f"Binance API price fetch failed for {symbol}: {e}")
            current_price = _get_fallback_price(symbol)

        return current_price, oi_value

    def get_latest_funding_rate(self, symbol: str = "BTCUSDT") -> Decimal:
        """Get latest funding rate for symbol.

        Args:
            symbol: Trading pair

        Returns:
            Current funding rate (e.g., 0.0001 for 0.01%)
        """
        # Try database first
        try:
            result = self.conn.execute(
                """
                SELECT funding_rate
                FROM funding_rate_history
                WHERE symbol = ?
                ORDER BY timestamp DESC
                LIMIT 1
                """,
                [symbol],
            ).fetchone()

            if result:
                return Decimal(str(result[0]))
        except duckdb.CatalogException as e:
            # Table doesn't exist, will load from CSV
            logger.debug(f"funding_rate_history table not found, loading from CSV: {e}")

        # Guard: read-only connections cannot write, return fallback
        if self.read_only:
            logger.warning(
                f"No funding rate for {symbol} and connection is read-only, using fallback"
            )
            return Decimal("0.0001")  # Default funding rate

        # Load from CSV
        csv_pattern = f"data/raw/{symbol}/fundingRate/{symbol}-fundingRate-*.csv"

        try:
            df = load_csv_glob(csv_pattern, loader_func=load_funding_rate_csv, conn=self.conn)
        except FileNotFoundError:
            return Decimal("0.0001")  # Default funding rate

        if df.empty:
            return Decimal("0.0001")

        # Create table if not exists with UNIQUE constraint
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS funding_rate_history (
                id BIGINT PRIMARY KEY,
                timestamp TIMESTAMP NOT NULL,
                symbol VARCHAR(20) NOT NULL,
                funding_rate DECIMAL(10, 8) NOT NULL,
                mark_price DECIMAL(18, 2),
                UNIQUE(timestamp, symbol)
            )
        """)

        # Insert with validation (INSERT OR IGNORE for duplicates)
        # Validate: funding rate within reasonable range (-1% to +1%), symbol not empty
        self.conn.execute("""
            INSERT OR IGNORE INTO funding_rate_history
            SELECT
                row_number() OVER (ORDER BY timestamp) +
                    COALESCE((SELECT MAX(id) FROM funding_rate_history), 0) as id,
                timestamp,
                symbol,
                funding_rate,
                mark_price
            FROM df
            WHERE funding_rate BETWEEN -0.01 AND 0.01
              AND symbol IS NOT NULL
              AND symbol != ''
        """)

        latest = df.iloc[-1]
        return Decimal(str(latest["funding_rate"]))

    def close(self, force: bool = False):
        """Close database connection.

        Args:
            force: If True, close even singleton connection. Default False preserves singleton.
        """
        # Don't close singleton connections unless forced
        if not force:
            return

        if self.conn:
            self.conn.close()
            self._initialized = False
            # Clear singleton reference from dict
            resolved_path = str(self.db_path.resolve())
            key = (resolved_path, self.read_only)
            if key in DuckDBService._instances:
                del DuckDBService._instances[key]

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - does NOT close singleton connection."""
        # Singleton stays open - this is intentional for performance
        pass

    def get_large_trades(
        self,
        symbol: str = "BTCUSDT",
        min_gross_value: Decimal = Decimal("100000"),
        start_datetime: str = None,
        end_datetime: str = None,
    ):
        """Get large trades from aggTrades data (filtered by timeframe only, no limit)."""
        import pandas as pd

        logger = logging.getLogger(__name__)

        # Defense-in-depth: validate symbol is alphanumeric (prevents SQL injection)
        if not symbol.isalnum():
            raise ValueError(f"Invalid symbol: {symbol}. Must be alphanumeric.")

        logger.info(
            f"get_large_trades called: symbol={symbol}, min_gross_value={min_gross_value}, "
            f"timeframe={start_datetime} to {end_datetime}"
        )

        # Try to query from DB first
        try:
            # Build query with temporal filters - use ALL trades (no sampling)
            # Default exchange to 'binance' for backward compatibility
            query_parts = [
                "SELECT timestamp, price, quantity, side, gross_value",
                "FROM aggtrades_history",
                "WHERE symbol = ? AND exchange = 'binance' AND gross_value >= ?",
            ]
            params = [symbol, float(min_gross_value)]

            if start_datetime:
                query_parts.append("AND timestamp >= ?")
                params.append(start_datetime)

            if end_datetime:
                query_parts.append("AND timestamp <= ?")
                params.append(end_datetime)

            query_parts.append("ORDER BY timestamp DESC")

            query = " ".join(query_parts)
            df = self.conn.execute(query, params).df()
            if not df.empty:
                logger.info(
                    f"Loaded {len(df)} trades from timeframe {start_datetime} to {end_datetime}"
                )
                return df
            logger.info("DB cache empty, loading from CSV")
        except Exception as e:
            logger.warning(f"DB query failed: {e}, loading from CSV")

        # Load from CSV if not in DB
        csv_path = f"/media/sam/3TB-WDC/binance-history-data-downloader/data/{symbol}/aggTrades/{symbol}-aggTrades-2025-10-*.csv"
        logger.info(f"CSV pattern: {csv_path}")

        # Create table with correct COMPOSITE PRIMARY KEY (agg_trade_id, symbol, exchange)
        try:
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS aggtrades_history (
                    agg_trade_id BIGINT NOT NULL,
                    timestamp TIMESTAMP NOT NULL,
                    symbol VARCHAR(20) NOT NULL,
                    exchange VARCHAR(20) NOT NULL DEFAULT 'binance',
                    price DECIMAL(18, 8) NOT NULL,
                    quantity DECIMAL(18, 8) NOT NULL,
                    side VARCHAR(4) NOT NULL,
                    gross_value DOUBLE NOT NULL,
                    PRIMARY KEY (agg_trade_id, symbol, exchange)
                )
            """)
        except Exception as e:
            logger.debug(f"Table creation skipped (may exist): {e}")

        # Load data using DOUBLE to avoid DECIMAL overflow
        # Updated INSERT with composite PK columns (agg_trade_id, symbol, exchange)
        try:
            logger.info(f"Loading CSV files matching: {csv_path}")
            self.conn.execute(f"""
                INSERT OR IGNORE INTO aggtrades_history
                (agg_trade_id, timestamp, symbol, exchange, price, quantity, side, gross_value)
                SELECT
                    agg_trade_id,
                    to_timestamp(transact_time / 1000) as timestamp,
                    '{symbol}' as symbol,
                    'binance' as exchange,
                    CAST(price AS DECIMAL(18, 8)) as price,
                    CAST(quantity AS DECIMAL(18, 8)) as quantity,
                    CASE WHEN is_buyer_maker THEN 'sell' ELSE 'buy' END as side,
                    price * quantity as gross_value
                FROM read_csv_auto('{csv_path}')
                WHERE (price * quantity) >= {float(min_gross_value)}
                LIMIT 10000
            """)

            # Return loaded data
            df = self.conn.execute(
                """
                SELECT timestamp, price, quantity, side, gross_value
                FROM aggtrades_history
                WHERE symbol = ? AND exchange = 'binance' AND gross_value >= ?
                ORDER BY timestamp DESC
                LIMIT 10000
            """,
                [symbol, float(min_gross_value)],
            ).df()

            logger.info(
                f"✅ Loaded {len(df)} large trades from CSV (buy: {len(df[df['side'] == 'buy'])}, sell: {len(df[df['side'] == 'sell'])})"
            )
            return df

        except Exception as e:
            logger.error(f"❌ Failed to load trades from CSV: {e}", exc_info=True)
            # Return empty DataFrame on error
            return pd.DataFrame(columns=["timestamp", "price", "quantity", "side", "gross_value"])

    def calculate_liquidations_sql(
        self,
        symbol: str = "BTCUSDT",
        current_price: float = None,
        bin_size: float = 200.0,
        min_gross_value: float = 500000.0,
        start_datetime: str = None,
        end_datetime: str = None,
    ):
        """Calculate liquidation levels DIRECTLY in DuckDB (100x faster than Python loops).

        Args:
            symbol: Trading pair
            current_price: Current market price
            bin_size: Price bucket size for aggregation
            min_gross_value: Minimum trade size (whale trades)
            start_datetime: Start of timeframe
            end_datetime: End of timeframe

        Returns:
            DataFrame with columns: price_bucket, leverage, side, volume, count
        """
        import logging

        logger = logging.getLogger(__name__)

        logger.info(
            f"calculate_liquidations_sql: symbol={symbol}, current_price={current_price}, "
            f"bin_size={bin_size}, timeframe={start_datetime} to {end_datetime}"
        )

        # MMR for simplicity (0.4% - conservative)
        mmr = 0.004

        query = f"""
        WITH leverage_tiers AS (
            SELECT unnest([5, 10, 25, 50, 100]) as leverage
        ),
        -- Step 1: Calculate liquidation prices for EACH leverage tier
        trades_with_liq_prices AS (
            SELECT
                t.timestamp,
                t.gross_value,
                t.side,
                t.price as entry_price,
                l.leverage,
                CASE
                    WHEN t.side = 'buy' THEN t.price * (1 - 1.0/l.leverage + {mmr})
                    WHEN t.side = 'sell' THEN t.price * (1 + 1.0/l.leverage - {mmr})
                END as liq_price
            FROM aggtrades_history t
            CROSS JOIN leverage_tiers l
            WHERE t.symbol = ?
              AND t.exchange = 'binance'
              AND t.gross_value >= ?
        """

        params = [symbol, min_gross_value]

        if start_datetime:
            query += " AND t.timestamp >= ?"
            params.append(start_datetime)

        if end_datetime:
            query += " AND t.timestamp <= ?"
            params.append(end_datetime)

        query += f"""
        ),
        -- Step 2: Filter for valid liquidations and count active leverage tiers per trade
        valid_liqs AS (
            SELECT
                *,
                -- Count how many leverage tiers result in valid liquidations for this trade
                COUNT(*) OVER (PARTITION BY timestamp, entry_price, gross_value, side) as active_leverage_count
            FROM trades_with_liq_prices
            WHERE (side = 'buy' AND liq_price < {current_price})
               OR (side = 'sell' AND liq_price > {current_price})
        ),
        -- Step 3: Bucket the valid liquidations
        bucketed_liqs AS (
            SELECT
                FLOOR(liq_price / {bin_size}) * {bin_size} as price_bucket,
                leverage,
                side,
                gross_value,
                active_leverage_count
            FROM valid_liqs
        )
        -- Step 4: Aggregate - distribute volume among active leverage tiers
        SELECT
            price_bucket,
            leverage,
            side,
            -- Divide by ACTUAL number of active leverage tiers (not hardcoded 5)
            SUM(gross_value / active_leverage_count) as total_volume,
            COUNT(*) as count
        FROM bucketed_liqs
        GROUP BY price_bucket, leverage, side
        ORDER BY price_bucket, leverage
        """

        df = self.conn.execute(query, params).df()
        logger.info(f"SQL aggregation complete: {len(df)} bins returned")
        return df

    # Default leverage distribution (approximate, based on Coinglass tier visibility)
    # Source: Observational analysis of Coinglass/Coinank liquidation maps
    # showing ~5 tiers with inverse relationship between leverage and usage.
    # These should be refined with actual Binance leverageBracket API data.
    DEFAULT_LEVERAGE_WEIGHTS: dict[int, float] = {
        5: 0.15,  # 15% - safer traders
        10: 0.30,  # 30% - conservative (most popular)
        25: 0.25,  # 25% - moderate
        50: 0.20,  # 20% - aggressive
        100: 0.10,  # 10% - high risk
    }

    @staticmethod
    def _kline_table_for_interval(interval: str) -> str:
        return f"klines_{interval}_history"

    def _table_exists(self, table_name: str) -> bool:
        row = self.conn.execute(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE lower(table_schema) = 'main'
              AND lower(table_name) = lower(?)
            LIMIT 1
            """,
            [table_name],
        ).fetchone()
        return bool(row)

    def _has_kline_coverage(
        self,
        table_name: str,
        symbol: str,
        lookback_days: int,
        expected_per_day: int,
        min_ratio: float,
        freshness_minutes: int = 20,
    ) -> bool:
        if not self._table_exists(table_name):
            return False

        max_ts_row = self.conn.execute(
            f"SELECT MAX(open_time) FROM {table_name} WHERE symbol = ?",
            [symbol],
        ).fetchone()
        if not max_ts_row or max_ts_row[0] is None:
            return False

        max_ts = max_ts_row[0]
        now_utc = datetime.now(UTC).replace(tzinfo=None)
        if max_ts < now_utc - timedelta(minutes=freshness_minutes):
            return False

        start_ts = max_ts - timedelta(days=lookback_days)
        count_row = self.conn.execute(
            f"""
            SELECT COUNT(*)
            FROM {table_name}
            WHERE symbol = ?
              AND open_time >= ?
            """,
            [symbol, start_ts],
        ).fetchone()
        count = count_row[0] if count_row else 0

        expected = max(1, lookback_days * expected_per_day)
        coverage_ratio = count / expected
        return coverage_ratio >= min_ratio

    def _resolve_oi_kline_source(
        self,
        symbol: str,
        lookback_days: int,
        kline_interval: str = "auto",
    ) -> tuple[str, str]:
        """Select klines table for OI-based model with robust fallback.

        Returns:
            (table_name, interval_used)
        """
        requested = (kline_interval or "auto").lower()
        if requested not in {"auto", "1m", "5m"}:
            raise ValueError(f"Unsupported kline_interval={kline_interval!r}")

        table_1m = self._kline_table_for_interval("1m")
        table_5m = self._kline_table_for_interval("5m")

        has_1m = self._has_kline_coverage(
            table_1m,
            symbol,
            lookback_days=lookback_days,
            expected_per_day=1440,
            min_ratio=0.60,
        )
        has_5m = self._has_kline_coverage(
            table_5m,
            symbol,
            lookback_days=lookback_days,
            expected_per_day=288,
            min_ratio=0.50,
        )

        if requested == "1m":
            if has_1m:
                return table_1m, "1m"
            if has_5m:
                logger.warning(
                    "Requested 1m klines for %s but coverage is insufficient; falling back to 5m",
                    symbol,
                )
                return table_5m, "5m"
            raise ValueError(f"No usable kline data for symbol={symbol}")

        if requested == "5m":
            if has_5m:
                return table_5m, "5m"
            if has_1m:
                logger.warning(
                    "Requested 5m klines for %s but coverage is insufficient; falling back to 1m",
                    symbol,
                )
                return table_1m, "1m"
            raise ValueError(f"No usable kline data for symbol={symbol}")

        # auto policy: prefer 1m only for short windows where higher granularity matters.
        if lookback_days <= 7 and has_1m:
            return table_1m, "1m"
        if has_5m:
            return table_5m, "5m"
        if has_1m:
            return table_1m, "1m"
        raise ValueError(f"No usable kline data for symbol={symbol}")

    def calculate_liquidations_oi_based(
        self,
        symbol: str = "BTCUSDT",
        current_price: float = None,
        bin_size: float = 500.0,
        lookback_days: int = 30,
        whale_threshold: float = 500000.0,
        leverage_weights: dict[int, float] | None = None,
        kline_interval: str = "auto",
    ):
        """Calculate liquidations using Open Interest-based volume profile scaling.

        This model uses volume profile from aggTrades for DISTRIBUTION SHAPE,
        then scales the total to match current Open Interest (positions still open).

        Methodology (Gemini AI - Option B: OpenInterest + Volume Profile Scaling):
        1. Build 30-day volume profile from aggTrades (relative distribution by price)
        2. Calculate total 30-day volume
        3. Get current OI from Binance API (~$8.5B)
        4. Calculate scaling factor: current_oi / total_30day_volume ≈ 0.017
        5. Apply scaling: scaled_volume = volume_at_price * scaling_factor
        6. Distribute across leverage tiers (5x: 15%, 10x: 30%, 25x: 25%, 50x: 20%, 100x: 10%)
        7. Split 50/50 between longs and shorts
        8. Calculate liquidation prices for each bin+leverage combination

        This produces numbers in ~3-4B range (like Coinglass) instead of inflated 200B (aggTrades only).

        Args:
            symbol: Trading pair (default: BTCUSDT)
            current_price: Current market price
            bin_size: Price bucket size for aggregation
            lookback_days: Days to look back for volume profile (default: 30)
            whale_threshold: Whale trade threshold (currently non-functional, see warning)
            leverage_weights: Override leverage distribution {leverage: weight}.
                Defaults to DEFAULT_LEVERAGE_WEIGHTS. Weights must sum to 1.0.
            kline_interval: Candle source for OI model ("auto", "1m", or "5m").
                "auto" prefers 1m for short windows (<=7d) when coverage is healthy,
                otherwise falls back to 5m for better stability and OI alignment.

        Returns:
            DataFrame with columns: price_bucket, leverage, side, volume, liq_price
        """
        import logging

        logger = logging.getLogger(__name__)

        logger.info(
            f"calculate_liquidations_oi_based: symbol={symbol}, lookback={lookback_days}d, bin_size={bin_size}"
        )

        # IMPORTANT: Warn if non-default whale_threshold is used (parameter currently non-functional)
        if whale_threshold != 500000.0:
            logger.warning(
                f"whale_threshold={whale_threshold} specified but volume_profile_daily cache "
                f"uses $500k hardcoded. Parameter currently has no effect. "
                f"See /tmp/CRITICAL_WHALE_THRESHOLD_BUG_18NOV2025.md for details."
            )

        # Build leverage distribution from parameter or defaults
        if leverage_weights is None:
            leverage_weights = self.DEFAULT_LEVERAGE_WEIGHTS
        if not leverage_weights:
            raise ValueError("leverage_weights cannot be empty")
        if any(w <= 0 for w in leverage_weights.values()):
            raise ValueError("All leverage_weights must be positive")
        weight_sum = sum(leverage_weights.values())
        if abs(weight_sum - 1.0) > 0.01:
            raise ValueError(f"leverage_weights must sum to 1.0 (got {weight_sum:.4f})")
        leverage_values = ", ".join(
            f"({lev}, {weight})" for lev, weight in leverage_weights.items()
        )

        kline_table, kline_interval_used = self._resolve_oi_kline_source(
            symbol=symbol,
            lookback_days=lookback_days,
            kline_interval=kline_interval,
        )
        logger.info(
            "OI model kline source: requested=%s selected=%s table=%s",
            kline_interval,
            kline_interval_used,
            kline_table,
        )

        # MMR (Maintenance Margin Rate) - dynamically computed per-bucket
        # from official Binance BTC/USDT tiers via MMRTiers CTE in SQL.
        # See src/liquidationheatmap/models/binance_standard.py lines 22-33.

        query = f"""
        WITH Params AS (
            -- Get latest Open Interest and calculate lookback period
            -- Use MAX timestamp from data (not CURRENT_TIMESTAMP) to handle historical data
            SELECT
                (SELECT open_interest_value FROM open_interest_history
                 WHERE symbol = ? ORDER BY timestamp DESC LIMIT 1) AS latest_oi,
                (SELECT MAX(open_time) FROM {kline_table} WHERE symbol = ?)
                    - INTERVAL '{lookback_days} days' AS start_time,
                {bin_size} AS price_bin_size
        ),

        -- Leverage distribution weights (matching Coinglass 5 tiers)
        -- Source: Observational analysis of Coinglass/Coinank liquidation maps.
        -- Parameterized via leverage_weights argument (default: DEFAULT_LEVERAGE_WEIGHTS).
        LeverageDistribution AS (
            SELECT * FROM (VALUES
                {leverage_values}
            ) AS t (leverage, weight)
        ),

        -- STEP 1: Use configured klines table (1m/5m) with OI-bucket alignment.
        -- OI source remains 5m, so we align each candle to a 5m bucket.
        CandleOHLC AS (
            SELECT
                open_time as candle_time,
                time_bucket(INTERVAL '5 minutes', open_time) AS oi_bucket_time,
                FLOOR(close / {bin_size}) * {bin_size} AS price_bin,
                open,
                high,
                low,
                close,
                COALESCE(quote_volume, close * volume) AS volume
            FROM {kline_table}
            WHERE symbol = ?
              AND open_time >= (SELECT start_time FROM Params)
        ),

        -- STEP 2: Compute OI Delta inline via LAG() window function
        OIDelta AS (
            SELECT
                timestamp as oi_bucket_time,
                open_interest_value - LAG(open_interest_value)
                    OVER (ORDER BY timestamp) AS oi_delta
            FROM open_interest_history
            WHERE symbol = ?
              AND timestamp >= (SELECT start_time FROM Params) - INTERVAL '1 day'
        ),

        -- STEP 3: Infer position SIDE from candle direction + OI delta
        -- Industry-standard logic:
        -- - Bullish candle (close > open) + OI increase → LONG positions opened
        -- - Bearish candle (close < open) + OI increase → SHORT positions opened
        CandleWithSide AS (
            SELECT
                c.candle_time,
                c.price_bin,
                c.open,
                c.high,
                c.low,
                c.close,
                c.volume,
                o.oi_delta,
                CASE
                    WHEN c.close > c.open AND o.oi_delta > 0 THEN 'buy'   -- Bullish + OI up = LONG
                    WHEN c.close < c.open AND o.oi_delta > 0 THEN 'sell'  -- Bearish + OI up = SHORT
                    ELSE NULL  -- Ignore neutral candles or OI decrease
                END as inferred_side
            FROM CandleOHLC c
            LEFT JOIN OIDelta o ON c.oi_bucket_time = o.oi_bucket_time
            WHERE
                -- Only keep candles with clear signal (non-null side)
                CASE
                    WHEN c.close > c.open AND o.oi_delta > 0 THEN 'buy'
                    WHEN c.close < c.open AND o.oi_delta > 0 THEN 'sell'
                    ELSE NULL
                END IS NOT NULL
        ),

        -- STEP 4: Distribute OI across price bins and sides
        -- Scale volume shape to match latest OI magnitude
        TotalVolume AS (
            SELECT COALESCE(SUM(volume), 1.0) as total_volume
            FROM CandleWithSide
        ),

        OIDistribution AS (
            SELECT
                price_bin,
                inferred_side as side,
                SUM(volume) as volume_at_price,
                -- Scale volume shape with latest OI
                SUM(volume) * (SELECT latest_oi FROM Params) /
                (SELECT total_volume FROM TotalVolume) as oi_at_price
            FROM CandleWithSide
            GROUP BY price_bin, inferred_side
        ),

        -- Binance BTC/USDT MMR tiers (official, position-size based)
        -- Source: https://www.binance.com/en/support/faq/liquidation
        -- Mirrors BinanceStandardModel.MMR_TIERS in models/binance_standard.py
        MMRTiers AS (
            SELECT * FROM (VALUES
                (50000.0,     0.004),
                (250000.0,    0.005),
                (1000000.0,   0.01),
                (10000000.0,  0.025),
                (20000000.0,  0.05),
                (50000000.0,  0.10),
                (100000000.0, 0.125),
                (200000000.0, 0.15),
                (300000000.0, 0.25),
                (500000000.0, 0.50)
            ) AS t (max_notional, mmr_rate)
        ),

        -- STEP 5a: Compute per-bucket notional and matching MMR tier
        BucketMMR AS (
            SELECT
                od.price_bin AS price_bucket,
                ld.leverage,
                od.side,
                od.oi_at_price * ld.weight AS volume,
                COALESCE(
                    (SELECT mmr_rate FROM MMRTiers
                     WHERE max_notional >= od.oi_at_price * ld.weight
                     ORDER BY max_notional LIMIT 1),
                    0.50
                ) AS mmr
            FROM OIDistribution od
            CROSS JOIN LeverageDistribution ld
        ),

        -- STEP 5: Calculate liquidation prices
        -- Use base MMR (0.4%) as maps are aggregated and individual position sizes are unknown
        AllLiquidations AS (
            SELECT
                price_bucket,
                leverage,
                side,
                volume,
                CASE
                    WHEN side = 'buy' THEN  -- LONG positions
                        price_bucket * (1 - 1.0/leverage + 0.004)
                    WHEN side = 'sell' THEN  -- SHORT positions
                        price_bucket * (1 + 1.0/leverage - 0.004)
                END AS liq_price
            FROM BucketMMR
        )

        -- STEP 6: Filter ONLY liquidations "at risk" based on liquidation price vs current
        SELECT
            price_bucket,
            leverage,
            side,
            volume,
            liq_price
        FROM AllLiquidations
        WHERE
            -- Shorts: liq_price ABOVE current (price must go UP to liquidate)
            (side = 'sell' AND liq_price > {current_price})
            OR
            -- Longs: liq_price BELOW current (price must go DOWN to liquidate)
            (side = 'buy' AND liq_price < {current_price})
        ORDER BY liq_price, leverage
        """

        # Updated params: Params CTE (latest_oi, max_time), CandleOHLC CTE, OIDelta CTE
        params = [symbol, symbol, symbol, symbol]

        try:
            df = self.conn.execute(query, params).df()
            logger.info(f"OI-based model complete: {len(df)} liquidation levels returned")

            # Sanity check: sum of all volumes should approximately equal latest OI
            # NOTE: Validation logging disabled for performance (skips 1.9B row aggtrades scan)
            # if not df.empty:
            #     total_distributed = df["volume"].sum()
            #
            #     # Get OI and total volume for validation
            #     oi_result = self.conn.execute(
            #         "SELECT open_interest_value FROM open_interest_history WHERE symbol = ? ORDER BY timestamp DESC LIMIT 1",
            #         [symbol],
            #     ).fetchone()
            #
            #     total_volume_result = self.conn.execute(
            #         f"""
            #         SELECT SUM(gross_value) as total_volume
            #         FROM aggtrades_history
            #         WHERE symbol = ?
            #           AND timestamp >= CURRENT_TIMESTAMP - INTERVAL '{lookback_days} days'
            #         """,
            #         [symbol],
            #     ).fetchone()
            #
            #     if oi_result and total_volume_result:
            #         latest_oi = float(oi_result[0])
            #         total_volume = float(total_volume_result[0])
            #         scaling_factor = latest_oi / total_volume if total_volume > 0 else 0
            #
            #         logger.info(
            #             f"📊 Volume Profile Scaling:\n"
            #             f"  - Latest OI: ${latest_oi:,.0f}\n"
            #             f"  - {lookback_days}-day volume: ${total_volume:,.0f}\n"
            #             f"  - Scaling factor: {scaling_factor:.4f}\n"
            #             f"  - Total distributed: ${total_distributed:,.0f}\n"
            #             f"  - Coverage: {total_distributed / latest_oi:.2%}"
            #         )

            return df
        except Exception as e:
            logger.error(f"OI-based calculation failed: {e}", exc_info=True)
            # Return empty DataFrame on error
            import pandas as pd

            return pd.DataFrame(columns=["price_bucket", "leverage", "side", "volume", "liq_price"])

    # ==========================================================================
    # TIME-EVOLVING HEATMAP SCHEMA (Feature 008)
    # ==========================================================================

    def initialize_snapshot_tables(self) -> None:
        """Initialize liquidation snapshot tables (alias for ensure_snapshot_tables).

        Deprecated: Use ensure_snapshot_tables() instead.
        """
        return self.ensure_snapshot_tables()

    def ensure_snapshot_tables(self) -> None:
        """Ensure liquidation snapshot tables exist.

        Creates the following tables if they don't exist:
        - liquidation_snapshots: Pre-computed heatmap snapshots for caching
        - position_events: Audit trail of position lifecycle events

        Per spec.md Phase 2 and data-model.md.
        """
        # Create liquidation_snapshots table (aligned with production schema)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS liquidation_snapshots (
                id INTEGER PRIMARY KEY,
                timestamp TIMESTAMP NOT NULL,
                symbol VARCHAR(20) NOT NULL,
                price_bucket DOUBLE NOT NULL,
                leverage_tier VARCHAR DEFAULT NULL,
                side VARCHAR(10) DEFAULT NULL,
                active_volume DOUBLE DEFAULT 0,
                density INTEGER DEFAULT 0,
                model VARCHAR DEFAULT 'binance_standard',
                current_price DOUBLE DEFAULT NULL
            )
        """)

        # Create indexes for efficient queries
        try:
            self.conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_liq_snap_ts_sym
                ON liquidation_snapshots(timestamp, symbol)
            """)
            self.conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_liq_snap_price
                ON liquidation_snapshots(price_bucket)
            """)
        except Exception:
            pass  # Indexes may already exist

        # Create position_events table for audit trail
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS position_events (
                id INTEGER PRIMARY KEY,
                timestamp TIMESTAMP NOT NULL,
                symbol VARCHAR(20) NOT NULL,
                event_type VARCHAR(20) NOT NULL,
                entry_price DECIMAL(18, 8) NOT NULL,
                liq_price DECIMAL(18, 8) NOT NULL,
                volume DECIMAL(20, 8) NOT NULL,
                side VARCHAR(10) NOT NULL,
                leverage INTEGER NOT NULL
            )
        """)

        logger.info("Snapshot tables ensured (liquidation_snapshots, position_events)")

    def save_snapshot(
        self,
        snapshot,  # HeatmapSnapshot from models.position
    ) -> int:
        """Save a heatmap snapshot to the database.

        Persists the snapshot cells for later retrieval, enabling
        cache-first query strategy for fast API responses.

        Args:
            snapshot: HeatmapSnapshot with timestamp, symbol, and cells

        Returns:
            Number of rows inserted

        Per spec.md Phase 2 - database caching layer.
        """
        self.ensure_snapshot_tables()

        rows_inserted = 0

        # Insert each cell as a row (with upsert semantics for unique constraint)
        for price_bucket, cell in snapshot.cells.items():
            # Insert long density
            if cell.long_density > 0:
                # Check if row already exists (unique: timestamp, symbol, price_bucket, side)
                existing = self.conn.execute(
                    """
                    SELECT id FROM liquidation_snapshots
                    WHERE timestamp = ? AND symbol = ? AND price_bucket = ? AND side = 'long'
                    """,
                    [snapshot.timestamp, snapshot.symbol, str(price_bucket)],
                ).fetchone()

                if existing:
                    # Update existing row
                    self.conn.execute(
                        """
                        UPDATE liquidation_snapshots
                        SET active_volume = ?, density = 1
                        WHERE id = ?
                        """,
                        [float(cell.long_density), existing[0]],
                    )
                else:
                    # Get next id and insert new row
                    result = self.conn.execute(
                        "SELECT COALESCE(MAX(id), 0) + 1 FROM liquidation_snapshots"
                    ).fetchone()
                    next_id = result[0]

                    self.conn.execute(
                        """
                        INSERT INTO liquidation_snapshots
                        (id, timestamp, symbol, price_bucket, side, active_volume, density, model)
                        VALUES (?, ?, ?, ?, 'long', ?, 1, 'binance_standard')
                        """,
                        [
                            next_id,
                            snapshot.timestamp,
                            snapshot.symbol,
                            float(price_bucket),
                            float(cell.long_density),
                        ],
                    )
                    rows_inserted += 1

            # Insert short density
            if cell.short_density > 0:
                # Check if row already exists
                existing = self.conn.execute(
                    """
                    SELECT id FROM liquidation_snapshots
                    WHERE timestamp = ? AND symbol = ? AND price_bucket = ? AND side = 'short'
                    """,
                    [snapshot.timestamp, snapshot.symbol, str(price_bucket)],
                ).fetchone()

                if existing:
                    # Update existing row
                    self.conn.execute(
                        """
                        UPDATE liquidation_snapshots
                        SET active_volume = ?, density = 1
                        WHERE id = ?
                        """,
                        [float(cell.short_density), existing[0]],
                    )
                else:
                    # Get next id and insert new row
                    result = self.conn.execute(
                        "SELECT COALESCE(MAX(id), 0) + 1 FROM liquidation_snapshots"
                    ).fetchone()
                    next_id = result[0]

                    self.conn.execute(
                        """
                        INSERT INTO liquidation_snapshots
                        (id, timestamp, symbol, price_bucket, side, active_volume, density, model)
                        VALUES (?, ?, ?, ?, 'short', ?, 1, 'binance_standard')
                        """,
                        [
                            next_id,
                            snapshot.timestamp,
                            snapshot.symbol,
                            float(price_bucket),
                            float(cell.short_density),
                        ],
                    )
                    rows_inserted += 1

        logger.debug(f"Saved snapshot for {snapshot.symbol} at {snapshot.timestamp}")
        return rows_inserted

    def load_snapshots(
        self,
        symbol: str,
        start_time,
        end_time,
    ):
        """Load pre-computed heatmap snapshots from database.

        Retrieves cached snapshots for fast API responses. Returns
        empty list if no cached data exists.

        Args:
            symbol: Trading pair (e.g., BTCUSDT)
            start_time: Start of time range (datetime)
            end_time: End of time range (datetime)

        Returns:
            List of HeatmapSnapshot objects, or empty list

        Per spec.md Phase 2 - cache-first query strategy.
        """
        from collections import defaultdict

        from src.liquidationheatmap.models.position import HeatmapSnapshot

        self.ensure_snapshot_tables()

        try:
            result = self.conn.execute(
                """
                SELECT
                    timestamp,
                    symbol,
                    price_bucket,
                    side,
                    active_volume
                FROM liquidation_snapshots
                WHERE symbol = ?
                  AND timestamp >= ?
                  AND timestamp <= ?
                ORDER BY timestamp, price_bucket
                """,
                [symbol, start_time, end_time],
            ).fetchall()

            if not result:
                return []

            # Group by timestamp
            snapshots_by_ts = defaultdict(list)
            for row in result:
                ts, _sym, price_bucket, side, active_vol = row
                snapshots_by_ts[ts].append(
                    {
                        "price_bucket": price_bucket,
                        "side": side,
                        "active_volume": active_vol,
                    }
                )

            # Convert to list of HeatmapSnapshot objects
            snapshots = []
            for ts, cells_data in sorted(snapshots_by_ts.items()):
                snapshot = HeatmapSnapshot(timestamp=ts, symbol=symbol)
                # Reconstruct cells from stored data
                for cell_data in cells_data:
                    price_bucket = Decimal(str(cell_data["price_bucket"]))
                    cell = snapshot.get_cell(price_bucket)
                    if cell_data["side"] == "long":
                        cell.long_density = Decimal(str(cell_data["active_volume"]))
                    elif cell_data["side"] == "short":
                        cell.short_density = Decimal(str(cell_data["active_volume"]))
                snapshots.append(snapshot)

            logger.debug(f"Loaded {len(snapshots)} cached snapshots for {symbol}")
            return snapshots

        except Exception as e:
            logger.warning(f"Failed to load snapshots: {e}")
            return []
