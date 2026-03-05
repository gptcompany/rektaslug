"""Gap-fill logic: bridge ccxt-data-pipeline Parquet -> DuckDB.

Extracted from scripts/fill_gap_from_ccxt.py for in-process use by the API.
Handles: klines (1m + 5m OHLCV), open interest, funding rate.
Venue: BINANCE only (consistent with CSV pipeline).
"""

import logging
import time
from pathlib import Path

import duckdb

logger = logging.getLogger(__name__)

VENUE = "BINANCE"
KLINE_INTERVALS = ("5m", "1m")
# For newly introduced intervals without CSV baseline, bootstrap a bounded window.
# 8 days covers 1w liq-map lookback with safety margin.
KLINE_BOOTSTRAP_DAYS_BY_INTERVAL = {"1m": 8}


def _kline_minutes(interval: str) -> int:
    if interval.endswith("m") and interval[:-1].isdigit():
        return int(interval[:-1])
    raise ValueError(f"Unsupported kline interval: {interval}")


def _ensure_klines_table(con: duckdb.DuckDBPyConnection, interval: str) -> str:
    table_name = f"klines_{interval}_history"
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            open_time TIMESTAMP NOT NULL,
            symbol VARCHAR NOT NULL,
            open DECIMAL(18, 8) NOT NULL,
            high DECIMAL(18, 8) NOT NULL,
            low DECIMAL(18, 8) NOT NULL,
            close DECIMAL(18, 8) NOT NULL,
            volume DECIMAL(18, 8) NOT NULL,
            close_time TIMESTAMP NOT NULL,
            quote_volume DECIMAL(20, 8),
            count INTEGER,
            taker_buy_volume DECIMAL(18, 8),
            taker_buy_quote_volume DECIMAL(20, 8),
            PRIMARY KEY (open_time, symbol)
        )
    """)
    return table_name


def parquet_glob(catalog: str, data_type: str, symbol: str) -> str:
    """Build glob path for ccxt-data-pipeline Parquet files."""
    return f"{catalog}/{data_type}/{symbol}-PERP.{VENUE}/*.parquet"


def get_watermark(con: duckdb.DuckDBPyConnection, table: str, ts_col: str, symbol: str):
    """Get MAX timestamp for a symbol in a table (the watermark)."""
    result = con.execute(
        f"SELECT MAX({ts_col}) FROM {table} WHERE symbol = ?", [symbol]
    ).fetchone()
    return result[0] if result and result[0] else None


def _fill_klines_interval(
    con: duckdb.DuckDBPyConnection,
    catalog: str,
    symbol: str,
    interval: str,
    dry_run: bool,
) -> dict:
    """Fill klines gap for a single interval from OHLCV Parquet files."""
    minutes = _kline_minutes(interval)
    table_name = f"klines_{interval}_history"
    if not dry_run:
        _ensure_klines_table(con, interval)
    glob_path = parquet_glob(catalog, "ohlcv", symbol)

    try:
        watermark = get_watermark(con, table_name, "open_time", symbol)
    except duckdb.CatalogException:
        # Table does not exist yet (e.g. dry_run on read-only connection)
        watermark = None

    bootstrap_days = KLINE_BOOTSTRAP_DAYS_BY_INTERVAL.get(interval)

    if watermark is None and bootstrap_days is None:
        logger.warning("No existing %s klines for %s, skipping (need CSV baseline first)", interval, symbol)
        return {"inserted": 0, "skipped": "no_baseline"}

    if watermark is None and bootstrap_days is not None:
        # Bootstrap: use a bounded time window instead of requiring CSV baseline
        logger.info("Bootstrapping %s klines for %s with %d-day window", interval, symbol, bootstrap_days)
        ts_filter = f"timestamp > (now() - INTERVAL '{bootstrap_days} days')"
    else:
        logger.info("Klines %s watermark for %s: %s", interval, symbol, watermark)
        ts_filter = f"timestamp > TIMESTAMP WITH TIME ZONE '{watermark} UTC'"

    try:
        avail = con.execute(f"""
            SELECT COUNT(*) FROM read_parquet('{glob_path}')
            WHERE venue = '{VENUE}'
              AND timeframe = '{interval}'
              AND {ts_filter}
        """).fetchone()[0]
    except Exception as e:
        logger.warning("No Parquet files found for %s klines %s: %s", interval, symbol, e)
        return {"inserted": 0, "skipped": "no_parquet"}

    if avail == 0:
        logger.info("No new %s klines data after watermark", interval)
        return {"inserted": 0, "skipped": "up_to_date"}

    logger.info("Found %d Parquet rows to fill for %s %s klines", avail, symbol, interval)

    if dry_run:
        return {"inserted": 0, "available": avail, "skipped": "dry_run"}

    count_before = con.execute(
        f"SELECT COUNT(*) FROM {table_name} WHERE symbol = ?", [symbol]
    ).fetchone()[0]

    con.execute(f"""
        INSERT OR IGNORE INTO {table_name}
            (open_time, symbol, open, high, low, close, volume,
             close_time, quote_volume, count, taker_buy_volume, taker_buy_quote_volume)
        SELECT
            timezone('UTC', timestamp)::TIMESTAMP AS open_time,
            REPLACE(symbol, '-PERP', '') AS symbol,
            CAST(open AS DECIMAL(18, 8)),
            CAST(high AS DECIMAL(18, 8)),
            CAST(low AS DECIMAL(18, 8)),
            CAST(close AS DECIMAL(18, 8)),
            CAST(volume AS DECIMAL(18, 8)),
            (timezone('UTC', timestamp)::TIMESTAMP + INTERVAL '{minutes} minutes') AS close_time,
            NULL AS quote_volume,
            NULL AS count,
            NULL AS taker_buy_volume,
            NULL AS taker_buy_quote_volume
        FROM read_parquet('{glob_path}')
        WHERE venue = '{VENUE}'
          AND timeframe = '{interval}'
          AND {ts_filter}
    """)

    count_after = con.execute(
        f"SELECT COUNT(*) FROM {table_name} WHERE symbol = ?", [symbol]
    ).fetchone()[0]

    inserted = count_after - count_before
    logger.info("Klines %s/%s: %d inserted, %d duplicates ignored", interval, symbol, inserted, avail - inserted)
    return {"inserted": inserted, "duplicates": avail - inserted}


def fill_klines(con: duckdb.DuckDBPyConnection, catalog: str, symbol: str, dry_run: bool) -> dict:
    """Fill klines gap for all configured intervals (5m, 1m).

    Returns a dict with per-interval results and total inserted count.
    """
    intervals_results: dict[str, dict] = {}
    total_inserted = 0

    for interval in KLINE_INTERVALS:
        result = _fill_klines_interval(con, catalog, symbol, interval, dry_run)
        intervals_results[interval] = result
        total_inserted += result.get("inserted", 0)

    return {"intervals": intervals_results, "inserted": total_inserted}


def fill_open_interest(
    con: duckdb.DuckDBPyConnection,
    catalog: str,
    symbol: str,
    dry_run: bool,
) -> dict:
    """Fill open_interest_history gap from OI Parquet files."""
    glob_path = parquet_glob(catalog, "open_interest", symbol)
    watermark = get_watermark(con, "open_interest_history", "timestamp", symbol)

    if watermark is None:
        logger.warning("No existing OI for %s, skipping (need CSV baseline first)", symbol)
        return {"inserted": 0, "skipped": "no_baseline"}

    logger.info("OI watermark for %s: %s", symbol, watermark)

    try:
        avail = con.execute(f"""
            SELECT COUNT(*) FROM read_parquet('{glob_path}')
            WHERE venue = '{VENUE}'
              AND timestamp > TIMESTAMP WITH TIME ZONE '{watermark} UTC'
        """).fetchone()[0]
    except Exception as e:
        logger.warning("No Parquet files found for OI %s: %s", symbol, e)
        return {"inserted": 0, "skipped": "no_parquet"}

    if avail == 0:
        logger.info("No new OI data after watermark")
        return {"inserted": 0, "skipped": "up_to_date"}

    logger.info("Found %d Parquet rows to fill for %s OI", avail, symbol)

    if dry_run:
        return {"inserted": 0, "available": avail, "skipped": "dry_run"}

    count_before = con.execute(
        "SELECT COUNT(*) FROM open_interest_history WHERE symbol = ?", [symbol]
    ).fetchone()[0]

    max_id = con.execute("SELECT COALESCE(MAX(id), 0) FROM open_interest_history").fetchone()[0]

    con.execute(f"""
        INSERT INTO open_interest_history
            (id, timestamp, symbol, open_interest_value, open_interest_contracts, source)
        SELECT
            ROW_NUMBER() OVER (ORDER BY p_ts) + {max_id} AS id,
            p_ts AS timestamp,
            p_sym AS symbol,
            CAST(p_oiv AS DECIMAL(20, 8)) AS open_interest_value,
            CAST(p_oi AS DECIMAL(18, 8)) AS open_interest_contracts,
            'ccxt-pipeline' AS source
        FROM (
            SELECT
                timezone('UTC', p.timestamp)::TIMESTAMP AS p_ts,
                REPLACE(p.symbol, '-PERP', '') AS p_sym,
                p.open_interest_value AS p_oiv,
                p.open_interest AS p_oi
            FROM read_parquet('{glob_path}') p
            WHERE p.venue = '{VENUE}'
              AND p.timestamp > TIMESTAMP WITH TIME ZONE '{watermark} UTC'
        ) sub
        WHERE NOT EXISTS (
            SELECT 1 FROM open_interest_history h
            WHERE h.timestamp = sub.p_ts AND h.symbol = sub.p_sym
        )
    """)

    count_after = con.execute(
        "SELECT COUNT(*) FROM open_interest_history WHERE symbol = ?", [symbol]
    ).fetchone()[0]

    inserted = count_after - count_before
    logger.info("OI %s: %d inserted, %d duplicates skipped", symbol, inserted, avail - inserted)
    return {"inserted": inserted, "duplicates": avail - inserted}


def fill_funding_rate(
    con: duckdb.DuckDBPyConnection,
    catalog: str,
    symbol: str,
    dry_run: bool,
) -> dict:
    """Fill funding_rate_history gap from funding rate Parquet files."""
    glob_path = parquet_glob(catalog, "funding_rate", symbol)
    watermark = get_watermark(con, "funding_rate_history", "timestamp", symbol)

    if watermark is None:
        logger.warning("No existing funding rate for %s, skipping (need CSV baseline first)", symbol)
        return {"inserted": 0, "skipped": "no_baseline"}

    logger.info("Funding rate watermark for %s: %s", symbol, watermark)

    try:
        avail = con.execute(f"""
            SELECT COUNT(*) FROM read_parquet('{glob_path}')
            WHERE venue = '{VENUE}'
              AND timestamp > TIMESTAMP WITH TIME ZONE '{watermark} UTC'
        """).fetchone()[0]
    except Exception as e:
        logger.warning("No Parquet files found for funding rate %s: %s", symbol, e)
        return {"inserted": 0, "skipped": "no_parquet"}

    if avail == 0:
        logger.info("No new funding rate data after watermark")
        return {"inserted": 0, "skipped": "up_to_date"}

    logger.info("Found %d Parquet rows to fill for %s funding rate", avail, symbol)

    if dry_run:
        return {"inserted": 0, "available": avail, "skipped": "dry_run"}

    count_before = con.execute(
        "SELECT COUNT(*) FROM funding_rate_history WHERE symbol = ?", [symbol]
    ).fetchone()[0]

    max_id = con.execute("SELECT COALESCE(MAX(id), 0) FROM funding_rate_history").fetchone()[0]

    con.execute(f"""
        INSERT INTO funding_rate_history
            (id, timestamp, symbol, funding_rate, funding_interval_hours)
        SELECT
            ROW_NUMBER() OVER (ORDER BY p_ts) + {max_id} AS id,
            p_ts AS timestamp,
            p_sym AS symbol,
            CAST(p_fr AS DECIMAL(10, 8)) AS funding_rate,
            8 AS funding_interval_hours
        FROM (
            SELECT
                timezone('UTC', p.timestamp)::TIMESTAMP AS p_ts,
                REPLACE(p.symbol, '-PERP', '') AS p_sym,
                p.funding_rate AS p_fr
            FROM read_parquet('{glob_path}') p
            WHERE p.venue = '{VENUE}'
              AND p.timestamp > TIMESTAMP WITH TIME ZONE '{watermark} UTC'
        ) sub
        WHERE NOT EXISTS (
            SELECT 1 FROM funding_rate_history h
            WHERE h.timestamp = sub.p_ts AND h.symbol = sub.p_sym
        )
    """)

    count_after = con.execute(
        "SELECT COUNT(*) FROM funding_rate_history WHERE symbol = ?", [symbol]
    ).fetchone()[0]

    inserted = count_after - count_before
    logger.info(
        "Funding rate %s: %d inserted, %d duplicates skipped",
        symbol,
        inserted,
        avail - inserted,
    )
    return {"inserted": inserted, "duplicates": avail - inserted}


def validate_gaps(con: duckdb.DuckDBPyConnection, symbol: str, watermark_ts):
    """Check for gaps in klines_5m after the original watermark (warning only)."""
    if watermark_ts is None:
        return

    result = con.execute(
        """
        SELECT COUNT(*) as gaps FROM (
            SELECT open_time,
                   LEAD(open_time) OVER (ORDER BY open_time) AS next_time
            FROM klines_5m_history
            WHERE symbol = ? AND open_time >= ?
        ) WHERE EXTRACT(EPOCH FROM (next_time - open_time)) > 300 * 1.5
    """,
        [symbol, watermark_ts],
    ).fetchone()[0]

    if result > 0:
        logger.warning(
            "Gap validation: %d gaps in klines %s after %s (may be exchange maintenance)",
            result,
            symbol,
            watermark_ts,
        )
    else:
        logger.info("Gap validation: no gaps in klines %s after %s", symbol, watermark_ts)


def run_gap_fill(
    db_path: str | Path,
    catalog: str | Path,
    symbols: list[str],
    dry_run: bool = False,
) -> dict:
    """Run the full gap-fill pipeline for all symbols.

    Opens a read-write DuckDB connection, fills klines/OI/funding for each
    symbol, validates gaps, and returns a summary dict.

    Args:
        db_path: Path to the DuckDB database file.
        catalog: Path to the ccxt-data-pipeline Parquet catalog.
        symbols: List of symbols to process (e.g. ["BTCUSDT", "ETHUSDT"]).
        dry_run: If True, count available data without writing.

    Returns:
        Dict with per-symbol results and total_inserted count.
    """
    catalog_path = Path(catalog)
    if not catalog_path.is_dir():
        raise FileNotFoundError(f"CCXT catalog not found: {catalog_path}")

    db = Path(db_path)
    if not db.exists():
        raise FileNotFoundError(f"DuckDB database not found: {db}")

    logger.info("Gap fill: catalog=%s db=%s symbols=%s dry_run=%s", catalog, db, symbols, dry_run)

    con = duckdb.connect(str(db), read_only=dry_run)
    summary: dict[str, dict] = {}

    try:
        for symbol in symbols:
            logger.info("=== Processing %s ===", symbol)
            symbol_results = {}

            klines_watermark = get_watermark(con, "klines_5m_history", "open_time", symbol)

            symbol_results["klines"] = fill_klines(con, str(catalog_path), symbol, dry_run)
            time.sleep(0.1)

            symbol_results["oi"] = fill_open_interest(con, str(catalog_path), symbol, dry_run)
            time.sleep(0.1)

            symbol_results["funding"] = fill_funding_rate(con, str(catalog_path), symbol, dry_run)
            time.sleep(0.1)

            if not dry_run:
                validate_gaps(con, symbol, klines_watermark)

            summary[symbol] = symbol_results
    finally:
        con.close()

    total_inserted = sum(
        r.get("inserted", 0)
        for sym_results in summary.values()
        for r in sym_results.values()
    )
    logger.info("Gap fill complete: %d total rows inserted", total_inserted)

    return {"symbols": summary, "total_inserted": total_inserted}
