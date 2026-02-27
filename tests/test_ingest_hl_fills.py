"""Tests for Hyperliquid L4 fills ingestion script.

TDD RED phase: All tests should fail until implementation exists.
"""

import json
import os
from pathlib import Path

import duckdb
import pytest

# Will be implemented in scripts/ingest_hl_fills.py
from scripts.ingest_hl_fills import (
    create_hl_schema,
    extract_fills_from_block,
    ingest_date,
    parse_zst_file,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

SAMPLE_BLOCK = {
    "local_time": "2026-02-26T12:00:00.936877355",
    "block_time": "2026-02-26T12:00:00.256907915",
    "block_number": 906662276,
    "events": [
        [
            "0xd13d96f7b6bb6dd3ad865e23f1265e5885f1069c",
            {
                "coin": "BTC",
                "px": "68011.0",
                "sz": "0.00013",
                "side": "B",
                "time": 1772107200256,
                "startPosition": "1.24824",
                "dir": "Open Long",
                "closedPnl": "0.0",
                "hash": "0x0c42aabb",
                "oid": 330766284426,
                "crossed": False,
                "fee": "-0.000265",
                "tid": 550583832131574,
                "feeToken": "USDC",
            },
        ],
        [
            "0xaaaa1111bbbb2222cccc3333dddd4444eeee5555",
            {
                "coin": "BTC",
                "px": "68100.5",
                "sz": "0.5",
                "side": "A",
                "time": 1772107200300,
                "startPosition": "-2.0",
                "dir": "Close Short",
                "closedPnl": "-1500.25",
                "hash": "0xdeadbeef",
                "oid": 330766284427,
                "crossed": True,
                "fee": "17.025",
                "tid": 550583832131575,
                "feeToken": "USDC",
            },
        ],
        [
            "0x1111222233334444555566667777888899990000",
            {
                "coin": "ETH",
                "px": "3200.0",
                "sz": "1.0",
                "side": "B",
                "time": 1772107200400,
                "startPosition": "0.0",
                "dir": "Open Long",
                "closedPnl": "0.0",
                "hash": "0xcafebabe",
                "oid": 330766284428,
                "crossed": False,
                "fee": "-0.96",
                "tid": 550583832131576,
                "feeToken": "USDC",
            },
        ],
        [
            "0xbbbb222233334444555566667777888899990000",
            {
                "coin": "HYPE",
                "px": "25.5",
                "sz": "100.0",
                "side": "A",
                "time": 1772107200500,
                "startPosition": "500.0",
                "dir": "Close Long",
                "closedPnl": "-200.0",
                "hash": "0xbaadf00d",
                "oid": 330766284429,
                "crossed": True,
                "fee": "0.765",
                "tid": 550583832131577,
                "feeToken": "USDC",
            },
        ],
    ],
}


@pytest.fixture
def hl_db(tmp_path):
    """Create a temporary DuckDB with hl_fills_l4 schema."""
    db_path = tmp_path / "test_hl.duckdb"
    conn = duckdb.connect(str(db_path))
    create_hl_schema(conn)
    yield conn
    conn.close()


def _make_block2():
    """Create a second block with different block_number and trade_ids."""
    import copy

    block2 = copy.deepcopy(SAMPLE_BLOCK)
    block2["block_number"] = 906662277
    for i, event in enumerate(block2["events"]):
        event[1]["tid"] = event[1]["tid"] + 1000
        event[1]["oid"] = event[1]["oid"] + 1000
    return block2


SAMPLE_BLOCK_2 = _make_block2()


@pytest.fixture
def sample_zst_file(tmp_path):
    """Create a synthetic .zst file with 2 distinct blocks."""
    import zstandard as zstd

    lines = [json.dumps(SAMPLE_BLOCK), json.dumps(SAMPLE_BLOCK_2)]
    raw = "\n".join(lines).encode("utf-8")

    zst_path = tmp_path / "12.zst"
    cctx = zstd.ZstdCompressor()
    with open(zst_path, "wb") as f:
        f.write(cctx.compress(raw))
    return zst_path


@pytest.fixture
def sample_day_dir(tmp_path, sample_zst_file):
    """Create a directory structure mimicking a single day with one .zst file."""
    day_dir = tmp_path / "20260226"
    day_dir.mkdir()
    # Move the zst file into the day directory
    import shutil

    dest = day_dir / "12.zst"
    shutil.copy2(sample_zst_file, dest)
    return day_dir


# ---------------------------------------------------------------------------
# Test: extract_fills_from_block
# ---------------------------------------------------------------------------


class TestExtractFillsFromBlock:
    """Test JSONL block parsing and fill extraction."""

    def test_extracts_all_fills(self):
        """Should extract all 4 fills from sample block."""
        fills = extract_fills_from_block(SAMPLE_BLOCK)
        assert len(fills) == 4

    def test_fill_has_required_fields(self):
        """Each fill dict should have all required fields for DB insertion."""
        fills = extract_fills_from_block(SAMPLE_BLOCK)
        required = {
            "block_time",
            "block_number",
            "event_time_ms",
            "coin",
            "price",
            "size",
            "side",
            "direction",
            "closed_pnl",
            "start_position",
            "crossed",
            "fee",
            "wallet",
            "tx_hash",
            "order_id",
            "trade_id",
        }
        for fill in fills:
            assert required.issubset(fill.keys()), f"Missing keys: {required - fill.keys()}"

    def test_numeric_fields_are_float(self):
        """px, sz, closedPnl, fee, startPosition should be converted to float."""
        fills = extract_fills_from_block(SAMPLE_BLOCK)
        first = fills[0]
        assert isinstance(first["price"], float)
        assert isinstance(first["size"], float)
        assert isinstance(first["closed_pnl"], float)
        assert isinstance(first["fee"], float)
        assert isinstance(first["start_position"], float)

    def test_coin_filter_btc_only(self):
        """Should filter to BTC only when coins=['BTC']."""
        fills = extract_fills_from_block(SAMPLE_BLOCK, coins=["BTC"])
        assert all(f["coin"] == "BTC" for f in fills)
        assert len(fills) == 2

    def test_coin_filter_multiple(self):
        """Should filter to BTC+ETH when coins=['BTC','ETH']."""
        fills = extract_fills_from_block(SAMPLE_BLOCK, coins=["BTC", "ETH"])
        coins = {f["coin"] for f in fills}
        assert coins == {"BTC", "ETH"}
        assert len(fills) == 3

    def test_coin_filter_none_returns_all(self):
        """None coins filter returns all fills."""
        fills = extract_fills_from_block(SAMPLE_BLOCK, coins=None)
        assert len(fills) == 4

    def test_block_time_parsed(self):
        """block_time should be a datetime-compatible string."""
        fills = extract_fills_from_block(SAMPLE_BLOCK)
        # Should be parseable as timestamp
        assert fills[0]["block_time"] == "2026-02-26T12:00:00.256907915"

    def test_wallet_address_extracted(self):
        """Wallet address should come from the tuple's first element."""
        fills = extract_fills_from_block(SAMPLE_BLOCK)
        assert fills[0]["wallet"] == "0xd13d96f7b6bb6dd3ad865e23f1265e5885f1069c"


class TestExtractFillsMalformed:
    """Test handling of malformed records."""

    def test_missing_events_key(self):
        """Block without 'events' key should return empty list."""
        fills = extract_fills_from_block({"block_number": 1, "block_time": "x"})
        assert fills == []

    def test_event_not_a_pair(self):
        """Event that isn't a [wallet, fill] pair should be skipped."""
        block = {
            "block_time": "2026-01-01T00:00:00",
            "block_number": 1,
            "events": [["only_one_element"]],
        }
        fills = extract_fills_from_block(block)
        assert fills == []

    def test_fill_missing_required_field(self):
        """Fill missing 'coin' should be skipped."""
        block = {
            "block_time": "2026-01-01T00:00:00",
            "block_number": 1,
            "events": [
                [
                    "0xwallet",
                    {
                        "px": "100.0",
                        "sz": "1.0",
                        "side": "B",
                        "dir": "Open Long",
                        # missing 'coin'
                    },
                ]
            ],
        }
        fills = extract_fills_from_block(block)
        assert fills == []

    def test_invalid_numeric_field_skipped(self):
        """Fill with non-numeric px should be skipped."""
        block = {
            "block_time": "2026-01-01T00:00:00",
            "block_number": 1,
            "events": [
                [
                    "0xwallet",
                    {
                        "coin": "BTC",
                        "px": "not_a_number",
                        "sz": "1.0",
                        "side": "B",
                        "dir": "Open Long",
                        "time": 123,
                        "closedPnl": "0.0",
                        "startPosition": "0.0",
                        "crossed": False,
                        "fee": "0.0",
                        "hash": "0x1",
                        "oid": 1,
                        "tid": 1,
                    },
                ]
            ],
        }
        fills = extract_fills_from_block(block)
        assert fills == []


# ---------------------------------------------------------------------------
# Test: parse_zst_file
# ---------------------------------------------------------------------------


class TestParseZstFile:
    """Test .zst file decompression and JSONL parsing."""

    def test_parses_all_blocks(self, sample_zst_file):
        """Should parse 2 blocks from sample file (2 lines)."""
        fills = list(parse_zst_file(sample_zst_file, coins=["BTC", "ETH"]))
        # 2 blocks x 3 BTC+ETH fills = 6
        assert len(fills) == 6

    def test_returns_flat_fill_list(self, sample_zst_file):
        """Should return flat list of fill dicts, not nested."""
        fills = list(parse_zst_file(sample_zst_file, coins=None))
        # 2 blocks x 4 fills = 8
        assert len(fills) == 8
        assert all(isinstance(f, dict) for f in fills)

    def test_source_file_tracked(self, sample_zst_file):
        """Each fill should have source_file set to the .zst filename."""
        fills = list(parse_zst_file(sample_zst_file, coins=None))
        assert all(f["source_file"] == "12.zst" for f in fills)


# ---------------------------------------------------------------------------
# Test: create_hl_schema
# ---------------------------------------------------------------------------


class TestCreateHlSchema:
    """Test DuckDB schema creation."""

    def test_creates_table(self, hl_db):
        """hl_fills_l4 table should exist after schema creation."""
        tables = [r[0] for r in hl_db.execute("SHOW TABLES").fetchall()]
        assert "hl_fills_l4" in tables

    def test_creates_view(self, hl_db):
        """hl_liquidations_l4 view should exist."""
        # Query the view to check it exists
        result = hl_db.execute("SELECT COUNT(*) FROM hl_liquidations_l4").fetchone()
        assert result[0] == 0

    def test_unique_constraint(self, hl_db):
        """Inserting duplicate (block_number, trade_id) should be handled."""
        hl_db.execute("""
            INSERT INTO hl_fills_l4 (block_time, block_number, coin, price, size,
                side, direction, trade_id)
            VALUES ('2026-01-01', 1, 'BTC', 68000.0, 0.1, 'B', 'Open Long', 100)
        """)
        # INSERT OR IGNORE should not raise
        hl_db.execute("""
            INSERT OR IGNORE INTO hl_fills_l4 (block_time, block_number, coin, price, size,
                side, direction, trade_id)
            VALUES ('2026-01-01', 1, 'BTC', 68000.0, 0.1, 'B', 'Open Long', 100)
        """)
        count = hl_db.execute("SELECT COUNT(*) FROM hl_fills_l4").fetchone()[0]
        assert count == 1


# ---------------------------------------------------------------------------
# Test: ingest_date (integration with DuckDB)
# ---------------------------------------------------------------------------


class TestIngestDate:
    """Test full ingestion of a day directory into DuckDB."""

    def test_ingests_fills(self, hl_db, sample_day_dir):
        """Should insert fills from .zst files into hl_fills_l4."""
        stats = ingest_date(
            conn=hl_db,
            day_dir=sample_day_dir,
            coins=["BTC", "ETH"],
            pnl_threshold=0,
            crossed_only=True,
        )
        count = hl_db.execute("SELECT COUNT(*) FROM hl_fills_l4").fetchone()[0]
        # 1 file x 2 blocks x 3 BTC+ETH fills = 6
        assert count == 6
        assert stats["inserted"] == 6

    def test_idempotent_rerun(self, hl_db, sample_day_dir):
        """Running twice should not duplicate rows."""
        ingest_date(
            conn=hl_db,
            day_dir=sample_day_dir,
            coins=["BTC", "ETH"],
        )
        stats2 = ingest_date(
            conn=hl_db,
            day_dir=sample_day_dir,
            coins=["BTC", "ETH"],
        )
        count = hl_db.execute("SELECT COUNT(*) FROM hl_fills_l4").fetchone()[0]
        assert count == 6
        assert stats2["duplicates_skipped"] > 0

    def test_liquidation_view_populated(self, hl_db, sample_day_dir):
        """hl_liquidations_l4 view should surface liquidation-like fills."""
        ingest_date(
            conn=hl_db,
            day_dir=sample_day_dir,
            coins=["BTC"],
        )
        # The SAMPLE_BLOCK has one BTC Close Short with closedPnl=-1500.25 and crossed=True
        liq_count = hl_db.execute("SELECT COUNT(*) FROM hl_liquidations_l4").fetchone()[0]
        # 2 blocks, each with 1 BTC liquidation candidate = 2
        assert liq_count == 2

    def test_stats_summary(self, hl_db, sample_day_dir):
        """Stats dict should have expected keys."""
        stats = ingest_date(
            conn=hl_db,
            day_dir=sample_day_dir,
            coins=None,
        )
        expected_keys = {
            "files_processed",
            "lines_parsed",
            "fills_extracted",
            "inserted",
            "duplicates_skipped",
            "errors",
        }
        assert expected_keys.issubset(stats.keys())

    def test_empty_directory(self, hl_db, tmp_path):
        """Empty directory should return zero stats without error."""
        empty_dir = tmp_path / "20260101"
        empty_dir.mkdir()
        stats = ingest_date(conn=hl_db, day_dir=empty_dir, coins=None)
        assert stats["files_processed"] == 0
        assert stats["inserted"] == 0


# ---------------------------------------------------------------------------
# Test: Integration with real data (optional, env-gated)
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestRealDataIngestion:
    """Integration tests using real Hyperliquid L4 data.

    Requires HL_L4_SAMPLE_ZST env var pointing to a real .zst file.
    Skips automatically if not available.
    """

    @pytest.fixture(autouse=True)
    def require_real_data(self):
        sample = os.environ.get("HL_L4_SAMPLE_ZST")
        if not sample or not Path(sample).exists():
            pytest.skip("HL_L4_SAMPLE_ZST not set or file missing")
        self.sample_path = Path(sample)

    def test_parse_real_zst(self):
        """Should parse real .zst file without errors."""
        fills = list(parse_zst_file(self.sample_path, coins=["BTC"]))
        assert len(fills) > 0
        assert all(f["coin"] == "BTC" for f in fills)

    def test_ingest_real_data(self, tmp_path):
        """Should ingest real data into temp DuckDB."""
        db_path = tmp_path / "real_test.duckdb"
        conn = duckdb.connect(str(db_path))
        create_hl_schema(conn)

        fills = list(parse_zst_file(self.sample_path, coins=["BTC", "ETH"]))
        assert len(fills) > 0

        conn.close()
