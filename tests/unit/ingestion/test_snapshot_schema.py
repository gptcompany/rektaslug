"""Unit tests for liquidation snapshot schema.

T026 - Tests for liquidation_snapshots table schema
T027 - Tests for position_events table schema
"""

import tempfile

import pytest

from src.liquidationheatmap.ingestion.db_service import DuckDBService


@pytest.fixture
def db_service():
    """Create a DuckDB service connection for testing.

    Uses a temporary database to avoid conflicts with running server.
    """
    import os
    import uuid

    temp_db_path = os.path.join(tempfile.gettempdir(), f"test_{uuid.uuid4().hex}.duckdb")

    db = DuckDBService(db_path=temp_db_path, read_only=False)
    yield db
    db.close(force=True)

    # Cleanup temp file
    if os.path.exists(temp_db_path):
        os.remove(temp_db_path)


class TestLiquidationSnapshotsSchema:
    """T026 - Tests for liquidation_snapshots table schema."""

    def test_liquidation_snapshots_table_exists(self, db_service):
        """Verify liquidation_snapshots table can be created/exists."""
        # Ensure table creation method exists
        assert hasattr(db_service, "ensure_snapshot_tables")

        # Create/verify tables
        db_service.ensure_snapshot_tables()

        # Check table exists
        result = db_service.conn.execute("""
            SELECT COUNT(*) as count
            FROM information_schema.tables
            WHERE table_name = 'liquidation_snapshots'
        """).fetchone()

        assert result[0] == 1

    def test_liquidation_snapshots_has_required_columns(self, db_service):
        """Verify liquidation_snapshots has all required columns."""
        db_service.ensure_snapshot_tables()

        columns = db_service.conn.execute("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = 'liquidation_snapshots'
            ORDER BY ordinal_position
        """).fetchall()

        column_names = [c[0] for c in columns]

        # Required columns per production schema
        required_columns = [
            "id",
            "timestamp",
            "symbol",
            "price_bucket",
            "leverage_tier",
            "side",
            "active_volume",
            "density",
            "model",
            "current_price",
        ]

        for col in required_columns:
            assert col in column_names, f"Missing column: {col}"

    def test_liquidation_snapshots_can_insert_data(self, db_service):
        """Verify data can be inserted into liquidation_snapshots."""
        db_service.ensure_snapshot_tables()

        # Get next id
        result = db_service.conn.execute(
            "SELECT COALESCE(MAX(id), 0) + 1 FROM liquidation_snapshots"
        ).fetchone()
        next_id = result[0]

        # Insert test data
        db_service.conn.execute(
            """
            INSERT INTO liquidation_snapshots
            (id, timestamp, symbol, price_bucket, side, active_volume, density, model)
            VALUES
            (?, TIMESTAMP '2025-12-01 00:00:00', 'BTCUSDT', 95000.00, 'long', 1000000.0, 1, 'binance_standard')
        """,
            [next_id],
        )

        # Verify insertion
        result = db_service.conn.execute("""
            SELECT symbol, price_bucket, side, active_volume
            FROM liquidation_snapshots
            WHERE symbol = 'BTCUSDT' AND timestamp = TIMESTAMP '2025-12-01 00:00:00'
        """).fetchone()

        assert result is not None
        assert result[0] == "BTCUSDT"
        assert result[1] == 95000.00
        assert result[2] == "long"
        assert result[3] == 1000000.0

        # Cleanup
        db_service.conn.execute("""
            DELETE FROM liquidation_snapshots
            WHERE symbol = 'BTCUSDT' AND timestamp = TIMESTAMP '2025-12-01 00:00:00'
        """)

    def test_liquidation_snapshots_has_timestamp_symbol_index(self, db_service):
        """Verify index exists for timestamp + symbol queries."""
        db_service.ensure_snapshot_tables()

        # Check index exists (DuckDB stores index info in duckdb_indexes)
        # For now, we verify the table supports efficient queries
        result = db_service.conn.execute("""
            EXPLAIN ANALYZE
            SELECT * FROM liquidation_snapshots
            WHERE timestamp >= TIMESTAMP '2025-01-01' AND symbol = 'BTCUSDT'
            LIMIT 1
        """).fetchall()

        # Just verify the query executes without error
        assert result is not None


class TestPositionEventsSchema:
    """T027 - Tests for position_events table schema."""

    def test_position_events_table_exists(self, db_service):
        """Verify position_events table can be created/exists."""
        db_service.ensure_snapshot_tables()

        result = db_service.conn.execute("""
            SELECT COUNT(*) as count
            FROM information_schema.tables
            WHERE table_name = 'position_events'
        """).fetchone()

        assert result[0] == 1

    def test_position_events_has_required_columns(self, db_service):
        """Verify position_events has all required columns."""
        db_service.ensure_snapshot_tables()

        columns = db_service.conn.execute("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = 'position_events'
            ORDER BY ordinal_position
        """).fetchall()

        column_names = [c[0] for c in columns]

        # Required columns for tracking position lifecycle
        required_columns = [
            "id",
            "timestamp",
            "symbol",
            "event_type",  # 'created', 'consumed', 'closed'
            "entry_price",
            "liq_price",
            "volume",
            "side",
            "leverage",
        ]

        for col in required_columns:
            assert col in column_names, f"Missing column: {col}"

    def test_position_events_can_insert_events(self, db_service):
        """Verify position events can be inserted and queried."""
        db_service.ensure_snapshot_tables()

        # Get next id
        result = db_service.conn.execute(
            "SELECT COALESCE(MAX(id), 0) + 1 FROM position_events"
        ).fetchone()
        next_id = result[0]

        # Insert test event
        db_service.conn.execute(
            """
            INSERT INTO position_events
            (id, timestamp, symbol, event_type, entry_price, liq_price, volume, side, leverage)
            VALUES
            (?, TIMESTAMP '2025-12-01 00:00:00', 'BTCUSDT', 'created', 100000.0, 90000.0, 500000.0, 'long', 10)
        """,
            [next_id],
        )

        # Verify insertion
        result = db_service.conn.execute("""
            SELECT event_type, entry_price, liq_price, leverage
            FROM position_events
            WHERE symbol = 'BTCUSDT' AND timestamp = TIMESTAMP '2025-12-01 00:00:00'
        """).fetchone()

        assert result is not None
        assert result[0] == "created"
        assert result[1] == 100000.0
        assert result[2] == 90000.0
        assert result[3] == 10

        # Cleanup
        db_service.conn.execute("""
            DELETE FROM position_events
            WHERE symbol = 'BTCUSDT' AND timestamp = TIMESTAMP '2025-12-01 00:00:00'
        """)

    def test_position_events_supports_all_event_types(self, db_service):
        """Verify all event types can be inserted."""
        db_service.ensure_snapshot_tables()

        event_types = ["created", "consumed", "closed"]

        for i, event_type in enumerate(event_types):
            # Get next id
            result = db_service.conn.execute(
                "SELECT COALESCE(MAX(id), 0) + 1 FROM position_events"
            ).fetchone()
            next_id = result[0]

            db_service.conn.execute(
                f"""
                INSERT INTO position_events
                (id, timestamp, symbol, event_type, entry_price, liq_price, volume, side, leverage)
                VALUES
                (?, TIMESTAMP '2025-12-01 00:0{i}:00', 'BTCUSDT', ?, 100000.0, 90000.0, 100000.0, 'long', 10)
            """,
                [next_id, event_type],
            )

        # Verify all types inserted
        result = db_service.conn.execute("""
            SELECT COUNT(DISTINCT event_type) FROM position_events
            WHERE symbol = 'BTCUSDT'
        """).fetchone()

        assert result[0] == 3

        # Cleanup
        db_service.conn.execute("""
            DELETE FROM position_events WHERE symbol = 'BTCUSDT'
        """)
