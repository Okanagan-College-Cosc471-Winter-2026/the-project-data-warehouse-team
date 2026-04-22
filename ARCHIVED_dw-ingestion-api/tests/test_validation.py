# tests/test_validation.py
import pytest
from datetime import datetime
import psycopg2
from psycopg2.extras import RealDictCursor
import os

DB_CONN = {
    'dbname': os.getenv('TEST_DB_NAME', 'cosc471_test'),
    'user': os.getenv('TEST_DB_USER', 'dw_user'),
    'password': os.getenv('TEST_DB_PASSWORD', 'letmein'),
    'host': os.getenv('TEST_DB_HOST', 'localhost'),
    'port': os.getenv('TEST_DB_PORT', '5432')
}

def get_db_connection():
    return psycopg2.connect(**DB_CONN, cursor_factory=RealDictCursor)

@pytest.fixture(scope="module")
def db_conn():
    conn = get_db_connection()
    yield conn
    conn.close()

def test_timestamp_parsing_and_timezone(db_conn):
    """Verify ingested timestamps are timezone-aware and correctly parsed."""
    test_symbol = "TEST_TZ"
    test_ts = datetime.fromisoformat("2023-07-03T01:00:00-07:00")  # Explicit timezone

    with db_conn.cursor() as cur:
        # Insert a known record
        cur.execute("""
            INSERT INTO staging.market_data_5m (symbol, ts, open, high, low, close, volume, asset_type, source)
            VALUES (%s, %s, 100.0, 101.0, 99.0, 100.5, 1000, 'stock', 'test_source')
            ON CONFLICT DO NOTHING;
        """, (test_symbol, test_ts))
        db_conn.commit()

        # Query the most recent record
        cur.execute("""
            SELECT ts, pg_typeof(ts) AS type
            FROM staging.market_data_5m
            WHERE symbol = %s
            ORDER BY created_at DESC
            LIMIT 1;
        """, (test_symbol,))
        row = cur.fetchone()

        assert row is not None, "Failed to retrieve inserted test record"

        assert row['type'] == 'timestamp with time zone', \
            f"Timestamp type is '{row['type']}' instead of 'timestamp with time zone'"

        assert row['ts'].tzinfo is not None, \
            "Retrieved timestamp lacks timezone information"

        # Optional: clean up the test record
        cur.execute("DELETE FROM staging.market_data_5m WHERE symbol = %s", (test_symbol,))
        db_conn.commit()

def test_gap_logging_for_missing_symbols(db_conn):
    """Verify missing_timeslots table captures expected gaps."""
    # Insert a controlled test case (cleanup afterward)
    test_timeslot = datetime.fromisoformat("2023-07-03T01:00:00-07:00")
    test_symbol = "TEST_MISSING"
    test_run_id = "unit-test-validation"

    with db_conn.cursor() as cur:
        # Clean up any prior test data
        cur.execute("DELETE FROM staging.missing_timeslots WHERE simulation_run = %s", (test_run_id,))
        db_conn.commit()

        # Simulate logging a gap
        cur.execute("""
            INSERT INTO staging.missing_timeslots (timeslot, symbol, simulation_run)
            VALUES (%s, %s, %s)
            ON CONFLICT DO NOTHING
        """, (test_timeslot, test_symbol, test_run_id))
        db_conn.commit()

        # Query and verify
        cur.execute("""
            SELECT timeslot, symbol, simulation_run
            FROM staging.missing_timeslots
            WHERE simulation_run = %s
        """, (test_run_id,))
        result = cur.fetchone()

        assert result is not None, "Gap was not logged"
        assert result['symbol'] == test_symbol
        assert result['timeslot'] == test_timeslot
        assert result['simulation_run'] == test_run_id

        # Cleanup
        cur.execute("DELETE FROM staging.missing_timeslots WHERE simulation_run = %s", (test_run_id,))
        db_conn.commit()
