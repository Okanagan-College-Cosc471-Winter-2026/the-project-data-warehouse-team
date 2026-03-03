#!/usr/bin/env python3
"""
Historical market data simulator with gap logging.
Fetches 5-min records from historical.market_data_5m and POSTs them sequentially
to the historical endpoint (single record per POST).
Missing symbols per timeslot are logged to staging.missing_timeslots.
"""
import argparse
import requests
import time
from datetime import datetime
import psycopg2
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# ────────────────────────────────────────────────
# Configuration
# ────────────────────────────────────────────────
DB_CONN = {
    'dbname':   'cosc471_test',
    'user':     'dw_user',
    'password': 'letmein',
    'host':     'localhost'
}
API_URL   = "http://localhost:8002/ingest/historical/single/"
HEADERS   = {"Content-Type": "application/json"}
SYMBOLS   = ["AAPL", "AMD", "AMZN", "BA"]

def get_db_connection():
    return psycopg2.connect(**DB_CONN)

def log_missing_timeslot(timeslot, missing_symbols, run_id=None):
    """Log missing symbols for a given timeslot to staging.missing_timeslots."""
    if not missing_symbols:
        return
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        for symbol in missing_symbols:
            cur.execute("""
                INSERT INTO staging.missing_timeslots (timeslot, symbol, simulation_run)
                VALUES (%s, %s, %s)
                ON CONFLICT DO NOTHING
            """, (timeslot, symbol, run_id))
        conn.commit()
        logger.info(f"Logged {len(missing_symbols)} missing symbol(s) for {timeslot}")
    except Exception as e:
        logger.error(f"Failed to log missing timeslot {timeslot}: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()

# ────────────────────────────────────────────────
# Data access helpers
# ────────────────────────────────────────────────
def get_earliest_timeslot():
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT MIN(ts) FROM historical.market_data_5m")
            return cur.fetchone()[0]

def get_next_timeslot(current_ts):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT MIN(ts) FROM historical.market_data_5m WHERE ts > %s", (current_ts,))
            return cur.fetchone()[0]

def get_record_for_symbol_and_ts(symbol, ts):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT symbol, ts, open, high, low, close, volume, asset_type, source
                FROM historical.market_data_5m
                WHERE symbol = %s AND ts = %s
            """, (symbol, ts))
            row = cur.fetchone()
            if row:
                return {
                    "symbol":     row[0],
                    "ts":         row[1].isoformat(),
                    "open":       float(row[2]),
                    "high":       float(row[3]),
                    "low":        float(row[4]),
                    "close":      float(row[5]),
                    "volume":     int(row[6]),
                    "asset_type": row[7],
                    "source":     row[8]
                }
    return None

# ────────────────────────────────────────────────
# Main simulation logic
# ────────────────────────────────────────────────
def main(max_timeslots: int = 20, run_id: str = None):
    current_ts = get_earliest_timeslot()
    if not current_ts:
        logger.error("No data found in historical.market_data_5m")
        return

    logger.info(f"Starting historical simulation from {current_ts} (run_id: {run_id or 'manual'})")
    successes = failures = processed_timeslots = 0

    while current_ts and processed_timeslots < max_timeslots:
        logger.info(f"Processing timeslot: {current_ts}")
        missing_symbols = []

        for symbol in SYMBOLS:
            record = get_record_for_symbol_and_ts(symbol, current_ts)
            if record:
                try:
                    response = requests.post(API_URL, headers=HEADERS, json=record, timeout=10)
                    if response.status_code == 200:
                        successes += 1
                        logger.info(f"Success: {symbol} at {current_ts}")
                    else:
                        failures += 1
                        logger.error(f"Error {response.status_code} for {symbol} at {current_ts}: {response.text}")
                except requests.RequestException as e:
                    failures += 1
                    logger.error(f"Request failed for {symbol} at {current_ts}: {e}")
            else:
                missing_symbols.append(symbol)

        if missing_symbols:
            logger.warning(f"Missing symbols at {current_ts}: {', '.join(missing_symbols)}")
            log_missing_timeslot(current_ts, missing_symbols, run_id)

        processed_timeslots += 1
        next_ts = get_next_timeslot(current_ts)
        if next_ts:
            current_ts = next_ts
            time.sleep(2)  # Adjustable delay to simulate real-time rate
        else:
            break

    logger.info(f"Simulation complete. Processed {processed_timeslots} timeslots. "
                f"Successes: {successes}, Failures: {failures}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Historical market data ingestion simulator with gap logging")
    parser.add_argument("--max",    type=int,   default=20,  help="Maximum timeslots to process (default: 20)")
    parser.add_argument("--run-id", type=str,   default=None, help="Optional identifier for this simulation run")
    args = parser.parse_args()
    main(args.max, args.run_id)
