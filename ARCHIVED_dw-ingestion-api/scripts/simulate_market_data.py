#!/usr/bin/env python3
"""
Periodic market data simulator for the ingestion API.
Generates 4–8 realistic records and POSTs them to the dev endpoint every run.
"""

import argparse
import json
import random
from datetime import datetime, timedelta, timezone
import requests
from pathlib import Path
from typing import Union  # Added for backward-compatible type hints

# Configuration
API_URL = "http://localhost:8001/ingest/market-data/"
HEADERS = {"Content-Type": "application/json"}

SYMBOLS = ["AAPL", "TSLA", "BTCUSD", "XOM", "GLD", "SPY", "MSFT", "NVDA"]

def generate_record(prev_close: Union[float, None] = None) -> dict:
    """Generate one randomized market data record."""
    symbol = random.choice(SYMBOLS)
    now = datetime.now(timezone.utc)
    # Small random offset to avoid exact duplicate timestamps
    ts = now - timedelta(seconds=random.randint(0, 300))

    if prev_close is None:
        base_price = random.uniform(50.0, 5000.0)   # wide range to cover stocks/crypto
    else:
        base_price = prev_close

    # Random walk for close price
    change_pct = random.uniform(-1.8, 1.8) / 100
    close = base_price * (1 + change_pct)

    open_price = close * random.uniform(0.985, 1.015)
    high = max(open_price, close) * random.uniform(1.001, 1.025)
    low = min(open_price, close) * random.uniform(0.975, 0.999)

    volume = int(random.lognormvariate(13.0, 1.2))  #  ~200k to ~10M range

    # Simplified VWAP approximation
    vwap = (high + low + close) / 3 * random.uniform(0.995, 1.005)

    return {
        "symbol": symbol,
        "ts": ts.isoformat(),
        "open": round(open_price, 6),
        "high": round(high, 6),
        "low": round(low, 6),
        "close": round(close, 6),
        "volume": volume,
        "vwap": round(vwap, 6),
    }


def main(num_records: int = None):
    if num_records is None:
        num_records = random.randint(4, 8)

    print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] Simulating {num_records} market data records...")

    records = []
    prev_close = None

    for _ in range(num_records):
        record = generate_record(prev_close)
        records.append(record)
        prev_close = record["close"]  # chain small movements

    successes = 0
    failures = 0

    for record in records:
        try:
            response = requests.post(API_URL, headers=HEADERS, json=record, timeout=10)
            if response.status_code == 200:
                successes += 1
            else:
                failures += 1
                print(f"  Error {response.status_code}: {response.text}")
        except requests.RequestException as e:
            failures += 1
            print(f"  Request failed: {e}")

    print(f"Completed: {successes} successful, {failures} failed")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Market data ingestion simulator")
    parser.add_argument("--num", type=int, help="Number of records to send (default: random 4-8)")
    args = parser.parse_args()

    main(args.num)
