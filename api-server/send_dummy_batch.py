# send_dummy_batch.py - One-time dummy batch sender for cron
import requests
import json
import random
import datetime

API_URL = "http://localhost:8000/v1/load/features"
API_KEY = "944f89421611f0e94c76b1234c540c7a01b4bf5de7521bdc3e1671b8577943ad"  # your key

headers = {
    "Content-Type": "application/json",
    "X-API-Key": API_KEY
}

symbols = ["AAPL", "MSFT", "GOOGL", "TSLA", "NVDA"]

# Generate a small batch of dummy records
batch = []
now = datetime.datetime.utcnow()
for i in range(5):  # 5 records per batch
    symbol = random.choice(symbols)
    # Use slightly different times within the last 5 minutes
    dt = (now - datetime.timedelta(minutes=random.randint(0, 5))).isoformat()
    record = {
        "symbol": symbol,
        "datetime": dt,
        "price": round(random.uniform(100, 500), 2),
        "market_cap": round(random.uniform(1e11, 3e12), 2),
        "volume": random.randint(1000000, 50000000),
        "change_percentage": round(random.uniform(-5, 5), 4),
        "company_name": f"{symbol} Inc.",
        "sector": "Technology"
    }
    batch.append(record)

try:
    response = requests.post(API_URL, json=batch, headers=headers, timeout=10)
    print(f"[{datetime.datetime.now()}] Sent batch of {len(batch)} records")
    print("Response:", response.json())
except Exception as e:
    print(f"[{datetime.datetime.now()}] Error sending batch: {str(e)}")
