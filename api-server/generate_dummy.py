import json
import datetime
import random

symbols = ["AAPL", "MSFT", "GOOGL", "TSLA", "NVDA"]

def generate_batch(size=10):
    batch = []
    now = datetime.datetime.utcnow()
    for i in range(size):
        symbol = random.choice(symbols)
        dt = now - datetime.timedelta(minutes=random.randint(0, 60))
        record = {
            "symbol": symbol,
            "datetime": dt.isoformat(),
            "price": round(random.uniform(100, 500), 2),
            "market_cap": round(random.uniform(1e11, 3e12), 2),
            "volume": random.randint(1000000, 50000000),
            "change_percentage": round(random.uniform(-5, 5), 4),
        }
        batch.append(record)
    return batch

if __name__ == "__main__":
    batch = generate_batch(5)
    print(json.dumps(batch, indent=2))
