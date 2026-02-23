## Market Data Simulation

A systemd timer runs `scripts/simulate_market_data.py` every 15 minutes, sending 4–8 randomized records to the dev endpoint (port 8001).

- Script location: `scripts/simulate_market_data.py`
- Timer: `market-data-simulator.timer`
- Service: `market-data-simulator.service`
- Current target: development environment only
