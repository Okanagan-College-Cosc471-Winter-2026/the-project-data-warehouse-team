```
## Market Data Simulation

A systemd timer runs `scripts/simulate_market_data.py` every 15 minutes, sending 4–8 randomized records to the dev endpoint (port 8001).

- Script location: `scripts/simulate_market_data.py`
- Timer: `market-data-simulator.timer`
- Service: `market-data-simulator.service`
- Current target: development environment only

# Cheat Sheet
API status:         sudo systemctl status dw-api-{dev,test,prod}.service
Simulation logs:    journalctl -u market-data-simulator.service -e
Backup info:        sudo -u postgres pgbackrest info --stanza=main-cluster
Backup check:       sudo -u postgres pgbackrest check --stanza=main-cluster
Last backup logs:   journalctl -u pgbackrest-daily.service -e
Force backup:       sudo -u postgres pgbackrest --stanza=main-cluster --repo=1 backup
Dry-run restore:    sudo -u postgres pgbackrest --stanza=main-cluster --repo=1 restore --dry-run
Recent rows:        psql -U dw_alex -d cosc471_dev -c "SELECT ... FROM staging.market_data ORDER BY ts DESC LIMIT 10;"
Timers overview:    sudo systemctl list-timers --all

# Create SSH Key Credential
ssh-keygen -t ed25519 -C "add email here"
# Copy paste it to Alex
cat ~/.ssh/id_ed25519.pub


# Start environment
ssh -i ~/.ssh/id_ed25519_cosc471 cosc-admin@10.12.43.85

cd /home/cosc-admin/projects/the-project-data-warehouse-team/dw-ingestion-api
python3.9 -m venv venv
source venv/bin/activate
echo $VIRTUAL_ENV

# Data Warehouse Ingestion API

This project implements a FastAPI-based ingestion service for staging financial market data into PostgreSQL databases. The API accepts JSON payloads via POST requests, validates them, and inserts records into a staging table with conflict handling. The system supports three isolated environments (development, test, production) with separate databases and persistent systemd services.

## Project Overview

- **Purpose**: Provide a reliable REST endpoint for ingesting market data (OHLCV + VWAP) from the data collection team into a data warehouse staging layer.
- **Core Technologies**:
  - FastAPI (Python web framework)
  - SQLAlchemy + asyncpg (async PostgreSQL driver)
  - Pydantic (data validation and settings management)
  - PostgreSQL 16 (three separate databases: cosc471_dev, cosc471_test, cosc471_prod)
  - Systemd (persistent service management)
  - pgBackRest (backup and recovery automation)

## Repository Structure
dw-ingestion-api/
├── src/
│   ├── config.py               # Environment-aware settings loading
│   ├── main.py                 # FastAPI app entry point
│   ├── database.py             # SQLAlchemy async session management
│   ├── schemas/
│   │   └── market_data.py      # Pydantic model for market data payload
│   ├── routers/
│   │   └── ingestion.py        # Ingestion endpoint (/ingest/market-data/)
│   └── services/
│       └── ingestion_service.py # Database insertion logic with commit
├── scripts/
│   └── simulate_market_data.py # Periodic simulation script
├── tests/                      # (optional – unit/integration tests)
├── requirements.txt
├── .gitignore
├── README.md                   # This file
└── .env.example                # Template for environment variables
text**Note**: Real `.env.*` files, virtual environments (`venv/`), and system-level configurations (systemd units, pgBackRest config) are **not** committed to Git for security and environment-specific reasons.

## Environments and Databases

| Environment   | Port | Database       | .env File          | Systemd Service         | Purpose                        |
|---------------|------|----------------|--------------------|-------------------------|--------------------------------|
| Production    | 8000 | cosc471_prod   | .env.production    | dw-api-prod.service     | Final staging environment      |
| Development   | 8001 | cosc471_dev    | .env.development   | dw-api-dev.service      | Development & debugging        |
| Test          | 8002 | cosc471_test   | .env.testing       | dw-api-test.service     | Integration & validation       |

All environments use the same codebase but load different `DATABASE_URL` values via the `ENVIRONMENT` variable.

## Setup Instructions

1. **Clone the repository**:
   ```bash
   git clone <repository-url>
   cd dw-ingestion-api

Create virtual environment and install dependencies:Bashpython3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
Create environment files (do not commit these):Bash# .env.development
DATABASE_URL=postgresql+asyncpg://dw_user:letmein@localhost:5432/cosc471_dev
ENVIRONMENT=development(Repeat for .env.testing and .env.production with appropriate database names.)
Create systemd service files (as root or with sudo):
/etc/systemd/system/dw-api-prod.service (port 8000)
/etc/systemd/system/dw-api-dev.service (port 8001)
/etc/systemd/system/dw-api-test.service (port 8002)
Example for production (adjust port and EnvironmentFile for dev/test):ini[Unit]
Description=Data Warehouse Ingestion API - Production
After=network.target postgresql-16.service

[Service]
User=cosc-admin
Group=cosc-admin
WorkingDirectory=/home/cosc-admin/projects/the-project-data-warehouse-team/dw-ingestion-api
EnvironmentFile=/etc/dw-api/prod.env
ExecStart=/home/cosc-admin/projects/the-project-data-warehouse-team/dw-ingestion-api/venv/bin/python -m uvicorn src.main:app --host 0.0.0.0 --port 8000
Restart=always
RestartSec=5
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
Reload systemd and enable/start services:Bashsudo systemctl daemon-reload
sudo systemctl enable dw-api-{prod,dev,test}.service
sudo systemctl start dw-api-{prod,dev,test}.service
Verify services:Bashsudo systemctl status dw-api-{prod,dev,test}.service
sudo lsof -i :8000 -i :8001 -i :8002

Testing the API
Send a sample POST request to any environment:
Bashcurl -X POST http://localhost:8001/ingest/market-data/ \
  -H "Content-Type: application/json" \
  -d '{
        "symbol": "TESTSYMBOL",
        "ts": "2026-02-20T13:48:00-08:00",
        "open": 14000.123456,
        "high": 14001.234567,
        "low": 13999.876543,
        "close": 14000.567890,
        "volume": 66666666,
        "vwap": 14000.345678
      }'
Expected response: {"status":"success"}
Verify insertion (adjust database name):
Bashpsql -U dw_alex -d cosc471_dev -h localhost -c "SELECT * FROM staging.market_data WHERE symbol = 'TESTSYMBOL' ORDER BY ts DESC LIMIT 1;"
Periodic Data Simulation (Load Generator)
A systemd timer runs every 15 minutes to simulate incoming market data by sending randomized records to the development endpoint (port 8001).

Script: scripts/simulate_market_data.py
Records per cycle: 4–8 (randomized)
Symbols: AAPL, TSLA, BTCUSD, XOM, GLD, SPY, MSFT, NVDA
Systemd units:
Service: market-data-simulator.service
Timer: market-data-simulator.timer

Logs: journalctl -u market-data-simulator.service -e
Purpose: Continuous testing of ingestion pipeline, WAL activity, and backup behavior under realistic load

To disable temporarily (e.g., during maintenance):
Bashsudo systemctl stop market-data-simulator.timer
To force a run:
Bashsudo systemctl start market-data-simulator.service
Backup and Recovery (pgBackRest)
Primary tool: pgBackRest (automated full/incremental backups + WAL archiving)
Fallback: Manual pg_basebackup script (educational only, not automated)
Repositories:

repo1: Local fast disk (/var/lib/pgbackrest-local) — primary for quick restores
repo2: Secondary storage (/mnt/backup-secondary/pgbackrest) — manual full backups to date
(Planned) repo3: S3-compatible cloud storage for geo-redundancy

Stanza: main-cluster
Configuration file: /etc/pgbackrest.conf
WAL archiving: Enabled via archive_command = 'pgbackrest --stanza=main-cluster archive-push %p'
Automation: Daily incremental backups to repo1 via pgbackrest-daily.timer (runs at 02:30)
Current status (as of 2026-02-23):

Full backups: Multiple in repo1, one in repo2
Incremental backups: Confirmed working (first automated incremental at 02:30 on 2026-02-23)
WAL archiving: Continuous to both repositories

Verification commands:
Bashsudo -u postgres pgbackrest info --stanza=main-cluster
journalctl -u pgbackrest-daily.service -e
Recovery testing:
Bashsudo -u postgres pgbackrest --stanza=main-cluster --repo=1 restore --dry-run
Security / Limitations:

All repositories are currently local to the server (vulnerable to total hardware failure).
Planned improvements: External USB drive (air-gapped) + S3 repo3 (off-site).
Temporary password letmein — rotation required before production use.

Monitoring & Logs

API services: sudo systemctl status dw-api-{dev,test,prod}.service
Simulation timer: sudo systemctl status market-data-simulator.timer
Backup timer: sudo systemctl status pgbackrest-daily.timer
Recent logs:Bashjournalctl -u dw-api-dev.service -e
journalctl -u market-data-simulator.service -e
journalctl -u pgbackrest-daily.service -e
PostgreSQL logs: /var/lib/pgsql/16/data/log/ or journalctl -u postgresql-16.service

Development Workflow

Branch from main for new features: git checkout -b feature/<name>
Commit frequently with descriptive messages
Test manually before enabling timers/services
Update this README after significant changes
Use formal commit messages and keep branches focused

Security Notes

Temporary password letmein for dw_user — must be rotated before production use.
Services currently bind to 0.0.0.0 — change to 127.0.0.1 if no external access is required.
Environment files contain credentials — never commit to Git.
```
