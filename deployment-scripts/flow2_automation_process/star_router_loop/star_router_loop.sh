#!/usr/bin/env bash
# star_router_loop.sh
# Indestructible Star Schema routing loop with High-Water Mark catch-up

set -euo pipefail
module load StdEnv/2023 postgresql/16.0 2>/dev/null || true

DB_HOST="cedar-pgsql-vm"
DB_NAME="penguin_db"

echo "=== Star Schema Routing Loop Started - $(date '+%Y-%m-%d %H:%M:%S %Z') ==="

while true; do
    cycle_start=$(date '+%Y-%m-%d %H:%M:%S')
    echo "➤ [${cycle_start}] Checking for new DW aggregations..."

    # Execute the High-Water Mark check and routing procedure.
    # The 'ON_ERROR_STOP' and '|| true' evaluations absorb dropped SSL
    # connections so the daemon survives network volatility without crashing.
    if ! psql -h "${DB_HOST}" -d "${DB_NAME}" -v ON_ERROR_STOP=1 -f star_run_batch.sql > star_batch.out 2>&1; then
        echo "[${cycle_start}] Router check FAILED — see star_batch.out"
    else
        cat star_batch.out | grep NOTICE || true
    fi

    echo "Sleeping for 300 seconds..."
    sleep 300
done
