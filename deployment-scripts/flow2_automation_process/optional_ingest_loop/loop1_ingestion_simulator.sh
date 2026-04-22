#!/usr/bin/env bash
# loop1_ingestion_simulator.sh
# Infinite ingestion loop: drips 5-min bars into the staging table

set -euo pipefail

module load StdEnv/2023 postgresql/16.0 2>/dev/null || true

DB_HOST="cedar-pgsql-vm"
DB_NAME="penguin_db"

echo "=== Staging Simulator Started - $(date '+%Y-%m-%d %H:%M:%S %Z') ==="

while true; do
    cycle_start=$(date '+%Y-%m-%d %H:%M:%S')
    echo "[${cycle_start}] Drip-feeding 5-minute historical window..."

    # 1. Log Start
    psql -h "${DB_HOST}" -d "${DB_NAME}" -v ON_ERROR_STOP=1 -c \
        "INSERT INTO pipeline.cycle_log (cycle_type, cycle_start_ts, status) VALUES ('ingestion', '${cycle_start}'::timestamptz, 'running');" > /dev/null 2>&1 || true

    # 2. Run Data Movement & Capture Raw Stats (Tuples Only Mode)
    stats=$(psql -h "${DB_HOST}" -d "${DB_NAME}" -t -A -F ',' -f ingest_simulator_data.sql 2>/dev/null) || stats="0,0"

    rows_ingested=$(echo "$stats" | cut -d',' -f1)
    symbols_ingested=$(echo "$stats" | cut -d',' -f2)

    rows_ingested=${rows_ingested:-0}
    symbols_ingested=${symbols_ingested:-0}

    # 3. Finalize Log using your dedicated script
    psql -h "${DB_HOST}" -d "${DB_NAME}" -v ON_ERROR_STOP=1 \
         -v cycle_start="${cycle_start}" \
         -v rows_ingested="${rows_ingested}" \
         -v symbols_ingested="${symbols_ingested}" \
         -f finalize_cycle.sql > /dev/null 2>&1 || true

    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Ingested ${rows_ingested} rows. Sleeping..."

    # Sleep to next 5-minute boundary + 5 second safety margin
    sleep $(( 300 - ($(date +%s) % 300) + 5 ))
done
