#!/usr/bin/env bash
# agg_aggregate_loop.sh
# Infinite micro-batch aggregation loop synchronized with staging progress

set -euo pipefail

# Load PostgreSQL module if on DRAC/cluster
module load StdEnv/2023 postgresql/16.0 2>/dev/null || true

DB_HOST="cedar-pgsql-vm"
DB_NAME="penguin_db"

echo "=== Aggregation Loop Started - $(date '+%Y-%m-%d %H:%M:%S %Z') ==="

while true; do
    cycle_start=$(date '+%Y-%m-%d %H:%M:%S')
    echo "[${cycle_start}] Starting aggregation micro-batch check"

    # FIX 1: Added '|| true' equivalent to absorb the crash and log a warning
    psql -h "${DB_HOST}" -d "${DB_NAME}" -v ON_ERROR_STOP=1 <<-EOF || echo "[${cycle_start}] Warning: Network blip prevented start log."
        INSERT INTO pipeline.cycle_log (cycle_type, cycle_start_ts, status)
        VALUES ('aggregation', '${cycle_start}'::timestamptz, 'running')
        RETURNING cycle_id;
EOF

    # This is safe from 'set -e' because it is evaluated in an IF condition
    if ! psql -h "${DB_HOST}" -d "${DB_NAME}" -v ON_ERROR_STOP=1 \
              -f agg_run_batch.sql > agg_batch.out 2>&1; then
        echo "[${cycle_start}] Micro-batch FAILED — see agg_batch.out (Likely SSL drop)"
        sleep 60
        continue
    fi

    # Print the outcome to the console
    cat agg_batch.out | grep NOTICE || true

    # FIX 2: Added safety net to the final logging call
    psql -h "${DB_HOST}" -d "${DB_NAME}" -v ON_ERROR_STOP=1 \
         -v cycle_start="${cycle_start}" \
         -v windows_processed="1" \
         -f finalize_aggregation.sql > /dev/null 2>&1 || echo "[${cycle_start}] Warning: Network blip prevented final log."

    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Aggregation check finished. Sleeping for 60 seconds."

    # Wait 60 seconds before polling the staging table again
    sleep 60
done
