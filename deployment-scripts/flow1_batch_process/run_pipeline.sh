#!/bin/bash
# ====================================================================
# SCRIPT: run_pipeline.sh
# PURPOSE: Master batch orchestrator. Loops through the defined historical
#          timeline month-by-month.
# ====================================================================

DB_HOST="cedar-pgsql-vm"
DB_NAME="penguin_db"

START_DATE="2023-01-01"
END_DATE="2026-01-01"

echo "🚀 INITIATING ML FEATURE STORE PIPELINE"
current_date="$START_DATE"

while [[ "$current_date" < "$END_DATE" ]]; do
    next_date=$(date -d "$current_date + 1 month" +%Y-%m-%d)
    
    if [[ "$next_date" > "$END_DATE" ]]; then
        next_date="$END_DATE"
    fi

    echo "📅 Processing Chunk: $current_date to $next_date"

    psql -h "$DB_HOST" -d "$DB_NAME" -v ON_ERROR_STOP=1 -c "CALL dw.build_warehouse_batch('$current_date', '$next_date');"
    if [ $? -ne 0 ]; then exit 1; fi

    psql -h "$DB_HOST" -d "$DB_NAME" -v ON_ERROR_STOP=1 -c "CALL dw.build_15m_bounded_ffill('$current_date', '$next_date');"
    if [ $? -ne 0 ]; then exit 1; fi
    
    current_date="$next_date"
done
echo "🎉 PIPELINE COMPLETE!"
