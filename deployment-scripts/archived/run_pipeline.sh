#!/bin/bash
# ====================================================================
# SCRIPT: run_pipeline.sh
# PURPOSE: Master batch orchestrator. Loops through the defined historical
#          timeline month-by-month. Sequentially triggers the Aggregation 
#          engine followed by the Imputation engine.
# DEPENDENCIES: All previous SQL scripts (01 through 05) must be completed.
# EXECUTION: Recommended to run inside a `screen` session to prevent 
#            process death on SSH disconnect.
# ====================================================================

# ==========================================
# CONFIGURATION
# ==========================================
DB_HOST="cedar-pgsql-vm"
DB_NAME="penguin_db"

# Set the total historical range
START_DATE="2023-01-01"
END_DATE="2026-01-01"

echo "====================================================="
echo "🚀 INITIATING ML FEATURE STORE PIPELINE"
echo "Target Database: $DB_NAME @ $DB_HOST"
echo "Processing Range: $START_DATE to $END_DATE"
echo "====================================================="

# Initialize the current date tracking variable
current_date="$START_DATE"

# ==========================================
# MONTHLY CHUNKING LOOP
# ==========================================
while [[ "$current_date" < "$END_DATE" ]]; do
    
    # Calculate the first day of the NEXT month (Standard Linux date command)
    next_date=$(date -d "$current_date + 1 month" +%Y-%m-%d)
    
    # Safety catch to ensure we don't overshoot the final end date
    if [[ "$next_date" > "$END_DATE" ]]; then
        next_date="$END_DATE"
    fi

    echo ""
    echo "-----------------------------------------------------"
    echo "📅 Processing Chunk: $current_date to $next_date"
    echo "-----------------------------------------------------"

    # Step 1: Execute the Filtered Aggregation (Module 3)
    echo "⏳ Running dw.build_warehouse_batch()..."
    psql -h "$DB_HOST" -d "$DB_NAME" -v ON_ERROR_STOP=1 -c "CALL dw.build_warehouse_batch('$current_date', '$next_date');"
    
    if [ $? -ne 0 ]; then
        echo "❌ ERROR: Aggregation failed for $current_date. Halting pipeline."
        exit 1
    fi

    # Step 2: Execute the Bounded Forward-Fill (Module 4)
    echo "⏳ Running dw.build_15m_bounded_ffill()..."
    psql -h "$DB_HOST" -d "$DB_NAME" -v ON_ERROR_STOP=1 -c "CALL dw.build_15m_bounded_ffill('$current_date', '$next_date');"
    
    if [ $? -ne 0 ]; then
        echo "❌ ERROR: Imputation failed for $current_date. Halting pipeline."
        exit 1
    fi
    
    echo "✅ Chunk $current_date to $next_date completed successfully."

    # Advance the loop
    current_date="$next_date"
done

echo ""
echo "====================================================="
echo "🎉 PIPELINE COMPLETE: All data processed and imputed!"
echo "====================================================="
