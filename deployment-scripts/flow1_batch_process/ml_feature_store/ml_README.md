Note: For all database commands, change the db string from penguin_db to match yours.

The Machine Learning Feature Store

=== Overview ===

While the Star Schema is optimized for Business Intelligence and human readability, Machine Learning models (like XGBoost or Neural Networks) require a different data geometry.

This module takes the fully aggregated, forward-filled historical data (dw.extract_15m_ffill) and flattens it into a strictly numeric, highly optimized matrix. By replacing heavy text strings and timestamps with integer keys, this architecture drastically reduces the memory footprint and cuts full-table scan times by over 60%, making it the undisputed champion for feeding massive datasets into Python training scripts.

=== File Structure ===

To build and test the ML Feature Store, ensure the following scripts are present:

Initialization Scripts:

    00_teardown_ml_schema.sql (Drops the existing matrix for clean rebuilds)

    01_create_star_ml_schema.sql (Builds the integer-mapped matrix and indexes)

Performance & Verification Scripts:

    explain_analyze_raw.sql

    explain_analyze_wh.sql

    explain_analyze_ml.sql

    verify_feature_store.sql

    check_max_price_swing.sql

=== Phase 1: Building the Feature Store ===

Because this matrix is generated from historical data, it does not require an infinite Bash loop like the live tables. It is generated as a bulk batch process.

    Clean the Environment
    If you are regenerating the matrix after adding new features, safely drop the old schema:

Bash:
psql -d penguin_db -f 00_teardown_ml_schema.sql

    Generate the Numeric Matrix
    Run the build script. This will join the historical data against the Star Schema dimensions, strip out the text/timestamps, replace them with integers, and build the composite indexes.

Bash:
psql -d penguin_db -f 01_create_star_ml_schema.sql

=== Phase 2: Performance Profiling (The 60% Speedup) ===

This architecture reduces model ingestion query times from ~1,300 ms down to ~500 ms. You can demonstrate this hardware-level efficiency by running the three EXPLAIN ANALYZE scripts back-to-back:

Bash:
psql -d penguin_db -f explain_analyze_raw.sql
psql -d penguin_db -f explain_analyze_wh.sql
psql -d penguin_db -f explain_analyze_ml.sql

Why the ML Matrix Wins:

    Integer Math vs. String Parsing: The database groups by a 4-byte symbol_key integer instead of hashing variable-length text strings and parsing complex timezones.

    Dense Page Packing (Disk I/O): Because the rows are "skinnier" without text and timestamps, Postgres packs thousands of more rows into a single 8KB memory page, requiring far fewer disk reads.

    Surgical Time-Slot Filtering: The matrix allows you to filter market hours natively using time_slot_key BETWEEN 930 AND 1600. Doing this on the raw table requires wrapping the timestamp in heavy EXTRACT(HOUR...) functions, which forces the engine to scan millions of useless overnight rows.

=== Phase 3: Verification ===

To verify the integrity of the data after the matrix is built, run the verification queries.

Check the row counts and key boundaries:

Bash:
psql -d penguin_db -f verify_feature_store.sql

Validate the dimensional joins and price metrics against the Star Schema:

Bash:
psql -d penguin_db -f check_max_price_swing.sql
