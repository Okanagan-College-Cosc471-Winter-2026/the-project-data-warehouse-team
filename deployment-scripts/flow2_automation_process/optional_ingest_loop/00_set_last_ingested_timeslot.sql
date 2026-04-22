-- Example: Set the simulator to start "playing" data from July 2024
UPDATE staging.ingestion_progress 
SET last_ingested_ts = '2024-07-01 06:25:00-08';
