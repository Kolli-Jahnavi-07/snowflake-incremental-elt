-- Operational checks for the pipeline. Run ad hoc or wire into an alert (CREATE ALERT ... IF EXISTS (...)).

-- 1. Task graph runs in the last 24 hours, failures first.
SELECT name, state, error_message, scheduled_time, completed_time,
       DATEDIFF(second, query_start_time, completed_time) AS seconds
FROM TABLE(LOANS.INFORMATION_SCHEMA.TASK_HISTORY(
       SCHEDULED_TIME_RANGE_START => DATEADD(hour, -24, CURRENT_TIMESTAMP()),
       RESULT_LIMIT => 1000))
ORDER BY IFF(state = 'FAILED', 0, 1), scheduled_time DESC;

-- 2. Snowpipe health: pending files and the last ingestion error, per pipe.
SELECT 'PIPE_LOAN_EVENTS' AS pipe, PARSE_JSON(SYSTEM$PIPE_STATUS('LOANS.RAW.PIPE_LOAN_EVENTS')) AS status
UNION ALL
SELECT 'PIPE_PAYMENTS', PARSE_JSON(SYSTEM$PIPE_STATUS('LOANS.RAW.PIPE_PAYMENTS'));

-- 3. Files that failed to load in the last 7 days (skipped whole because of ON_ERROR = SKIP_FILE).
SELECT file_name, status, first_error_message, last_load_time
FROM TABLE(LOANS.INFORMATION_SCHEMA.COPY_HISTORY(
       TABLE_NAME => 'LOANS.RAW.RAW_LOAN_EVENTS', START_TIME => DATEADD(day, -7, CURRENT_TIMESTAMP())))
WHERE status <> 'Loaded'
UNION ALL
SELECT file_name, status, first_error_message, last_load_time
FROM TABLE(LOANS.INFORMATION_SCHEMA.COPY_HISTORY(
       TABLE_NAME => 'LOANS.RAW.RAW_PAYMENTS', START_TIME => DATEADD(day, -7, CURRENT_TIMESTAMP())))
WHERE status <> 'Loaded';

-- 4. Streams close to going stale (a stale stream must be recreated and the table backfilled).
SHOW STREAMS IN DATABASE LOANS;
SELECT "name", "table_name", "stale", "stale_after"
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
WHERE "stale" = 'true' OR "stale_after" < DATEADD(day, 2, CURRENT_TIMESTAMP());

-- 5. Freshness: how far behind the newest event the current-state table is.
SELECT MAX(event_ts) AS newest_event, DATEDIFF(minute, MAX(event_ts), CURRENT_TIMESTAMP()) AS minutes_behind
FROM LOANS.CORE.LOAN_ACCOUNTS;

-- 6. Rejected rows by reason in the last day.
SELECT source, reason, COUNT(*) AS rows_rejected
FROM LOANS.CORE.REJECTED_EVENTS
WHERE rejected_at >= DATEADD(day, -1, CURRENT_TIMESTAMP())
GROUP BY source, reason
ORDER BY rows_rejected DESC;
