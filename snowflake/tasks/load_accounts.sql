-- Upsert the latest image of each loan from new raw events.
--  * several events for one loan in a batch: keep the newest (QUALIFY)
--  * an event older than what is already stored (late or replayed): ignored
--  * invalid rows: skipped here, quarantined by the root task
MERGE INTO CORE.LOAN_ACCOUNTS t
USING (
  SELECT
    loan_id, customer_id, state, principal, balance, apr, status, is_closed, event_ts,
    SHA2(CONCAT_WS('|', customer_id, COALESCE(state, ''), COALESCE(CAST(balance AS STRING), ''),
                   COALESCE(CAST(apr AS STRING), ''), COALESCE(status, ''), CAST(is_closed AS STRING)), 256) AS hashdiff
  FROM (
    SELECT
      payload:loan_id::STRING                               AS loan_id,
      payload:customer_id::STRING                           AS customer_id,
      payload:state::STRING                                 AS state,
      TRY_TO_DECIMAL(payload:principal::STRING, 12, 2)      AS principal,
      TRY_TO_DECIMAL(payload:balance::STRING, 12, 2)        AS balance,
      TRY_TO_DECIMAL(payload:apr::STRING, 6, 4)             AS apr,
      UPPER(TRIM(payload:status::STRING))                   AS status,
      COALESCE(payload:event_type::STRING = 'closed', FALSE) AS is_closed,
      TRY_TO_TIMESTAMP_NTZ(payload:event_ts::STRING)        AS event_ts
    FROM RAW.RAW_LOAN_EVENTS_STREAM
  )
  WHERE loan_id IS NOT NULL AND customer_id IS NOT NULL AND event_ts IS NOT NULL
    AND balance IS NOT NULL AND balance >= 0
  QUALIFY ROW_NUMBER() OVER (PARTITION BY loan_id ORDER BY event_ts DESC) = 1
) s
ON t.loan_id = s.loan_id
WHEN MATCHED AND s.event_ts > t.event_ts THEN UPDATE SET
  customer_id = s.customer_id,
  state       = s.state,
  principal   = COALESCE(s.principal, t.principal),
  balance     = s.balance,
  apr         = s.apr,
  status      = s.status,
  is_closed   = s.is_closed,
  event_ts    = s.event_ts,
  hashdiff    = s.hashdiff,
  updated_at  = CURRENT_TIMESTAMP()::TIMESTAMP_NTZ
WHEN NOT MATCHED THEN INSERT
  (loan_id, customer_id, state, principal, balance, apr, status, is_closed, event_ts, hashdiff, updated_at)
VALUES
  (s.loan_id, s.customer_id, s.state, s.principal, s.balance, s.apr, s.status, s.is_closed, s.event_ts, s.hashdiff,
   CURRENT_TIMESTAMP()::TIMESTAMP_NTZ);
