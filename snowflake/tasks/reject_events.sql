-- Root task body: quarantine raw rows that fail validation.
-- Reads the *_DQ streams, which are separate from the streams the load tasks consume,
-- so rejecting a row never moves the load tasks' offsets.
INSERT INTO CORE.REJECTED_EVENTS (source, payload, file_name, reason, rejected_at)
SELECT source, payload, file_name, reason, CURRENT_TIMESTAMP()::TIMESTAMP_NTZ
FROM (
  SELECT
    'loan_events' AS source,
    payload,
    file_name,
    CASE
      WHEN payload:loan_id::STRING IS NULL THEN 'missing loan_id'
      WHEN payload:customer_id::STRING IS NULL THEN 'missing customer_id'
      WHEN TRY_TO_TIMESTAMP_NTZ(payload:event_ts::STRING) IS NULL THEN 'invalid event_ts'
      WHEN TRY_TO_DECIMAL(payload:balance::STRING, 12, 2) IS NULL THEN 'invalid balance'
      WHEN TRY_TO_DECIMAL(payload:balance::STRING, 12, 2) < 0 THEN 'negative balance'
    END AS reason
  FROM RAW.RAW_LOAN_EVENTS_DQ_STREAM
  UNION ALL
  SELECT
    'payments' AS source,
    payload,
    file_name,
    CASE
      WHEN payload:payment_id::STRING IS NULL THEN 'missing payment_id'
      WHEN payload:loan_id::STRING IS NULL THEN 'missing loan_id'
      WHEN TRY_TO_DECIMAL(payload:amount::STRING, 12, 2) IS NULL THEN 'invalid amount'
      WHEN TRY_TO_DECIMAL(payload:amount::STRING, 12, 2) <= 0 THEN 'non-positive amount'
      WHEN TRY_TO_DATE(payload:due_date::STRING) IS NULL THEN 'invalid due_date'
      WHEN TRY_TO_DATE(payload:paid_date::STRING) IS NULL THEN 'invalid paid_date'
    END AS reason
  FROM RAW.RAW_PAYMENTS_DQ_STREAM
)
WHERE reason IS NOT NULL;
