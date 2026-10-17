-- Insert-only load of payments, idempotent on payment_id (re-sent payments are ignored).
MERGE INTO CORE.PAYMENTS t
USING (
  SELECT *
  FROM (
    SELECT
      payload:payment_id::STRING                       AS payment_id,
      payload:loan_id::STRING                          AS loan_id,
      TRY_TO_DECIMAL(payload:amount::STRING, 12, 2)    AS amount,
      TRY_TO_DATE(payload:due_date::STRING)            AS due_date,
      TRY_TO_DATE(payload:paid_date::STRING)           AS paid_date,
      LOWER(payload:channel::STRING)                   AS channel,
      loaded_at
    FROM RAW.RAW_PAYMENTS_STREAM
  )
  WHERE payment_id IS NOT NULL AND loan_id IS NOT NULL AND amount > 0
    AND due_date IS NOT NULL AND paid_date IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (PARTITION BY payment_id ORDER BY loaded_at) = 1
) s
ON t.payment_id = s.payment_id
WHEN NOT MATCHED THEN INSERT
  (payment_id, loan_id, amount, due_date, paid_date, days_late, channel, loaded_at)
VALUES
  (s.payment_id, s.loan_id, s.amount, s.due_date, s.paid_date,
   GREATEST(DATEDIFF(day, s.due_date, s.paid_date), 0), s.channel, s.loaded_at);
