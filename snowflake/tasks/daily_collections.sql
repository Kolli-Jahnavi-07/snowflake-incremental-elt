-- Recompute only the dates that received new payments (including late-arriving ones),
-- instead of rebuilding the whole mart.
MERGE INTO MART.DAILY_COLLECTIONS d
USING (
  SELECT
    paid_date,
    COUNT(*)                     AS payments,
    SUM(amount)                  AS amount_collected,
    COUNT_IF(days_late > 0)      AS late_payments,
    ROUND(AVG(days_late), 2)     AS avg_days_late
  FROM CORE.PAYMENTS
  WHERE paid_date IN (SELECT DISTINCT paid_date FROM CORE.PAYMENTS_STREAM)
  GROUP BY paid_date
) s
ON d.paid_date = s.paid_date
WHEN MATCHED THEN UPDATE SET
  payments         = s.payments,
  amount_collected = s.amount_collected,
  late_payments    = s.late_payments,
  avg_days_late    = s.avg_days_late,
  refreshed_at     = CURRENT_TIMESTAMP()::TIMESTAMP_NTZ
WHEN NOT MATCHED THEN INSERT
  (paid_date, payments, amount_collected, late_payments, avg_days_late, refreshed_at)
VALUES
  (s.paid_date, s.payments, s.amount_collected, s.late_payments, s.avg_days_late, CURRENT_TIMESTAMP()::TIMESTAMP_NTZ);
