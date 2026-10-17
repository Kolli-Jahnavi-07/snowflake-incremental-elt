-- Daily collections by paid date. Recomputed only for dates touched by new payments.
CREATE TABLE IF NOT EXISTS MART.DAILY_COLLECTIONS (
  paid_date        DATE          NOT NULL PRIMARY KEY,
  payments         NUMBER(10, 0) NOT NULL,
  amount_collected NUMBER(14, 2) NOT NULL,
  late_payments    NUMBER(10, 0) NOT NULL,
  avg_days_late    NUMBER(8, 2),
  refreshed_at     TIMESTAMP_NTZ NOT NULL
);
