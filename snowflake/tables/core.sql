-- Current state of every loan account (one row per loan), maintained by MERGE from the raw stream.
CREATE TABLE IF NOT EXISTS CORE.LOAN_ACCOUNTS (
  loan_id          STRING        NOT NULL PRIMARY KEY,
  customer_id      STRING        NOT NULL,
  state            STRING,
  principal        NUMBER(12, 2),
  balance          NUMBER(12, 2),
  apr              NUMBER(6, 4),
  status           STRING,
  is_closed        BOOLEAN,
  event_ts         TIMESTAMP_NTZ NOT NULL,
  hashdiff         STRING        NOT NULL,
  updated_at       TIMESTAMP_NTZ NOT NULL
);

-- Full history of account changes (SCD Type 2), fed by a standard stream on LOAN_ACCOUNTS.
CREATE TABLE IF NOT EXISTS CORE.LOAN_ACCOUNT_HISTORY (
  loan_id          STRING        NOT NULL,
  customer_id      STRING        NOT NULL,
  state            STRING,
  balance          NUMBER(12, 2),
  apr              NUMBER(6, 4),
  status           STRING,
  is_closed        BOOLEAN,
  hashdiff         STRING        NOT NULL,
  valid_from       TIMESTAMP_NTZ NOT NULL,
  valid_to         TIMESTAMP_NTZ,
  is_current       BOOLEAN       NOT NULL
);

-- One row per payment, insert-only and de-duplicated on payment_id.
CREATE TABLE IF NOT EXISTS CORE.PAYMENTS (
  payment_id       STRING        NOT NULL PRIMARY KEY,
  loan_id          STRING        NOT NULL,
  amount           NUMBER(12, 2) NOT NULL,
  due_date         DATE          NOT NULL,
  paid_date        DATE          NOT NULL,
  days_late        NUMBER(5, 0)  NOT NULL,
  channel          STRING,
  loaded_at        TIMESTAMP_NTZ NOT NULL
);

-- Raw rows that failed validation, with the reason. Fed by a second stream on the raw table.
CREATE TABLE IF NOT EXISTS CORE.REJECTED_EVENTS (
  source           STRING        NOT NULL,
  payload          VARIANT,
  file_name        STRING,
  reason           STRING        NOT NULL,
  rejected_at      TIMESTAMP_NTZ NOT NULL
);
