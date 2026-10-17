-- Raw tables keep the event exactly as received plus load metadata.
CREATE TABLE IF NOT EXISTS RAW.RAW_LOAN_EVENTS (
  payload   VARIANT,
  file_name STRING,
  loaded_at TIMESTAMP_NTZ
) CHANGE_TRACKING = TRUE;  -- streams are created by another role, which cannot enable tracking itself
CREATE TABLE IF NOT EXISTS RAW.RAW_PAYMENTS (
  payload   VARIANT,
  file_name STRING,
  loaded_at TIMESTAMP_NTZ
) CHANGE_TRACKING = TRUE;
