-- SCD Type 2 in a single MERGE, driven by the standard stream on CORE.LOAN_ACCOUNTS.
-- An update shows up in the stream as a DELETE + INSERT pair; the INSERT side is the new image.
-- Each changed loan appears twice in the source:
--   merge_key = loan_id -> matches the current version and closes it
--   merge_key = NULL    -> never matches, so it inserts the new version
-- A new loan has no current version, so its single row falls through to INSERT.
-- "Changed" is decided from the stream itself (old image vs new image), not by joining the
-- history table, so the source never reads the table this statement is modifying.
MERGE INTO CORE.LOAN_ACCOUNT_HISTORY h
USING (
  SELECT c.loan_id AS merge_key, c.loan_id, c.customer_id, c.state, c.balance, c.apr, c.status,
         c.is_closed, c.hashdiff, c.event_ts
  FROM CORE.LOAN_ACCOUNTS_STREAM c
  WHERE c.METADATA$ACTION = 'INSERT'
  UNION ALL
  SELECT NULL AS merge_key, c.loan_id, c.customer_id, c.state, c.balance, c.apr, c.status,
         c.is_closed, c.hashdiff, c.event_ts
  FROM CORE.LOAN_ACCOUNTS_STREAM c
  JOIN CORE.LOAN_ACCOUNTS_STREAM old
    ON old.loan_id = c.loan_id AND old.METADATA$ACTION = 'DELETE' AND old.hashdiff <> c.hashdiff
  WHERE c.METADATA$ACTION = 'INSERT'
) s
ON h.loan_id = s.merge_key AND h.is_current
WHEN MATCHED AND h.hashdiff <> s.hashdiff THEN UPDATE SET
  valid_to   = s.event_ts,
  is_current = FALSE
WHEN NOT MATCHED THEN INSERT
  (loan_id, customer_id, state, balance, apr, status, is_closed, hashdiff, valid_from, valid_to, is_current)
VALUES
  (s.loan_id, s.customer_id, s.state, s.balance, s.apr, s.status, s.is_closed, s.hashdiff, s.event_ts, NULL, TRUE);
