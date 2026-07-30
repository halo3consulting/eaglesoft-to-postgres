-- Replace the CDC-synced "transactions" table with a view over
-- transactions_header + transactions_detail.
--
-- Why: PPM.transactions in Eaglesoft is itself a view over these two
-- tables, and its rw_transactions CDC feed emits only UPDATE events —
-- zero inserts/deletes — so the synced copy silently loses every new
-- transaction that never receives a later update (~5,900 rows missing
-- as of 2026-07-26, growing daily since the 2026-05-03 full rebuild).
-- Replicating the view in Postgres makes transactions exactly as fresh
-- as transactions_header/transactions_detail and removes a redundant
-- 416 MB table.
--
-- Deploy order:
--   1. Commit the sync_config.yaml change removing the transactions
--      entry and redeploy the chart, so no sync job writes to
--      public.transactions anymore.
--   2. Run this script against the reporting database.
--
-- Translation notes vs the SQL Anywhere original:
--   * SELECT FIRST ... ORDER BY 1  ->  ORDER BY 1 NULLS FIRST LIMIT 1
--     (SQL Anywhere sorts NULLs first ascending; Postgres defaults to
--     NULLS LAST, so the explicit NULLS FIRST preserves semantics).
--   * NULL AS old_tooth is cast to smallint to keep the column type
--     the old synced table exposed.
-- Verified 2026-07-26 against the live database: all 5 detail-derived
-- columns matched the existing table for every overlapping row in the
-- last 30 days (0 mismatches across 2,076 rows).

BEGIN;

DROP TABLE IF EXISTS public.transactions;

CREATE VIEW public.transactions AS
SELECT
    h.tran_num,
    h.user_id,
    h.type,
    h.tran_date,
    (SELECT d.patient_id
       FROM public.transactions_detail d
      WHERE d.tran_num = h.tran_num
      ORDER BY 1 NULLS FIRST LIMIT 1) AS patient_id,
    h.resp_party_id,
    h.amount,
    h.service_code,
    h.paytype_id,
    h.sequence,
    (SELECT d.provider_id
       FROM public.transactions_detail d
      WHERE d.tran_num = h.tran_num
      ORDER BY 1 NULLS FIRST LIMIT 1) AS provider_id,
    (SELECT d.collections_go_to
       FROM public.transactions_detail d
      WHERE d.tran_num = h.tran_num
      ORDER BY 1 NULLS FIRST LIMIT 1) AS collections_go_to,
    h.statement_num,
    NULL::smallint AS old_tooth,
    h.surface,
    h.fee,
    h.discount_surcharge,
    h.tax,
    h.description,
    h.defective,
    h.impacts,
    h.status,
    h.adjustment_type,
    h.claim_id,
    h.est_primary,
    h.est_secondary,
    h.paid_primary,
    h.paid_secondary,
    (SELECT d.provider_practice_id
       FROM public.transactions_detail d
      WHERE d.tran_num = h.tran_num
      ORDER BY 1 NULLS FIRST LIMIT 1) AS provider_practice_id,
    (SELECT d.patient_practice_id
       FROM public.transactions_detail d
      WHERE d.tran_num = h.tran_num
      ORDER BY 1 NULLS FIRST LIMIT 1) AS patient_practice_id,
    h.bulk_payment_num,
    h.aging_date,
    h.tooth,
    h.lab_fee,
    h.lab_fee2,
    h.lab_code,
    h.lab_code2,
    h.pre_fee,
    h.standard_fee_id,
    h.practice_id,
    h.procedure_type_codes,
    h.balance
FROM public.transactions_header h;

COMMIT;

-- Optional cleanup: the CDC bookkeeping row for the old table.
-- DELETE FROM sync_metadata.sync_state WHERE table_name = 'transactions';
