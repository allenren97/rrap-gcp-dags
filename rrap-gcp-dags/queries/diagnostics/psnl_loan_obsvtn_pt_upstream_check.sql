-- Upstream row-count check before running PSNL_LOAN_OBSVTN_PT_DRVD_VAR.
-- Replace OBSN_DT and STREAM for the process month under test.

SELECT 'drvd_vars' AS tbl, COUNT(*) AS row_count
FROM emulated.BASEL_PSNL_LOAN_ACCT_DRVD_VARS
WHERE OBSN_DT = DATE '2026-01-31'
  AND STREAM = 'NON_RESL'

UNION ALL

SELECT 'drvd_vars_2', COUNT(*)
FROM emulated.BASEL_PSNL_LOAN_ACCT_DRVD_VARS_2
WHERE OBSN_DT = DATE '2026-01-31'
  AND STREAM = 'NON_RESL'

UNION ALL

SELECT 'spl_snapshot', COUNT(*)
FROM ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT
WHERE MTH_TM_ID = (
    SELECT TM_ID
    FROM ingestion.TM_DIM
    WHERE TM_LVL = 'Month'
      AND TM_LVL_END_DT = DATE '2026-01-31'
);
