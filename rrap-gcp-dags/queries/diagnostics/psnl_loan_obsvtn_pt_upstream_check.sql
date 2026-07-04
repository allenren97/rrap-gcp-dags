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

-- Step 4: qualifying accounts at observation points for process month 2026-01-31.
-- LGDD obs end  = LAST_DAY(process_date - 24 months) -> 2024-01-31
-- LGDND obs start = LAST_DAY(process_date - 12 months) -> 2025-01-31

SELECT 'lgdd_candidates' AS check_name, COUNT(*) AS row_count
FROM emulated.BASEL_PSNL_LOAN_ACCT_DRVD_VARS_2 d
INNER JOIN ingestion.TM_DIM t ON d.MTH_TM_ID = t.TM_ID
WHERE d.STREAM = 'NON_RESL'
  AND t.TM_LVL_END_DT = DATE '2024-01-31'
  AND UPPER(TRIM(d.PIT_STATUS_V2)) = 'DEF'
  AND d.TREATMNT_F = 'A'

UNION ALL

SELECT 'lgdnd_cur_at_obs_start', COUNT(*)
FROM emulated.BASEL_PSNL_LOAN_ACCT_DRVD_VARS_2 d
INNER JOIN ingestion.TM_DIM t ON d.MTH_TM_ID = t.TM_ID
WHERE d.STREAM = 'NON_RESL'
  AND t.TM_LVL_END_DT = DATE '2025-01-31'
  AND UPPER(TRIM(d.PIT_STATUS_V2)) = 'CUR'
  AND d.TREATMNT_F = 'A';
