-- Verify features.PIT_STATUS_CROSS_DEFAULT_ORIG for SPL (personal loan).
-- Edit process month / stream below.
--
-- Chain: PIT_STATUS_ACCOUNT_ORIG -> PIT_STATUS_CROSS_DEFAULT_ORIG -> 2104 PIT_STATUS_V2
--
-- For PSNL_LOAN_OBSVTN_PT_DRVD_VAR process 2026-01-31 you need SPL feature rows for:
--   LGDD window OBSN_DTs: 2022-01-31 .. 2024-01-31
--   LGDND window OBSN_DTs: 2025-01-31 .. 2026-01-31

-- ---------------------------------------------------------------------------
-- 1) Monthly coverage: one partition per month-end, non-zero row count
-- ---------------------------------------------------------------------------
SELECT
    t.TM_LVL_END_DT AS obsn_dt,
    COUNT(pit.BASEL_ACCT_ID) AS pit_cross_default_rows,
    COUNT(orig.BASEL_ACCT_ID) AS pit_account_orig_rows,
    COUNT(snp.BASEL_ACCT_ID) AS snapshot_eligible_rows
FROM ingestion.TM_DIM t
LEFT JOIN features.PIT_STATUS_CROSS_DEFAULT_ORIG pit
    ON pit.OBSN_DT = t.TM_LVL_END_DT
   AND pit.SRC_SYS_CD = 'SPL'
LEFT JOIN features.PIT_STATUS_ACCOUNT_ORIG orig
    ON orig.OBSN_DT = t.TM_LVL_END_DT
   AND orig.SRC_SYS_CD = 'SPL'
LEFT JOIN ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT snp
    ON snp.MTH_TM_ID = t.TM_ID
   AND TRIM(snp.RECD_STAT_CD) IN ('4', '5', '6', '7', '8')
WHERE TRIM(t.TM_LVL) = 'Month'
  AND t.TM_LVL_END_DT BETWEEN DATE '2022-01-31' AND DATE '2026-01-31'
GROUP BY 1
ORDER BY 1;

-- ---------------------------------------------------------------------------
-- 2) NULL / blank PIT on feature (should be ~0 for eligible snapshot accounts)
-- ---------------------------------------------------------------------------
SELECT
    pit.OBSN_DT,
    COUNT(*) AS total_rows,
    COUNT(*) FILTER (
        WHERE pit.PIT_STATUS_CROSS_DEFAULT_ORIG IS NULL
           OR TRIM(pit.PIT_STATUS_CROSS_DEFAULT_ORIG) = ''
    ) AS null_or_blank_pit,
    COUNT(*) FILTER (WHERE TRIM(pit.PIT_STATUS_CROSS_DEFAULT_ORIG) = 'CUR') AS pit_cur,
    COUNT(*) FILTER (WHERE TRIM(pit.PIT_STATUS_CROSS_DEFAULT_ORIG) = 'DEF') AS pit_def,
    COUNT(*) FILTER (WHERE TRIM(pit.PIT_STATUS_CROSS_DEFAULT_ORIG) = 'CHG') AS pit_chg
FROM features.PIT_STATUS_CROSS_DEFAULT_ORIG pit
WHERE pit.SRC_SYS_CD = 'SPL'
  AND pit.OBSN_DT BETWEEN DATE '2022-01-31' AND DATE '2026-01-31'
GROUP BY 1
ORDER BY 1;

-- ---------------------------------------------------------------------------
-- 3) Snapshot accounts missing from feature (join gap -> NULL PIT in 2104)
-- ---------------------------------------------------------------------------
SELECT
    t.TM_LVL_END_DT AS obsn_dt,
    COUNT(snp.BASEL_ACCT_ID) AS snapshot_accounts,
    COUNT(pit.BASEL_ACCT_ID) AS matched_in_feature,
    COUNT(snp.BASEL_ACCT_ID) - COUNT(pit.BASEL_ACCT_ID) AS missing_from_feature
FROM ingestion.TM_DIM t
INNER JOIN ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT snp
    ON snp.MTH_TM_ID = t.TM_ID
   AND TRIM(snp.RECD_STAT_CD) IN ('4', '5', '6', '7', '8')
LEFT JOIN features.PIT_STATUS_CROSS_DEFAULT_ORIG pit
    ON pit.BASEL_ACCT_ID = snp.BASEL_ACCT_ID
   AND pit.OBSN_DT = t.TM_LVL_END_DT
   AND pit.SRC_SYS_CD = 'SPL'
WHERE TRIM(t.TM_LVL) = 'Month'
  AND t.TM_LVL_END_DT BETWEEN DATE '2022-01-31' AND DATE '2026-01-31'
GROUP BY 1
ORDER BY 1;

-- ---------------------------------------------------------------------------
-- 4) Parity vs ingestion.PIT_STATUS_PRE_STEP (if you still have PRE_STEP loaded)
-- ---------------------------------------------------------------------------
SELECT
    t.TM_LVL_END_DT AS obsn_dt,
    COUNT(*) AS joined_rows,
    COUNT(*) FILTER (
        WHERE TRIM(pit.PIT_STATUS_CROSS_DEFAULT_ORIG)
           <> TRIM(pre.CROSS_DFLT_PIT_STATUS)
    ) AS pit_mismatch,
    COUNT(*) FILTER (
        WHERE pre.CROSS_DFLT_PIT_STATUS IS NULL
    ) AS missing_pre_step
FROM ingestion.TM_DIM t
INNER JOIN ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT snp
    ON snp.MTH_TM_ID = t.TM_ID
   AND TRIM(snp.RECD_STAT_CD) IN ('4', '5', '6', '7', '8')
INNER JOIN features.PIT_STATUS_CROSS_DEFAULT_ORIG pit
    ON pit.BASEL_ACCT_ID = snp.BASEL_ACCT_ID
   AND pit.OBSN_DT = t.TM_LVL_END_DT
   AND pit.SRC_SYS_CD = 'SPL'
LEFT JOIN ingestion.PIT_STATUS_PRE_STEP pre
    ON pre.BASEL_ACCT_ID = snp.BASEL_ACCT_ID
   AND pre.MTH_TM_ID = t.TM_ID
   AND pre.SRC_SYS_CD = 'SPL'
WHERE TRIM(t.TM_LVL) = 'Month'
  AND t.TM_LVL_END_DT = DATE '2024-01-31'   -- edit anchor month to spot-check
GROUP BY 1;

-- ---------------------------------------------------------------------------
-- 5) After re-running 2104: feature should match emulated PIT_STATUS_V2
-- ---------------------------------------------------------------------------
SELECT
    t.TM_LVL_END_DT AS obsn_dt,
    COUNT(*) AS joined_rows,
    COUNT(*) FILTER (
        WHERE TRIM(d.PIT_STATUS_V2) <> TRIM(pit.PIT_STATUS_CROSS_DEFAULT_ORIG)
    ) AS drvd_vs_feature_mismatch,
    COUNT(*) FILTER (WHERE d.PIT_STATUS_V2 IS NULL) AS null_in_2104
FROM ingestion.TM_DIM t
INNER JOIN emulated.BASEL_PSNL_LOAN_ACCT_DRVD_VARS_2 d
    ON d.MTH_TM_ID = t.TM_ID
   AND d.STREAM = 'NON_RESL'
INNER JOIN features.PIT_STATUS_CROSS_DEFAULT_ORIG pit
    ON pit.BASEL_ACCT_ID = d.BASEL_ACCT_ID
   AND pit.OBSN_DT = t.TM_LVL_END_DT
   AND pit.SRC_SYS_CD = 'SPL'
WHERE TRIM(t.TM_LVL) = 'Month'
  AND t.TM_LVL_END_DT BETWEEN DATE '2022-01-31' AND DATE '2026-01-31'
GROUP BY 1
ORDER BY 1;

-- ---------------------------------------------------------------------------
-- 6) 2201 anchor sanity (same checks as obs-point job needs)
-- ---------------------------------------------------------------------------
SELECT
    t.TM_LVL_END_DT,
    COUNT(*) FILTER (WHERE UPPER(TRIM(pit.PIT_STATUS_CROSS_DEFAULT_ORIG)) = 'CUR') AS feature_cur,
    COUNT(*) FILTER (WHERE UPPER(TRIM(pit.PIT_STATUS_CROSS_DEFAULT_ORIG)) = 'DEF') AS feature_def,
    COUNT(*) FILTER (WHERE UPPER(TRIM(pit.PIT_STATUS_CROSS_DEFAULT_ORIG)) = 'CHG') AS feature_chg,
    COUNT(*) FILTER (WHERE pit.PIT_STATUS_CROSS_DEFAULT_ORIG IS NULL) AS feature_null
FROM features.PIT_STATUS_CROSS_DEFAULT_ORIG pit
JOIN ingestion.TM_DIM t ON pit.OBSN_DT = t.TM_LVL_END_DT AND TRIM(t.TM_LVL) = 'Month'
WHERE pit.SRC_SYS_CD = 'SPL'
  AND t.TM_LVL_END_DT IN (DATE '2024-01-31', DATE '2025-01-31')
GROUP BY 1
ORDER BY 1;
