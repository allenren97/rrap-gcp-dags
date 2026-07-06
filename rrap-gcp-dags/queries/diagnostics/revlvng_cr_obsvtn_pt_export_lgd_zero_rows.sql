-- Why REVLVNG_CR_OBSVTN_PT_DRVD_VAR.export_lgd exports 0 rows.
-- Mirrors export_dataprep_lgd + export_lgd CTEs in REVLVNG_CR_OBSVTN_PT_DRVD_VAR.py.
-- Edit process month below, then run each section.

-- process: 2026-01-31 | stream: NON_RESL
-- LGD window = 24–48 months before process month (OBSN_DTs ~2022-01-31 .. 2024-01-31)

-- ---------------------------------------------------------------------------
-- 0) Window bounds (sanity check mth_tm_id arithmetic)
-- ---------------------------------------------------------------------------
WITH process_tm AS (
    SELECT TM_ID, TM_LVL_END_DT
    FROM ingestion.TM_DIM
    WHERE TRIM(TM_LVL) = 'Month'
      AND TM_LVL_END_DT = DATE '2026-01-31'
)
SELECT
    p.TM_ID AS process_mth_tm_id,
    p.TM_LVL_END_DT AS process_dt,
    start_tm.TM_ID AS lgd_window_start_tm_id,
    start_tm.TM_LVL_END_DT AS lgd_window_start_dt,
    end_tm.TM_ID AS lgd_window_end_tm_id,
    end_tm.TM_LVL_END_DT AS lgd_window_end_dt
FROM process_tm p
INNER JOIN ingestion.TM_DIM end_tm
    ON end_tm.TM_ID = p.TM_ID - 24 * 40
   AND TRIM(end_tm.TM_LVL) = 'Month'
INNER JOIN ingestion.TM_DIM start_tm
    ON start_tm.TM_ID = p.TM_ID - 48 * 40
   AND TRIM(start_tm.TM_LVL) = 'Month';

-- ---------------------------------------------------------------------------
-- 1) Snapshot volume in LGD window (export_dataprep_lgd should be > 0)
-- ---------------------------------------------------------------------------
WITH process_tm AS (
    SELECT TM_ID FROM ingestion.TM_DIM
    WHERE TRIM(TM_LVL) = 'Month' AND TM_LVL_END_DT = DATE '2026-01-31'
)
SELECT
    COUNT(*) AS snapshot_rows,
    COUNT(DISTINCT snp.BASEL_ACCT_ID) AS distinct_accounts
FROM ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT snp
CROSS JOIN process_tm p
WHERE snp.MTH_TM_ID BETWEEN p.TM_ID - 48 * 40 AND p.TM_ID - 24 * 40;

-- ---------------------------------------------------------------------------
-- 2) Feature coverage by OBSN_DT in LGD window (most common root cause)
--    pit_cross / prd / heloc should be close to snapshot_rows per month
-- ---------------------------------------------------------------------------
WITH process_tm AS (
    SELECT TM_ID FROM ingestion.TM_DIM
    WHERE TRIM(TM_LVL) = 'Month' AND TM_LVL_END_DT = DATE '2026-01-31'
),
lgd_months AS (
    SELECT tm.TM_ID, tm.TM_LVL_END_DT
    FROM ingestion.TM_DIM tm
    CROSS JOIN process_tm p
    WHERE TRIM(tm.TM_LVL) = 'Month'
      AND tm.TM_ID BETWEEN p.TM_ID - 48 * 40 AND p.TM_ID - 24 * 40
)
SELECT
    m.TM_LVL_END_DT AS obsn_dt,
    COUNT(snp.BASEL_ACCT_ID) AS snapshot_rows,
    COUNT(pit.BASEL_ACCT_ID) AS pit_cross_rows,
    COUNT(prd.BASEL_ACCT_ID) AS basel_prd_rows,
    COUNT(heloc.BASEL_ACCT_ID) AS heloc_rows,
    COUNT(accrl.BASEL_ACCT_ID) AS accrl_stat_rows,
    COUNT(*) FILTER (WHERE pit.PIT_STATUS_CROSS_DEFAULT_ORIG IS NULL) AS null_pit_on_join,
    COUNT(*) FILTER (WHERE prd.BASEL_PRD_CD IS NULL) AS null_prd_on_join,
    COUNT(*) FILTER (WHERE heloc.HELOC_F IS NULL) AS null_heloc_on_join
FROM lgd_months m
LEFT JOIN ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT snp
    ON snp.MTH_TM_ID = m.TM_ID
LEFT JOIN features.PIT_STATUS_CROSS_DEFAULT_ORIG pit
    ON snp.BASEL_ACCT_ID = pit.BASEL_ACCT_ID
   AND pit.OBSN_DT = m.TM_LVL_END_DT
   AND pit.SRC_SYS_CD = 'KS'
LEFT JOIN features.BASEL_PRD_CD prd
    ON snp.BASEL_ACCT_ID = prd.BASEL_ACCT_ID
   AND prd.OBSN_DT = m.TM_LVL_END_DT
LEFT JOIN features.HELOC_F heloc
    ON snp.BASEL_ACCT_ID = heloc.BASEL_ACCT_ID
   AND heloc.OBSN_DT = m.TM_LVL_END_DT
LEFT JOIN features.ACCRL_STAT_F accrl
    ON snp.BASEL_ACCT_ID = accrl.BASEL_ACCT_ID
   AND accrl.OBSN_DT = m.TM_LVL_END_DT
GROUP BY 1
ORDER BY 1;

-- ---------------------------------------------------------------------------
-- 3) new_defaults count (export_lgd output size; should be > 0)
--    Reuses _NEW_DEFAULT_FLG from REVLVNG_CR_OBSVTN_PT_DRVD_VAR.py
-- ---------------------------------------------------------------------------
WITH
    process_tm AS (
        SELECT TM_ID FROM ingestion.TM_DIM
        WHERE TRIM(TM_LVL) = 'Month' AND TM_LVL_END_DT = DATE '2026-01-31'
    ),
    mth_tm_id AS (
        SELECT TM_ID AS val FROM process_tm
    ),
    dataprep AS (
        SELECT
            snp.BASEL_ACCT_ID,
            'KS' AS SRC_SYS_CD,
            snp.MTH_TM_ID,
            snp.TOT_NEW_BAL_AMT AS OS_BAL_AMT,
            snp.TOT_UNPAID_FNCL_CHRG_AMT,
            accrl.ACCRL_STAT_F,
            pit.PIT_STATUS_CROSS_DEFAULT_ORIG AS PIT_STATUS_CD,
            prd.BASEL_PRD_CD,
            CAST(NULL AS DOUBLE) AS TOT_CRNT_BAL_AMT,
            heloc.HELOC_F
        FROM ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT snp
        INNER JOIN ingestion.TM_DIM tm
            ON snp.MTH_TM_ID = tm.TM_ID
           AND TRIM(tm.TM_LVL) = 'Month'
        CROSS JOIN mth_tm_id
        LEFT JOIN features.BASEL_PRD_CD prd
            ON snp.BASEL_ACCT_ID = prd.BASEL_ACCT_ID
           AND prd.OBSN_DT = tm.TM_LVL_END_DT
        LEFT JOIN features.PIT_STATUS_CROSS_DEFAULT_ORIG pit
            ON snp.BASEL_ACCT_ID = pit.BASEL_ACCT_ID
           AND pit.OBSN_DT = tm.TM_LVL_END_DT
           AND pit.SRC_SYS_CD = 'KS'
        LEFT JOIN features.HELOC_F heloc
            ON snp.BASEL_ACCT_ID = heloc.BASEL_ACCT_ID
           AND heloc.OBSN_DT = tm.TM_LVL_END_DT
        LEFT JOIN features.ACCRL_STAT_F accrl
            ON snp.BASEL_ACCT_ID = accrl.BASEL_ACCT_ID
           AND accrl.OBSN_DT = tm.TM_LVL_END_DT
        WHERE snp.MTH_TM_ID >= mth_tm_id.val - 48 * 40
          AND snp.MTH_TM_ID <= mth_tm_id.val - 24 * 40
    ),
    lagged AS (
        SELECT
            *,
            LAG(OS_BAL_AMT) OVER (
                PARTITION BY BASEL_ACCT_ID ORDER BY MTH_TM_ID
            ) AS LAG_OS_BAL_AMT,
            LAG(TOT_UNPAID_FNCL_CHRG_AMT) OVER (
                PARTITION BY BASEL_ACCT_ID ORDER BY MTH_TM_ID
            ) AS LAG_TOT_UNPAID_FNCL_CHRG_AMT,
            LAG(PIT_STATUS_CD) OVER (
                PARTITION BY BASEL_ACCT_ID ORDER BY MTH_TM_ID
            ) AS LAG_PIT_STATUS_CD
        FROM dataprep
        CROSS JOIN mth_tm_id
        WHERE MTH_TM_ID >= mth_tm_id.val - 48 * 40
          AND MTH_TM_ID <= mth_tm_id.val - 24 * 40
    ),
    new_defaults AS (
        SELECT BASEL_ACCT_ID, MTH_TM_ID
        FROM lagged
        WHERE CASE
            WHEN SRC_SYS_CD IN ('SPL')
             AND PIT_STATUS_CD IN ('DEF', 'CHG')
             AND OS_BAL_AMT >= 1
             AND TOT_CRNT_BAL_AMT > 0
             AND COALESCE(LAG_PIT_STATUS_CD, 'CUR') NOT IN ('DEF', 'CHG')
            THEN 1
            WHEN SRC_SYS_CD IN ('TNG-MOR', 'MOR')
             AND PIT_STATUS_CD IN ('DEF')
             AND LAG_PIT_STATUS_CD = 'CUR'
            THEN 1
            WHEN SRC_SYS_CD IN ('KS')
             AND BASEL_PRD_CD = 'CC'
             AND HELOC_F = 'N'
             AND PIT_STATUS_CD IN ('DEF', 'CHG')
             AND LAG_PIT_STATUS_CD = 'CUR'
             AND OS_BAL_AMT > 0
             AND LAG_TOT_UNPAID_FNCL_CHRG_AMT <> LAG_OS_BAL_AMT
            THEN 1
            WHEN SRC_SYS_CD IN ('KS')
             AND BASEL_PRD_CD = 'CC'
             AND HELOC_F = 'N'
             AND PIT_STATUS_CD IN ('DEF', 'CHG')
             AND LAG_PIT_STATUS_CD = 'CUR'
             AND OS_BAL_AMT > 0
             AND LAG_TOT_UNPAID_FNCL_CHRG_AMT = LAG_OS_BAL_AMT
             AND LAG_TOT_UNPAID_FNCL_CHRG_AMT <= 0
            THEN 1
            WHEN SRC_SYS_CD IN ('KS')
             AND BASEL_PRD_CD = 'CC'
             AND HELOC_F = 'N'
             AND PIT_STATUS_CD IN ('DEF', 'CHG')
             AND LAG_PIT_STATUS_CD = 'CUR'
             AND OS_BAL_AMT > 0
             AND LAG_TOT_UNPAID_FNCL_CHRG_AMT = LAG_OS_BAL_AMT
             AND LAG_TOT_UNPAID_FNCL_CHRG_AMT > 0
             AND TOT_UNPAID_FNCL_CHRG_AMT <> OS_BAL_AMT
             AND PIT_STATUS_CD <> 'CHG'
             AND OS_BAL_AMT >= 5
            THEN 1
            WHEN SRC_SYS_CD IN ('KS')
             AND BASEL_PRD_CD = 'CC'
             AND HELOC_F = 'N'
             AND PIT_STATUS_CD IN ('DEF', 'CHG')
             AND LAG_PIT_STATUS_CD = 'CUR'
            THEN 0
            WHEN SRC_SYS_CD IN ('KS')
             AND BASEL_PRD_CD = 'CC'
             AND HELOC_F = 'Y'
             AND PIT_STATUS_CD IN ('DEF', 'CHG')
             AND LAG_PIT_STATUS_CD = 'CUR'
             AND LAG_TOT_UNPAID_FNCL_CHRG_AMT <> LAG_OS_BAL_AMT
             AND OS_BAL_AMT > 0
            THEN 1
            WHEN SRC_SYS_CD IN ('KS')
             AND BASEL_PRD_CD <> 'CC'
             AND PIT_STATUS_CD IN ('DEF', 'CHG')
             AND LAG_PIT_STATUS_CD = 'CUR'
             AND LAG_TOT_UNPAID_FNCL_CHRG_AMT <> LAG_OS_BAL_AMT
             AND OS_BAL_AMT > 0
            THEN 1
            ELSE 0
        END = 1
    )
SELECT
    COUNT(*) AS new_default_events,
    COUNT(DISTINCT BASEL_ACCT_ID) AS distinct_default_accounts
FROM new_defaults;
