-- Why PSNL_LOAN_OBSVTN_PT_DRVD_VAR inserts 0 rows.
-- Mirrors PSNL_LOAN_OBSVTN_PT_DRVD_VAR.duckdb_load CTEs exactly.
-- Edit process date and stream below, then run all steps.

-- process: 2026-01-31 | stream: NON_RESL

WITH
    process_tm AS (
        SELECT TM_ID, TM_LVL_END_DT
        FROM ingestion.TM_DIM
        WHERE TRIM(TM_LVL) = 'Month'
          AND TM_LVL_END_DT = DATE '2026-01-31'
    ),
    lgdd_obs AS (
        SELECT
            p.TM_ID AS PROCESS_MTH_TM_ID,
            p.TM_LVL_END_DT AS PROCESS_DT,
            LAST_DAY((p.TM_LVL_END_DT - INTERVAL 24 MONTH)::DATE) AS OBS_END_DT,
            LAST_DAY((p.TM_LVL_END_DT - INTERVAL 48 MONTH)::DATE) AS OBS_START_DT
        FROM process_tm p
    ),
    lgdd_obs_tm AS (
        SELECT
            o.PROCESS_MTH_TM_ID,
            o.PROCESS_DT,
            end_tm.TM_ID AS OBS_END_TM_ID,
            end_tm.TM_LVL_END_DT AS OBS_END_DT,
            start_tm.TM_ID AS OBS_START_TM_ID,
            start_tm.TM_LVL_END_DT AS OBS_START_DT
        FROM lgdd_obs o
        INNER JOIN ingestion.TM_DIM end_tm
            ON TRIM(end_tm.TM_LVL) = 'Month'
           AND end_tm.TM_LVL_END_DT = o.OBS_END_DT
        INNER JOIN ingestion.TM_DIM start_tm
            ON TRIM(start_tm.TM_LVL) = 'Month'
           AND start_tm.TM_LVL_END_DT = o.OBS_START_DT
    ),
    lgdnd_obs AS (
        SELECT
            p.TM_ID AS PROCESS_MTH_TM_ID,
            p.TM_LVL_END_DT AS PROCESS_DT,
            LAST_DAY(p.TM_LVL_END_DT::DATE) AS OBS_END_DT,
            LAST_DAY((p.TM_LVL_END_DT - INTERVAL 12 MONTH)::DATE) AS OBS_START_DT
        FROM process_tm p
    ),
    lgdnd_obs_tm AS (
        SELECT
            o.PROCESS_MTH_TM_ID,
            o.PROCESS_DT,
            end_tm.TM_ID AS OBS_END_TM_ID,
            end_tm.TM_LVL_END_DT AS OBS_END_DT,
            start_tm.TM_ID AS OBS_START_TM_ID,
            start_tm.TM_LVL_END_DT AS OBS_START_DT
        FROM lgdnd_obs o
        INNER JOIN ingestion.TM_DIM end_tm
            ON TRIM(end_tm.TM_LVL) = 'Month'
           AND end_tm.TM_LVL_END_DT = o.OBS_END_DT
        INNER JOIN ingestion.TM_DIM start_tm
            ON TRIM(start_tm.TM_LVL) = 'Month'
           AND start_tm.TM_LVL_END_DT = o.OBS_START_DT
    ),
    max_non_def AS (
        SELECT
            d.BASEL_ACCT_ID,
            MAX(d.MTH_TM_ID) AS MAX_NON_DEF_TM_ID,
            MAX(d.PROCESS_DT) AS MAX_NON_DEF_DT
        FROM emulated.BASEL_PSNL_LOAN_ACCT_DRVD_VARS_2 d
        CROSS JOIN lgdd_obs_tm o
        WHERE d.STREAM = 'NON_RESL'
          AND UPPER(TRIM(d.PIT_STATUS_V2)) = 'CUR'
          AND d.MTH_TM_ID BETWEEN o.OBS_START_TM_ID AND o.OBS_END_TM_ID
        GROUP BY d.BASEL_ACCT_ID
    ),
    def_after_cur AS (
        SELECT
            d.BASEL_ACCT_ID,
            MIN(d.MTH_TM_ID) AS LAST_NEW_DFT_MTH_TM_ID,
            MIN(d.PROCESS_DT) AS LAST_NEW_DFT_DT
        FROM emulated.BASEL_PSNL_LOAN_ACCT_DRVD_VARS_2 d
        INNER JOIN max_non_def m ON d.BASEL_ACCT_ID = m.BASEL_ACCT_ID
        CROSS JOIN lgdd_obs_tm o
        WHERE d.STREAM = 'NON_RESL'
          AND UPPER(TRIM(d.PIT_STATUS_V2)) = 'DEF'
          AND d.MTH_TM_ID > m.MAX_NON_DEF_TM_ID
          AND d.MTH_TM_ID <= o.OBS_END_TM_ID
        GROUP BY d.BASEL_ACCT_ID
    ),
    pop_in_window AS (
        SELECT DISTINCT d.BASEL_ACCT_ID
        FROM emulated.BASEL_PSNL_LOAN_ACCT_DRVD_VARS_2 d
        CROSS JOIN lgdd_obs_tm o
        WHERE d.STREAM = 'NON_RESL'
          AND d.MTH_TM_ID BETWEEN o.OBS_START_TM_ID AND o.OBS_END_TM_ID
    ),
    never_cur AS (
        SELECT p.BASEL_ACCT_ID
        FROM pop_in_window p
        WHERE p.BASEL_ACCT_ID NOT IN (SELECT BASEL_ACCT_ID FROM max_non_def)
    ),
    def_never_cur AS (
        SELECT
            d.BASEL_ACCT_ID,
            MIN(d.MTH_TM_ID) AS LAST_NEW_DFT_MTH_TM_ID,
            MIN(d.PROCESS_DT) AS LAST_NEW_DFT_DT
        FROM emulated.BASEL_PSNL_LOAN_ACCT_DRVD_VARS_2 d
        INNER JOIN never_cur n ON d.BASEL_ACCT_ID = n.BASEL_ACCT_ID
        CROSS JOIN lgdd_obs_tm o
        WHERE d.STREAM = 'NON_RESL'
          AND UPPER(TRIM(d.PIT_STATUS_V2)) = 'DEF'
          AND d.MTH_TM_ID BETWEEN o.OBS_START_TM_ID AND o.OBS_END_TM_ID
        GROUP BY d.BASEL_ACCT_ID
    ),
    last_new_def AS (
        SELECT * FROM def_after_cur
        UNION ALL
        SELECT * FROM def_never_cur
    ),
    os_bal_at_def AS (
        SELECT l.BASEL_ACCT_ID
        FROM last_new_def l
        INNER JOIN ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT s
            ON l.BASEL_ACCT_ID = s.BASEL_ACCT_ID
           AND l.LAST_NEW_DFT_MTH_TM_ID = s.MTH_TM_ID
    ),
    lgdd_anchor AS (
        SELECT c.BASEL_ACCT_ID
        FROM lgdd_obs_tm o
        INNER JOIN emulated.BASEL_PSNL_LOAN_ACCT_DRVD_VARS_2 c
            ON c.MTH_TM_ID = o.OBS_END_TM_ID
           AND c.STREAM = 'NON_RESL'
           AND UPPER(TRIM(c.PIT_STATUS_V2)) = 'DEF'
           AND c.TREATMNT_F = 'A'
        INNER JOIN emulated.BASEL_PSNL_LOAN_ACCT_DRVD_VARS b
            ON b.BASEL_ACCT_ID = c.BASEL_ACCT_ID
           AND b.MTH_TM_ID = c.MTH_TM_ID
           AND b.STREAM = 'NON_RESL'
    ),
    lgdd_rows AS (
        SELECT a.BASEL_ACCT_ID
        FROM lgdd_obs_tm o
        INNER JOIN ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT a
            ON a.MTH_TM_ID = o.OBS_END_TM_ID
        INNER JOIN emulated.BASEL_PSNL_LOAN_ACCT_DRVD_VARS b
            ON a.BASEL_ACCT_ID = b.BASEL_ACCT_ID
           AND a.MTH_TM_ID = b.MTH_TM_ID
           AND b.STREAM = 'NON_RESL'
        INNER JOIN emulated.BASEL_PSNL_LOAN_ACCT_DRVD_VARS_2 c
            ON a.BASEL_ACCT_ID = c.BASEL_ACCT_ID
           AND a.MTH_TM_ID = c.MTH_TM_ID
           AND c.STREAM = 'NON_RESL'
           AND UPPER(TRIM(c.PIT_STATUS_V2)) = 'DEF'
           AND c.TREATMNT_F = 'A'
        INNER JOIN os_bal_at_def pf ON a.BASEL_ACCT_ID = pf.BASEL_ACCT_ID
    ),
    filter_account_raw AS (
        SELECT
            b.BASEL_ACCT_ID,
            b.MTH_TM_ID,
            ROUND(a.TOT_CRNT_BAL_AMT + a.ADD_ON_BAL_AMT + a.ACCR_INTR, 3) AS OS_BAL_AMT_V2,
            a.TOT_CRNT_BAL_AMT,
            ROW_NUMBER() OVER (
                PARTITION BY b.BASEL_ACCT_ID
                ORDER BY b.MTH_TM_ID
            ) AS RN
        FROM ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT a
        INNER JOIN emulated.BASEL_PSNL_LOAN_ACCT_DRVD_VARS_2 b
            ON a.MTH_TM_ID = b.MTH_TM_ID
           AND a.BASEL_ACCT_ID = b.BASEL_ACCT_ID
           AND b.STREAM = 'NON_RESL'
        CROSS JOIN lgdnd_obs_tm o
        WHERE b.MTH_TM_ID BETWEEN o.OBS_START_TM_ID AND o.OBS_END_TM_ID
          AND UPPER(TRIM(b.PIT_STATUS_V2)) IN ('DEF', 'CHG')
    ),
    filter_account AS (
        SELECT BASEL_ACCT_ID
        FROM filter_account_raw
        WHERE RN = 1
          AND OS_BAL_AMT_V2 >= 1
          AND TOT_CRNT_BAL_AMT > 0
    ),
    lgdnd_rows AS (
        SELECT a.BASEL_ACCT_ID
        FROM lgdnd_obs_tm o
        INNER JOIN ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT a
            ON a.MTH_TM_ID = o.OBS_START_TM_ID
        INNER JOIN emulated.BASEL_PSNL_LOAN_ACCT_DRVD_VARS b
            ON a.BASEL_ACCT_ID = b.BASEL_ACCT_ID
           AND a.MTH_TM_ID = b.MTH_TM_ID
           AND b.STREAM = 'NON_RESL'
        INNER JOIN emulated.BASEL_PSNL_LOAN_ACCT_DRVD_VARS_2 c
            ON a.BASEL_ACCT_ID = c.BASEL_ACCT_ID
           AND a.MTH_TM_ID = c.MTH_TM_ID
           AND c.STREAM = 'NON_RESL'
           AND UPPER(TRIM(c.PIT_STATUS_V2)) = 'CUR'
           AND c.TREATMNT_F = 'A'
        INNER JOIN filter_account fa ON a.BASEL_ACCT_ID = fa.BASEL_ACCT_ID
    ),
    combined AS (
        SELECT BASEL_ACCT_ID FROM lgdd_rows
        UNION ALL
        SELECT BASEL_ACCT_ID FROM lgdnd_rows
    ),
    pit_null AS (
        SELECT COUNT(*) AS n
        FROM emulated.BASEL_PSNL_LOAN_ACCT_DRVD_VARS_2 d
        CROSS JOIN lgdd_obs_tm o
        WHERE d.STREAM = 'NON_RESL'
          AND d.MTH_TM_ID BETWEEN o.OBS_START_TM_ID AND o.OBS_END_TM_ID
          AND d.PIT_STATUS_V2 IS NULL
    ),
    obs_windows AS (
        SELECT
            'LGDD window' AS path,
            OBS_START_DT AS window_start,
            OBS_END_DT AS window_end,
            OBS_END_DT AS anchor_dt
        FROM lgdd_obs_tm
        UNION ALL
        SELECT
            'LGDND window',
            OBS_START_DT,
            OBS_END_DT,
            OBS_START_DT
        FROM lgdnd_obs_tm
    )
SELECT step, cnt
FROM (
    SELECT 0 AS ord, '0_obs_windows (sanity)' AS step,
           (SELECT COUNT(*) FROM obs_windows) AS cnt

    UNION ALL SELECT 1, '1_pop_in_window (2104 rows in LGDD window)',
           (SELECT COUNT(*) FROM pop_in_window)

    UNION ALL SELECT 2, '2_max_non_def (had CUR in LGDD window)',
           (SELECT COUNT(*) FROM max_non_def)

    UNION ALL SELECT 3, '3_def_after_cur (CUR then DEF transition)',
           (SELECT COUNT(*) FROM def_after_cur)

    UNION ALL SELECT 4, '4_def_never_cur (never CUR, had DEF)',
           (SELECT COUNT(*) FROM def_never_cur)

    UNION ALL SELECT 5, '5_last_new_def (union of 3+4)',
           (SELECT COUNT(*) FROM last_new_def)

    UNION ALL SELECT 6, '6_os_bal_at_def (snapshot at default month)',
           (SELECT COUNT(*) FROM os_bal_at_def)

    UNION ALL SELECT 7, '7_lgdd_anchor (DEF+A at obs end + 2105)',
           (SELECT COUNT(*) FROM lgdd_anchor)

    UNION ALL SELECT 8, '8_lgdd_rows (full LGDD path)',
           (SELECT COUNT(*) FROM lgdd_rows)

    UNION ALL SELECT 9, '9_filter_account (LGDND prior default)',
           (SELECT COUNT(*) FROM filter_account)

    UNION ALL SELECT 10, '10_lgdnd_rows (full LGDND path)',
           (SELECT COUNT(*) FROM lgdnd_rows)

    UNION ALL SELECT 11, '11_combined (expected insert count)',
           (SELECT COUNT(*) FROM combined)

    UNION ALL SELECT 12, '12_null_pit_in_lgdd_window (should be 0)',
           (SELECT n FROM pit_null)
) x
ORDER BY ord;

-- Window dates for process 2026-01-31:
--   LGDD:  2022-01-31 -> 2024-01-31, anchor obs end   2024-01-31
--   LGDND: 2025-01-31 -> 2026-01-31, anchor obs start 2025-01-31
