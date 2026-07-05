-- Why PSNL_LOAN_OBSVTN_PT_DRVD_VAR inserts 0 rows (process 2026-01-31, NON_RESL).
-- Each step mirrors a CTE in PSNL_LOAN_OBSVTN_PT_DRVD_VAR.duckdb_load.
-- First step that returns 0 is where the pipeline breaks.

WITH
    process_tm AS (
        SELECT TM_ID, TM_LVL_END_DT
        FROM ingestion.TM_DIM
        WHERE TM_LVL = 'Month' AND TM_LVL_END_DT = DATE '2026-01-31'
    ),
    lgdd_obs_tm AS (
        SELECT
            p.TM_ID AS PROCESS_MTH_TM_ID,
            end_tm.TM_ID AS OBS_END_TM_ID,
            start_tm.TM_ID AS OBS_START_TM_ID
        FROM process_tm p
        INNER JOIN ingestion.TM_DIM end_tm
            ON end_tm.TM_LVL = 'Month'
           AND end_tm.TM_LVL_END_DT = LAST_DAY((p.TM_LVL_END_DT - INTERVAL 24 MONTH)::DATE)
        INNER JOIN ingestion.TM_DIM start_tm
            ON start_tm.TM_LVL = 'Month'
           AND start_tm.TM_LVL_END_DT = LAST_DAY((p.TM_LVL_END_DT - INTERVAL 48 MONTH)::DATE)
    ),
    lgdnd_obs_tm AS (
        SELECT
            end_tm.TM_ID AS OBS_END_TM_ID,
            start_tm.TM_ID AS OBS_START_TM_ID
        FROM process_tm p
        INNER JOIN ingestion.TM_DIM end_tm
            ON end_tm.TM_LVL = 'Month'
           AND end_tm.TM_LVL_END_DT = p.TM_LVL_END_DT
        INNER JOIN ingestion.TM_DIM start_tm
            ON start_tm.TM_LVL = 'Month'
           AND start_tm.TM_LVL_END_DT = LAST_DAY((p.TM_LVL_END_DT - INTERVAL 12 MONTH)::DATE)
    ),
    max_non_def AS (
        SELECT d.BASEL_ACCT_ID, MAX(d.MTH_TM_ID) AS MAX_NON_DEF_TM_ID
        FROM emulated.BASEL_PSNL_LOAN_ACCT_DRVD_VARS_2 d
        CROSS JOIN lgdd_obs_tm o
        WHERE d.STREAM = 'NON_RESL'
          AND UPPER(TRIM(d.PIT_STATUS_V2)) = 'CUR'
          AND d.MTH_TM_ID BETWEEN o.OBS_START_TM_ID AND o.OBS_END_TM_ID
        GROUP BY d.BASEL_ACCT_ID
    ),
    def_after_cur AS (
        SELECT d.BASEL_ACCT_ID
        FROM emulated.BASEL_PSNL_LOAN_ACCT_DRVD_VARS_2 d
        INNER JOIN max_non_def m ON d.BASEL_ACCT_ID = m.BASEL_ACCT_ID
        CROSS JOIN lgdd_obs_tm o
        WHERE d.STREAM = 'NON_RESL'
          AND UPPER(TRIM(d.PIT_STATUS_V2)) = 'DEF'
          AND d.MTH_TM_ID > m.MAX_NON_DEF_TM_ID
          AND d.MTH_TM_ID <= o.OBS_END_TM_ID
    ),
    pop_in_window AS (
        SELECT DISTINCT d.BASEL_ACCT_ID
        FROM emulated.BASEL_PSNL_LOAN_ACCT_DRVD_VARS_2 d
        CROSS JOIN lgdd_obs_tm o
        WHERE d.STREAM = 'NON_RESL'
          AND d.MTH_TM_ID BETWEEN o.OBS_START_TM_ID AND o.OBS_END_TM_ID
    ),
    def_never_cur AS (
        SELECT d.BASEL_ACCT_ID
        FROM emulated.BASEL_PSNL_LOAN_ACCT_DRVD_VARS_2 d
        CROSS JOIN lgdd_obs_tm o
        WHERE d.STREAM = 'NON_RESL'
          AND d.BASEL_ACCT_ID IN (
              SELECT BASEL_ACCT_ID FROM pop_in_window
              EXCEPT SELECT BASEL_ACCT_ID FROM max_non_def
          )
          AND UPPER(TRIM(d.PIT_STATUS_V2)) = 'DEF'
          AND d.MTH_TM_ID BETWEEN o.OBS_START_TM_ID AND o.OBS_END_TM_ID
    ),
    last_new_def AS (
        SELECT BASEL_ACCT_ID FROM def_after_cur
        UNION
        SELECT BASEL_ACCT_ID FROM def_never_cur
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
    lgdd_final AS (
        SELECT a.BASEL_ACCT_ID
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
        INNER JOIN last_new_def l ON c.BASEL_ACCT_ID = l.BASEL_ACCT_ID
    ),
    filter_account_raw AS (
        SELECT
            b.BASEL_ACCT_ID,
            b.MTH_TM_ID,
            ROUND(a.TOT_CRNT_BAL_AMT + a.ADD_ON_BAL_AMT + a.ACCR_INTR, 3) AS OS_BAL_AMT_V2,
            a.TOT_CRNT_BAL_AMT,
            ROW_NUMBER() OVER (PARTITION BY b.BASEL_ACCT_ID ORDER BY b.MTH_TM_ID) AS RN
        FROM lgdnd_obs_tm o
        INNER JOIN emulated.BASEL_PSNL_LOAN_ACCT_DRVD_VARS_2 b
            ON b.STREAM = 'NON_RESL'
           AND b.MTH_TM_ID BETWEEN o.OBS_START_TM_ID AND o.OBS_END_TM_ID
           AND UPPER(TRIM(b.PIT_STATUS_V2)) IN ('DEF', 'CHG')
        INNER JOIN ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT a
            ON a.MTH_TM_ID = b.MTH_TM_ID
           AND a.BASEL_ACCT_ID = b.BASEL_ACCT_ID
    ),
    filter_account AS (
        SELECT BASEL_ACCT_ID
        FROM filter_account_raw
        WHERE RN = 1
          AND OS_BAL_AMT_V2 >= 1
          AND TOT_CRNT_BAL_AMT > 0
    ),
    lgdnd_final AS (
        SELECT c.BASEL_ACCT_ID
        FROM lgdnd_obs_tm o
        INNER JOIN emulated.BASEL_PSNL_LOAN_ACCT_DRVD_VARS_2 c
            ON c.MTH_TM_ID = o.OBS_START_TM_ID
           AND c.STREAM = 'NON_RESL'
           AND UPPER(TRIM(c.PIT_STATUS_V2)) = 'CUR'
           AND c.TREATMNT_F = 'A'
        INNER JOIN emulated.BASEL_PSNL_LOAN_ACCT_DRVD_VARS b
            ON b.BASEL_ACCT_ID = c.BASEL_ACCT_ID
           AND b.MTH_TM_ID = c.MTH_TM_ID
           AND b.STREAM = 'NON_RESL'
        INNER JOIN filter_account fa ON c.BASEL_ACCT_ID = fa.BASEL_ACCT_ID
    ),
    pit_null AS (
        SELECT COUNT(*) AS n
        FROM emulated.BASEL_PSNL_LOAN_ACCT_DRVD_VARS_2 d
        CROSS JOIN lgdd_obs_tm o
        WHERE d.STREAM = 'NON_RESL'
          AND d.MTH_TM_ID BETWEEN o.OBS_START_TM_ID AND o.OBS_END_TM_ID
          AND d.PIT_STATUS_V2 IS NULL
    )
SELECT step, cnt FROM (
    SELECT 1 AS ord, '1_pop_in_window (any 2104 in LGDD window)' AS step, COUNT(*) AS cnt FROM pop_in_window
    UNION ALL SELECT 2, '2_max_non_def (had CUR in window)', COUNT(*) FROM max_non_def
    UNION ALL SELECT 3, '3_last_new_def (default transition)', COUNT(*) FROM last_new_def
    UNION ALL SELECT 4, '4_lgdd_anchor DEF+A at 2024-01 + 2105 join', COUNT(*) FROM lgdd_anchor
    UNION ALL SELECT 5, '5_lgdd_final (lgdd output rows)', COUNT(*) FROM lgdd_final
    UNION ALL SELECT 6, '6_filter_account (LGDND prior default)', COUNT(*) FROM filter_account
    UNION ALL SELECT 7, '7_lgdnd_final (lgdnd output rows)', COUNT(*) FROM lgdnd_final
    UNION ALL SELECT 8, '8_null_pit_in_lgdd_window', (SELECT n FROM pit_null)
) x ORDER BY ord;
