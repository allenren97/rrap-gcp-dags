"""
Rewrite of J_RRAP_TL10_2201_PSNL_LOAN_OBSVTN_PT_DRVD_VAR.sas only.

Builds emulated.PSNL_LOAN_OBSVTN_PT_DRVD_VAR for the current process month:
  - LGDD path (PIT_STAT_VER_2_CD = 'DEF')
  - LGDND path (PIT_STAT_VER_2_CD = 'CUR')
"""

UPSTREAM_ASSET = [
    "ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT",
    "ingestion.TM_DIM",
    "emulated.BASEL_PSNL_LOAN_ACCT_DRVD_VARS",
    "emulated.BASEL_PSNL_LOAN_ACCT_DRVD_VARS_2",
]

DOWNSTREAM_ASSET = "emulated.PSNL_LOAN_OBSVTN_PT_DRVD_VAR"

DEPENDENCIES = {
    "duckdb_delete": ["duckdb_load"],
}


def duckdb_delete(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    DELETE FROM {DOWNSTREAM_ASSET}
    WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
      AND STREAM = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}'
    """,
):
    pass


def duckdb_load(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} BY NAME
    WITH
        mth_tm_id AS (
            SELECT {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}} AS val
        ),
        process_tm AS (
            SELECT TM_ID, TM_LVL_END_DT
            FROM ingestion.TM_DIM
            WHERE TM_ID = (SELECT val FROM mth_tm_id)
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
            WHERE UPPER(TRIM(d.PIT_STATUS_V2)) = 'CUR'
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
            WHERE UPPER(TRIM(d.PIT_STATUS_V2)) = 'DEF'
              AND d.MTH_TM_ID > m.MAX_NON_DEF_TM_ID
              AND d.MTH_TM_ID <= o.OBS_END_TM_ID
            GROUP BY d.BASEL_ACCT_ID
        ),
        pop_in_window AS (
            SELECT DISTINCT d.BASEL_ACCT_ID
            FROM emulated.BASEL_PSNL_LOAN_ACCT_DRVD_VARS_2 d
            CROSS JOIN lgdd_obs_tm o
            WHERE d.MTH_TM_ID BETWEEN o.OBS_START_TM_ID AND o.OBS_END_TM_ID
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
            WHERE UPPER(TRIM(d.PIT_STATUS_V2)) = 'DEF'
              AND d.MTH_TM_ID BETWEEN o.OBS_START_TM_ID AND o.OBS_END_TM_ID
            GROUP BY d.BASEL_ACCT_ID
        ),
        last_new_def AS (
            SELECT * FROM def_after_cur
            UNION ALL
            SELECT * FROM def_never_cur
        ),
        os_bal_at_def AS (
            SELECT
                l.BASEL_ACCT_ID,
                l.LAST_NEW_DFT_MTH_TM_ID,
                l.LAST_NEW_DFT_DT,
                ROUND(s.TOT_CRNT_BAL_AMT + s.ADD_ON_BAL_AMT + s.ACCR_INTR, 3) AS LAST_NEW_DFT_BAL_AMT
            FROM last_new_def l
            INNER JOIN ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT s
                ON l.BASEL_ACCT_ID = s.BASEL_ACCT_ID
               AND l.LAST_NEW_DFT_MTH_TM_ID = s.MTH_TM_ID
        ),
        lgdd_rows AS (
            SELECT
                a.BASEL_ACCT_ID,
                o.OBS_END_TM_ID AS OBSVTN_MTH_TM_ID,
                o.PROCESS_MTH_TM_ID,
                pf.LAST_NEW_DFT_MTH_TM_ID,
                o.PROCESS_DT,
                o.OBS_END_DT AS OBSVTN_DT,
                TRIM(a.LOAN_NUM) AS LOAN_NUM,
                TRIM(c.CAB) AS CAB,
                c.SUB_PORTFL,
                SUBSTR(c.PIT_STATUS_V2, 1, 3) AS PIT_STAT_VER_2_CD,
                '' AS MODEL_DFT_F,
                pf.LAST_NEW_DFT_DT,
                pf.LAST_NEW_DFT_BAL_AMT,
                -1 AS RCVRY_WINDOW_CUTOFF_TM_ID,
                LAST_DAY((pf.LAST_NEW_DFT_DT + INTERVAL 24 MONTH)::DATE) AS RCVRY_WINDOW_CUTOFF_DT
            FROM lgdd_obs_tm o
            INNER JOIN ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT a
                ON a.MTH_TM_ID = o.OBS_END_TM_ID
            INNER JOIN emulated.BASEL_PSNL_LOAN_ACCT_DRVD_VARS b
                ON a.BASEL_ACCT_ID = b.BASEL_ACCT_ID
               AND a.MTH_TM_ID = b.MTH_TM_ID
            INNER JOIN emulated.BASEL_PSNL_LOAN_ACCT_DRVD_VARS_2 c
                ON a.BASEL_ACCT_ID = c.BASEL_ACCT_ID
               AND a.MTH_TM_ID = c.MTH_TM_ID
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
        lgdnd_last_def AS (
            SELECT
                d.BASEL_ACCT_ID,
                MIN(d.MTH_TM_ID) AS LAST_NEW_DFT_MTH_TM_ID,
                MIN(d.PROCESS_DT) AS LAST_NEW_DFT_DT
            FROM emulated.BASEL_PSNL_LOAN_ACCT_DRVD_VARS_2 d
            CROSS JOIN lgdnd_obs_tm o
            WHERE UPPER(TRIM(d.PIT_STATUS_V2)) IN ('DEF', 'CHG')
              AND d.MTH_TM_ID BETWEEN o.OBS_START_TM_ID AND o.OBS_END_TM_ID
            GROUP BY d.BASEL_ACCT_ID
        ),
        lgdnd_os_bal AS (
            SELECT
                l.BASEL_ACCT_ID,
                l.LAST_NEW_DFT_MTH_TM_ID,
                l.LAST_NEW_DFT_DT,
                ROUND(s.TOT_CRNT_BAL_AMT + s.ADD_ON_BAL_AMT + s.ACCR_INTR, 3) AS LAST_NEW_DFT_BAL_AMT
            FROM lgdnd_last_def l
            INNER JOIN ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT s
                ON l.BASEL_ACCT_ID = s.BASEL_ACCT_ID
               AND l.LAST_NEW_DFT_MTH_TM_ID = s.MTH_TM_ID
        ),
        lgdnd_rows AS (
            SELECT
                a.BASEL_ACCT_ID,
                o.OBS_START_TM_ID AS OBSVTN_MTH_TM_ID,
                o.PROCESS_MTH_TM_ID,
                pf.LAST_NEW_DFT_MTH_TM_ID,
                o.PROCESS_DT,
                o.OBS_START_DT AS OBSVTN_DT,
                TRIM(a.LOAN_NUM) AS LOAN_NUM,
                TRIM(c.CAB) AS CAB,
                c.SUB_PORTFL,
                SUBSTR(c.PIT_STATUS_V2, 1, 3) AS PIT_STAT_VER_2_CD,
                CASE WHEN pf.LAST_NEW_DFT_DT IS NULL THEN 'N' ELSE 'Y' END AS MODEL_DFT_F,
                pf.LAST_NEW_DFT_DT,
                pf.LAST_NEW_DFT_BAL_AMT,
                -1 AS RCVRY_WINDOW_CUTOFF_TM_ID,
                LAST_DAY((pf.LAST_NEW_DFT_DT + INTERVAL 24 MONTH)::DATE) AS RCVRY_WINDOW_CUTOFF_DT
            FROM lgdnd_obs_tm o
            INNER JOIN ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT a
                ON a.MTH_TM_ID = o.OBS_START_TM_ID
            INNER JOIN emulated.BASEL_PSNL_LOAN_ACCT_DRVD_VARS b
                ON a.BASEL_ACCT_ID = b.BASEL_ACCT_ID
               AND a.MTH_TM_ID = b.MTH_TM_ID
            INNER JOIN emulated.BASEL_PSNL_LOAN_ACCT_DRVD_VARS_2 c
                ON a.BASEL_ACCT_ID = c.BASEL_ACCT_ID
               AND a.MTH_TM_ID = c.MTH_TM_ID
               AND UPPER(TRIM(c.PIT_STATUS_V2)) = 'CUR'
               AND c.TREATMNT_F = 'A'
            INNER JOIN filter_account fa ON a.BASEL_ACCT_ID = fa.BASEL_ACCT_ID
            LEFT JOIN lgdnd_os_bal pf ON a.BASEL_ACCT_ID = pf.BASEL_ACCT_ID
        ),
        combined AS (
            SELECT * FROM lgdd_rows
            UNION ALL
            SELECT * FROM lgdnd_rows
        )
    SELECT
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}' AS STREAM,
        c.BASEL_ACCT_ID,
        c.OBSVTN_MTH_TM_ID,
        c.PROCESS_MTH_TM_ID,
        c.LAST_NEW_DFT_MTH_TM_ID,
        c.PROCESS_DT,
        c.OBSVTN_DT,
        c.LOAN_NUM,
        c.CAB,
        c.SUB_PORTFL,
        c.PIT_STAT_VER_2_CD,
        c.MODEL_DFT_F,
        c.LAST_NEW_DFT_DT,
        c.LAST_NEW_DFT_BAL_AMT,
        c.RCVRY_WINDOW_CUTOFF_TM_ID,
        c.RCVRY_WINDOW_CUTOFF_DT,
        cutoff_tm.TM_ID AS RCVRY_WINDOW_CUTOFF_MTH_TM_ID,
        CURRENT_TIMESTAMP AS INSRT_PROCESS_TMSTMP,
        CURRENT_TIMESTAMP AS UPDT_PROCESS_TMSTMP
    FROM combined c
    LEFT JOIN ingestion.TM_DIM cutoff_tm
        ON TRIM(cutoff_tm.TM_LVL) = 'Month'
       AND c.RCVRY_WINDOW_CUTOFF_DT BETWEEN cutoff_tm.TM_LVL_ST_DT AND cutoff_tm.TM_LVL_END_DT
    """,
):
    pass
