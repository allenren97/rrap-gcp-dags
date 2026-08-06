UPSTREAM_ASSET = [
    "features.PIT_STATUS_CROSS_DEFAULT_ORIG",
    "features.TREATMENT_F",
    "features.MODEL_DFT_F",
    "features.LAST_NEW_DFT_DT",
    "features.LAST_NEW_DFT_BAL_AMT",
    "ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT",
    "ingestion.TM_DIM",
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
    WITH cohort AS (
        SELECT DISTINCT cur12.BASEL_ACCT_ID,
            {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}} - 12 * 40 AS OBSVTN_MTH_TM_ID
        FROM (
            SELECT pit.BASEL_ACCT_ID
            FROM features.PIT_STATUS_CROSS_DEFAULT_ORIG pit
            JOIN ingestion.TM_DIM tm ON tm.TM_LVL_END_DT = pit.OBSN_DT AND TRIM(tm.TM_LVL) = 'Month'
            WHERE tm.TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}} - 12 * 40
              AND pit.SRC_SYS_CD = 'SPL' AND TRIM(pit.PIT_STATUS_CROSS_DEFAULT_ORIG) = 'CUR'
        ) cur12
        JOIN (
            SELECT t.BASEL_ACCT_ID
            FROM features.TREATMENT_F t
            JOIN ingestion.TM_DIM tm ON tm.TM_LVL_END_DT = t.OBSN_DT AND TRIM(tm.TM_LVL) = 'Month'
            WHERE tm.TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}} - 12 * 40
              AND t.TREATMENT_F = 'A'
        ) trt12 ON trt12.BASEL_ACCT_ID = cur12.BASEL_ACCT_ID
        JOIN (
            SELECT DISTINCT p.BASEL_ACCT_ID
            FROM features.PIT_STATUS_CROSS_DEFAULT_ORIG p
            JOIN ingestion.TM_DIM tm ON tm.TM_LVL_END_DT = p.OBSN_DT AND TRIM(tm.TM_LVL) = 'Month'
            JOIN ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT s
                ON s.BASEL_ACCT_ID = p.BASEL_ACCT_ID
               AND s.MTH_TM_ID = tm.TM_ID
            WHERE p.SRC_SYS_CD = 'SPL'
              AND tm.TM_ID BETWEEN {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}} - 12 * 40
                              AND {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
              AND TRIM(p.PIT_STATUS_CROSS_DEFAULT_ORIG) IN ('DEF', 'CHG')
              AND s.TOT_CRNT_BAL_AMT + s.ADD_ON_BAL_AMT + s.ACCR_INTR >= 1
              AND s.TOT_CRNT_BAL_AMT > 0
        ) filt ON filt.BASEL_ACCT_ID = cur12.BASEL_ACCT_ID
        UNION ALL
        SELECT DISTINCT BASEL_ACCT_ID, OBSVTN_MTH_TM_ID
        FROM features.MODEL_DFT_F
        WHERE OBSVTN_MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}} - 24 * 40
          AND OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
          AND SRC_SYS_CD = 'SPL'
    )
    SELECT
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}' AS STREAM,
        c.BASEL_ACCT_ID,
        c.OBSVTN_MTH_TM_ID,
        {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}} AS PROCESS_MTH_TM_ID,
        DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS PROCESS_DATE,
        dt.LAST_NEW_DFT_DT,
        bal.LAST_NEW_DFT_BAL_AMT,
        CASE
            WHEN c.OBSVTN_MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}} - 24 * 40 THEN NULL
            WHEN mdf.BASEL_ACCT_ID IS NOT NULL THEN 'Y'
            ELSE 'N'
        END AS MODEL_DFT_F,
        CURRENT_TIMESTAMP AS INSRT_PROCESS_TMSTMP,
        CURRENT_TIMESTAMP AS UPDT_PROCESS_TMSTMP
    FROM cohort c
    LEFT JOIN features.MODEL_DFT_F mdf
        ON mdf.BASEL_ACCT_ID = c.BASEL_ACCT_ID
       AND mdf.OBSVTN_MTH_TM_ID = c.OBSVTN_MTH_TM_ID
       AND mdf.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
       AND mdf.SRC_SYS_CD = 'SPL'
    LEFT JOIN features.LAST_NEW_DFT_DT dt
        ON dt.BASEL_ACCT_ID = c.BASEL_ACCT_ID
       AND dt.OBSVTN_MTH_TM_ID = c.OBSVTN_MTH_TM_ID
       AND dt.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
       AND dt.SRC_SYS_CD = 'SPL'
    LEFT JOIN features.LAST_NEW_DFT_BAL_AMT bal
        ON bal.BASEL_ACCT_ID = c.BASEL_ACCT_ID
       AND bal.OBSVTN_MTH_TM_ID = c.OBSVTN_MTH_TM_ID
       AND bal.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
       AND bal.SRC_SYS_CD = 'SPL'
    """,
):
    pass
