
UPSTREAM_ASSET = [
    "ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT",
    "features.PIT_STATUS_CROSS_DEFAULT_ORIG",
    "features.PRD_ID",
    "features.MODEL_EXCL_F_V2",
    "features.COMM_F_V2",
    "features.TREATMENT_F",
    "features.OS_BAL_AMT_V2",
]

DOWNSTREAM_ASSET = "emulated.BASEL_PSNL_LOAN_ACCT_DRVD_VARS_2"

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
    WITH base AS (
        SELECT
            a.MTH_TM_ID,
            a.BASEL_ACCT_ID
        FROM ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT a
        WHERE a.MTH_TM_ID =
            {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
          AND a.RECD_STAT_CD IN ('4', '5', '6', '7', '8')
    )
    SELECT
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}' AS STREAM,
        base.BASEL_ACCT_ID,
        base.MTH_TM_ID,
        pit.PIT_STATUS_CROSS_DEFAULT_ORIG AS PIT_STATUS_V2,
        prd.PRD_ID AS PRD_ID,
        mex.MODEL_EXCL_F_V2 AS MODEL_EXCL_F,
        cmf.COMM_F_V2 AS COMM_F_V2,
        trt.TREATMENT_F AS TREATMNT_F,
        osb.OS_BAL_AMT_V2 AS OS_BAL_AMT_V2,
        CURRENT_TIMESTAMP AS INSRT_PROCESS_TMSTMP,
        CURRENT_TIMESTAMP AS UPDT_PROCESS_TMSTMP
    FROM base
    LEFT JOIN features.PIT_STATUS_CROSS_DEFAULT_ORIG pit
        ON pit.BASEL_ACCT_ID = base.BASEL_ACCT_ID
       AND pit.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
       AND pit.SRC_SYS_CD = 'SPL'
    LEFT JOIN features.PRD_ID prd
        ON prd.BASEL_ACCT_ID = base.BASEL_ACCT_ID
       AND prd.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
       AND prd.SRC_SYS_CD = 'SPL'
    LEFT JOIN features.MODEL_EXCL_F_V2 mex
        ON mex.BASEL_ACCT_ID = base.BASEL_ACCT_ID
       AND mex.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    LEFT JOIN features.COMM_F_V2 cmf
        ON cmf.BASEL_ACCT_ID = base.BASEL_ACCT_ID
       AND cmf.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
       AND cmf.SRC_SYS_CD = 'SPL'
    LEFT JOIN features.TREATMENT_F trt
        ON trt.BASEL_ACCT_ID = base.BASEL_ACCT_ID
       AND trt.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    LEFT JOIN features.OS_BAL_AMT_V2 osb
        ON osb.BASEL_ACCT_ID = base.BASEL_ACCT_ID
       AND osb.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """,
):
    pass
