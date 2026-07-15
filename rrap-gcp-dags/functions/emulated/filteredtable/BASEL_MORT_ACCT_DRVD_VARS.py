UPSTREAM_ASSET = [
    "ingestion.MORT_MTH_SNAPSHOT",
    "ingestion.BASEL_ACCT_DIM",
    "features.COMM_TP_CD",
    "features.CONSM_PRD_TREATMNT_CD",
    "features.DLQNT_DAY_CNT",
    "features.DLQNT_MTH_CNT",
    "features.LAND_RGSTRN_ACT_STAT_F",
    "features.OS_BAL_AMT",
    "features.PIT_STAT_VER_1_CD",
    "features.STEP_FLAG",
    "features.TRNST_EXCLSN_F_MORT",
]

DOWNSTREAM_ASSET = "emulated.BASEL_MORT_ACCT_DRVD_VARS"

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
            a.BASEL_ACCT_ID,
            a.PRIM_BASEL_CUST_ID AS BASEL_CUST_ID,
            TRIM(ba.ACCT_NUM) AS ACCT_NUM
        FROM ingestion.MORT_MTH_SNAPSHOT a
        LEFT JOIN ingestion.BASEL_ACCT_DIM ba
            ON a.BASEL_ACCT_ID = ba.BASEL_ACCT_ID
           AND ba.SRC_APP_CD = 'MO'
        WHERE a.MTH_TM_ID =
            {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    )
    SELECT
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}' AS STREAM,
        base.MTH_TM_ID,
        base.BASEL_ACCT_ID,
        base.BASEL_CUST_ID,
        base.ACCT_NUM,
        comm.COMM_TP_CD,
        cons.CONSM_PRD_TREATMNT_CD,
        dday.DLQNT_DAY_CNT,
        dmth.DLQNT_MTH_CNT,
        land.LAND_RGSTRN_ACT_STAT_F,
        osb.OS_BAL_AMT,
        pit.PIT_STAT_VER_1_CD,
        CASE
            WHEN step.STEP_FLAG = 'STEP' THEN 'Y'
            WHEN step.STEP_FLAG = 'STANDALONE' THEN 'N'
            ELSE NULL
        END AS STEP_F,
        trn.TRNST_EXCLSN_F_MORT AS TRNST_EXCLSN_F,
        CURRENT_TIMESTAMP AS INSRT_PROCESS_TMSTMP,
        CURRENT_TIMESTAMP AS UPDT_PROCESS_TMSTMP
    FROM base
    LEFT JOIN features.COMM_TP_CD comm
        ON comm.BASEL_ACCT_ID = base.BASEL_ACCT_ID
       AND comm.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    LEFT JOIN features.CONSM_PRD_TREATMNT_CD cons
        ON cons.BASEL_ACCT_ID = base.BASEL_ACCT_ID
       AND cons.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
       AND cons.SRC_SYS_CD = 'MOR'
    LEFT JOIN features.DLQNT_DAY_CNT dday
        ON dday.BASEL_ACCT_ID = base.BASEL_ACCT_ID
       AND dday.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
       AND dday.SRC_SYS_CD = 'MO'
    LEFT JOIN features.DLQNT_MTH_CNT dmth
        ON dmth.BASEL_ACCT_ID = base.BASEL_ACCT_ID
       AND dmth.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
       AND dmth.SRC_SYS_CD = 'MO'
    LEFT JOIN features.LAND_RGSTRN_ACT_STAT_F land
        ON land.BASEL_ACCT_ID = base.BASEL_ACCT_ID
       AND land.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    LEFT JOIN features.OS_BAL_AMT osb
        ON osb.BASEL_ACCT_ID = base.BASEL_ACCT_ID
       AND osb.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
       AND osb.SRC_SYS_CD = 'MOR'
    LEFT JOIN features.PIT_STAT_VER_1_CD pit
        ON pit.BASEL_ACCT_ID = base.BASEL_ACCT_ID
       AND pit.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
       AND pit.SRC_SYS_CD = 'MOR'
    LEFT JOIN features.STEP_FLAG step
        ON step.BASEL_ACCT_ID = base.BASEL_ACCT_ID
       AND step.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
       AND step.SRC_SYS_CD = 'MOR'
    LEFT JOIN features.TRNST_EXCLSN_F_MORT trn
        ON trn.BASEL_ACCT_ID = base.BASEL_ACCT_ID
       AND trn.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
       AND trn.SRC_SYS_CD = 'MOR'
    """,
):
    pass
