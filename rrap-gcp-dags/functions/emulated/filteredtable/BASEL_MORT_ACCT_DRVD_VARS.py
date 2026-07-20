UPSTREAM_ASSET = [
    "ingestion.MORT_MTH_SNAPSHOT",
    "features.COMM_TP_CD",
    "features.CONSM_PRD_TREATMNT_CD",
    "features.DLQNT_DAY_CNT",
    "features.OS_BAL_AMT",
]

DOWNSTREAM_ASSET = "emulated.BASEL_MORT_ACCT_DRVD_VARS"

_RUNDATE = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
_STREAM = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
_MTH_TM_ID = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}'

DEPENDENCIES = {
    "duckdb_delete": ["duckdb_load"],
}


def duckdb_delete(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    DELETE FROM {DOWNSTREAM_ASSET}
    WHERE OBSN_DT = '{_RUNDATE}'
      AND STREAM = '{_STREAM}'
    """,
):
    pass


def duckdb_load(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} BY NAME
    SELECT
        '{_RUNDATE}' AS OBSN_DT,
        '{_STREAM}' AS STREAM,
        a.MTH_TM_ID,
        a.BASEL_ACCT_ID,
        comm.COMM_TP_CD,
        cons.CONSM_PRD_TREATMNT_CD,
        dday.DLQNT_DAY_CNT,
        osb.OS_BAL_AMT,
        CURRENT_TIMESTAMP AS INSRT_PROCESS_TMSTMP,
        CURRENT_TIMESTAMP AS UPDT_PROCESS_TMSTMP
    FROM ingestion.MORT_MTH_SNAPSHOT a
    LEFT JOIN features.COMM_TP_CD comm
        ON comm.BASEL_ACCT_ID = a.BASEL_ACCT_ID
       AND comm.OBSN_DT = '{_RUNDATE}'
    LEFT JOIN features.CONSM_PRD_TREATMNT_CD cons
        ON cons.BASEL_ACCT_ID = a.BASEL_ACCT_ID
       AND cons.OBSN_DT = '{_RUNDATE}'
       AND cons.SRC_SYS_CD = 'MOR'
    LEFT JOIN features.DLQNT_DAY_CNT dday
        ON dday.BASEL_ACCT_ID = a.BASEL_ACCT_ID
       AND dday.OBSN_DT = '{_RUNDATE}'
       AND dday.SRC_SYS_CD = 'MO'
    LEFT JOIN features.OS_BAL_AMT osb
        ON osb.BASEL_ACCT_ID = a.BASEL_ACCT_ID
       AND osb.OBSN_DT = '{_RUNDATE}'
       AND osb.SRC_SYS_CD = 'MOR'
    WHERE a.MTH_TM_ID = {_MTH_TM_ID}
    """,
):
    pass
