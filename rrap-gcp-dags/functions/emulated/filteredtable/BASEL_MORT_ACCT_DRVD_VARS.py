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


# Base + each feature are deduped to one row per BASEL_ACCT_ID before joining, so
# the LEFT JOINs stay 1:1 (a duplicate row in any source would otherwise fan the
# output out).
def duckdb_load(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} BY NAME
    WITH base AS (
        SELECT MTH_TM_ID, BASEL_ACCT_ID
        FROM ingestion.MORT_MTH_SNAPSHOT
        WHERE MTH_TM_ID = {_MTH_TM_ID}
        QUALIFY ROW_NUMBER() OVER (PARTITION BY BASEL_ACCT_ID ORDER BY MTH_TM_ID) = 1
    )
    SELECT
        '{_RUNDATE}' AS OBSN_DT,
        '{_STREAM}' AS STREAM,
        base.MTH_TM_ID,
        base.BASEL_ACCT_ID,
        comm.COMM_TP_CD,
        cons.CONSM_PRD_TREATMNT_CD,
        dday.DLQNT_DAY_CNT,
        osb.OS_BAL_AMT,
        CURRENT_TIMESTAMP AS INSRT_PROCESS_TMSTMP,
        CURRENT_TIMESTAMP AS UPDT_PROCESS_TMSTMP
    FROM base
    LEFT JOIN (
        SELECT BASEL_ACCT_ID, COMM_TP_CD FROM features.COMM_TP_CD
        WHERE OBSN_DT = '{_RUNDATE}'
        QUALIFY ROW_NUMBER() OVER (PARTITION BY BASEL_ACCT_ID ORDER BY COMM_TP_CD DESC NULLS LAST) = 1
    ) comm ON comm.BASEL_ACCT_ID = base.BASEL_ACCT_ID
    LEFT JOIN (
        SELECT BASEL_ACCT_ID, CONSM_PRD_TREATMNT_CD FROM features.CONSM_PRD_TREATMNT_CD
        WHERE OBSN_DT = '{_RUNDATE}' AND SRC_SYS_CD = 'MOR'
        QUALIFY ROW_NUMBER() OVER (PARTITION BY BASEL_ACCT_ID ORDER BY CONSM_PRD_TREATMNT_CD DESC NULLS LAST) = 1
    ) cons ON cons.BASEL_ACCT_ID = base.BASEL_ACCT_ID
    LEFT JOIN (
        SELECT BASEL_ACCT_ID, DLQNT_DAY_CNT FROM features.DLQNT_DAY_CNT
        WHERE OBSN_DT = '{_RUNDATE}' AND SRC_SYS_CD = 'MO'
        QUALIFY ROW_NUMBER() OVER (PARTITION BY BASEL_ACCT_ID ORDER BY DLQNT_DAY_CNT DESC NULLS LAST) = 1
    ) dday ON dday.BASEL_ACCT_ID = base.BASEL_ACCT_ID
    LEFT JOIN (
        SELECT BASEL_ACCT_ID, OS_BAL_AMT FROM features.OS_BAL_AMT
        WHERE OBSN_DT = '{_RUNDATE}' AND SRC_SYS_CD = 'MOR'
        QUALIFY ROW_NUMBER() OVER (PARTITION BY BASEL_ACCT_ID ORDER BY OS_BAL_AMT DESC NULLS LAST) = 1
    ) osb ON osb.BASEL_ACCT_ID = base.BASEL_ACCT_ID
    """,
):
    pass
