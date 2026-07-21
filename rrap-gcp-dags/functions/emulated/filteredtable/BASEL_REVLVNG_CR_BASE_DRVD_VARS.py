UPSTREAM_ASSET = [
    "ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT",
    "features.PIT_STATUS_CROSS_DEFAULT_ORIG",
    "features.CONSM_PRD_TREATMNT_CD",
    "features.CONSM_SCORECRD_EXCLSN_F",
]

DOWNSTREAM_ASSET = "emulated.BASEL_REVLVNG_CR_BASE_DRVD_VARS"

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


# Only the columns custuniv_01 reads (REV branch): PIT_STAT_VER_2_CD,
# CONSM_PRD_TREATMNT_CD, CONSM_SCORECRD_EXCLSN_F. Base + each feature are deduped
# to one row per BASEL_ACCT_ID before joining, so the LEFT JOINs stay 1:1 and
# each feature is pre-filtered to the run-month partition (partition pruning).
def duckdb_load(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} BY NAME
    WITH base AS (
        SELECT MTH_TM_ID, BASEL_ACCT_ID
        FROM ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT
        WHERE MTH_TM_ID = {_MTH_TM_ID}
        QUALIFY ROW_NUMBER() OVER (PARTITION BY BASEL_ACCT_ID ORDER BY MTH_TM_ID) = 1
    )
    SELECT
        '{_RUNDATE}' AS OBSN_DT,
        '{_STREAM}' AS STREAM,
        base.MTH_TM_ID,
        base.BASEL_ACCT_ID,
        pit.PIT_STATUS_CROSS_DEFAULT_ORIG AS PIT_STAT_VER_2_CD,
        cons.CONSM_PRD_TREATMNT_CD,
        csef.CONSM_SCORECRD_EXCLSN_F
    FROM base
    LEFT JOIN (
        SELECT BASEL_ACCT_ID, PIT_STATUS_CROSS_DEFAULT_ORIG FROM features.PIT_STATUS_CROSS_DEFAULT_ORIG
        WHERE OBSN_DT = '{_RUNDATE}' AND SRC_SYS_CD = 'KS'
        QUALIFY ROW_NUMBER() OVER (PARTITION BY BASEL_ACCT_ID ORDER BY PIT_STATUS_CROSS_DEFAULT_ORIG DESC NULLS LAST) = 1
    ) pit ON pit.BASEL_ACCT_ID = base.BASEL_ACCT_ID
    LEFT JOIN (
        SELECT BASEL_ACCT_ID, CONSM_PRD_TREATMNT_CD FROM features.CONSM_PRD_TREATMNT_CD
        WHERE OBSN_DT = '{_RUNDATE}'
        QUALIFY ROW_NUMBER() OVER (PARTITION BY BASEL_ACCT_ID ORDER BY CONSM_PRD_TREATMNT_CD DESC NULLS LAST) = 1
    ) cons ON cons.BASEL_ACCT_ID = base.BASEL_ACCT_ID
    LEFT JOIN (
        SELECT BASEL_ACCT_ID, CONSM_SCORECRD_EXCLSN_F FROM features.CONSM_SCORECRD_EXCLSN_F
        WHERE OBSN_DT = '{_RUNDATE}'
        QUALIFY ROW_NUMBER() OVER (PARTITION BY BASEL_ACCT_ID ORDER BY CONSM_SCORECRD_EXCLSN_F DESC NULLS LAST) = 1
    ) csef ON csef.BASEL_ACCT_ID = base.BASEL_ACCT_ID
    """,
):
    pass
