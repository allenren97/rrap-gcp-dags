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


# Each feature/dim is pre-filtered to the run-month partition and deduped to one
# row per BASEL_ACCT_ID BEFORE joining. This (a) forces DuckLake partition pruning
# to the single OBSN_DT partition instead of scanning all backfilled months, and
# (b) guarantees each LEFT JOIN is 1:1 so rows can't fan out across the 9 joins.
def duckdb_load(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} BY NAME
    WITH base AS (
        SELECT
            a.MTH_TM_ID,
            a.BASEL_ACCT_ID,
            a.PRIM_BASEL_CUST_ID AS BASEL_CUST_ID,
            ba.ACCT_NUM
        FROM ingestion.MORT_MTH_SNAPSHOT a
        LEFT JOIN (
            SELECT BASEL_ACCT_ID, TRIM(ACCT_NUM) AS ACCT_NUM
            FROM ingestion.BASEL_ACCT_DIM
            WHERE SRC_APP_CD = 'MO'
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY BASEL_ACCT_ID
                ORDER BY CASE WHEN COALESCE(SRC_SYS_DEL_F, 'N') = 'Y' THEN 1 ELSE 0 END,
                         ACCT_NUM DESC NULLS LAST
            ) = 1
        ) ba ON a.BASEL_ACCT_ID = ba.BASEL_ACCT_ID
        WHERE a.MTH_TM_ID = {_MTH_TM_ID}
    )
    SELECT
        '{_RUNDATE}' AS OBSN_DT,
        '{_STREAM}' AS STREAM,
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
        SELECT BASEL_ACCT_ID, DLQNT_MTH_CNT FROM features.DLQNT_MTH_CNT
        WHERE OBSN_DT = '{_RUNDATE}' AND SRC_SYS_CD = 'MO'
        QUALIFY ROW_NUMBER() OVER (PARTITION BY BASEL_ACCT_ID ORDER BY DLQNT_MTH_CNT DESC NULLS LAST) = 1
    ) dmth ON dmth.BASEL_ACCT_ID = base.BASEL_ACCT_ID
    LEFT JOIN (
        SELECT BASEL_ACCT_ID, LAND_RGSTRN_ACT_STAT_F FROM features.LAND_RGSTRN_ACT_STAT_F
        WHERE OBSN_DT = '{_RUNDATE}'
        QUALIFY ROW_NUMBER() OVER (PARTITION BY BASEL_ACCT_ID ORDER BY LAND_RGSTRN_ACT_STAT_F DESC NULLS LAST) = 1
    ) land ON land.BASEL_ACCT_ID = base.BASEL_ACCT_ID
    LEFT JOIN (
        SELECT BASEL_ACCT_ID, OS_BAL_AMT FROM features.OS_BAL_AMT
        WHERE OBSN_DT = '{_RUNDATE}' AND SRC_SYS_CD = 'MOR'
        QUALIFY ROW_NUMBER() OVER (PARTITION BY BASEL_ACCT_ID ORDER BY OS_BAL_AMT DESC NULLS LAST) = 1
    ) osb ON osb.BASEL_ACCT_ID = base.BASEL_ACCT_ID
    LEFT JOIN (
        SELECT BASEL_ACCT_ID, PIT_STAT_VER_1_CD FROM features.PIT_STAT_VER_1_CD
        WHERE OBSN_DT = '{_RUNDATE}' AND SRC_SYS_CD = 'MOR'
        QUALIFY ROW_NUMBER() OVER (PARTITION BY BASEL_ACCT_ID ORDER BY PIT_STAT_VER_1_CD DESC NULLS LAST) = 1
    ) pit ON pit.BASEL_ACCT_ID = base.BASEL_ACCT_ID
    LEFT JOIN (
        SELECT BASEL_ACCT_ID, STEP_FLAG FROM features.STEP_FLAG
        WHERE OBSN_DT = '{_RUNDATE}' AND SRC_SYS_CD = 'MOR'
        QUALIFY ROW_NUMBER() OVER (PARTITION BY BASEL_ACCT_ID ORDER BY STEP_FLAG DESC NULLS LAST) = 1
    ) step ON step.BASEL_ACCT_ID = base.BASEL_ACCT_ID
    LEFT JOIN (
        SELECT BASEL_ACCT_ID, TRNST_EXCLSN_F_MORT FROM features.TRNST_EXCLSN_F_MORT
        WHERE OBSN_DT = '{_RUNDATE}' AND SRC_SYS_CD = 'MOR'
        QUALIFY ROW_NUMBER() OVER (PARTITION BY BASEL_ACCT_ID ORDER BY TRNST_EXCLSN_F_MORT DESC NULLS LAST) = 1
    ) trn ON trn.BASEL_ACCT_ID = base.BASEL_ACCT_ID
    """,
):
    pass
