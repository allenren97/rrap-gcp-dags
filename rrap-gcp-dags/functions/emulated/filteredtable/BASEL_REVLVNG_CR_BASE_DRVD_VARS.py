
UPSTREAM_ASSET = [
    "ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT",
    "features.BASEL_PRD_CD",
    "features.PIT_STAT_VER_2_CD90",
    "features.PIT_STAT_VER_2_CD180",
    "features.PIT_STATUS_CROSS_DEFAULT_ORIG",
    "features.REVISED_EXPSR_AMT",
    "features.SML_BUS_F",
    "features.TRNST_EXCLSN_F",
    "features.RS_F",
    "features.ACCRL_STAT_F",
    "features.LTV_TP_CD",
    "features.BNKRPY_F",
    "features.CONSM_PRD_TREATMNT_CD",
    "features.HELOC_F",
    "features.STEP_CD",
    "features.CONSM_SCORECRD_EXCLSN_F",
    "features.TOTAL_EXPSR_ABOVE_LMT_F",
]

DOWNSTREAM_ASSET = "emulated.BASEL_REVLVNG_CR_BASE_DRVD_VARS_FEATURE_GENERATED"

_TASK_GROUP = "filteredtable__BASEL_REVLVNG_CR_BASE_DRVD_VARS_FEATURE_GENERATED"

_RUNDATE = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
_STREAM = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
_MTH_TM_ID = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}'

_TASK_GROUP = "filteredtable__BASEL_REVLVNG_CR_BASE_DRVD_VARS"

DEPENDENCIES = {
    "duckdb_delete": ["export_result"],
    "export_result": ["duckdb_load"],
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


# Each feature is pre-filtered to the run-month partition and deduped to one row
# per BASEL_ACCT_ID before joining -- forces DuckLake partition pruning to the single
# OBSN_DT partition (was scanning all backfilled months via ON-clause filters) and
# keeps every LEFT JOIN 1:1 so the 16 joins can't fan out.
def export_result(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    WITH base AS (
        SELECT
            MTH_TM_ID,
            BASEL_ACCT_ID,
            PRIM_BASEL_CUST_ID AS BASEL_CUST_ID,
            ACCT_NUM
        FROM ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT
        WHERE MTH_TM_ID = {_MTH_TM_ID}
    )
    SELECT
        '{_RUNDATE}' AS OBSN_DT,
        '{_STREAM}' AS STREAM,
        base.MTH_TM_ID,
        base.BASEL_ACCT_ID,
        base.BASEL_CUST_ID,
        base.ACCT_NUM,
        prd.BASEL_PRD_CD,
        prd.BASEL_PRD_DESC,
        csef.CONSM_SCORECRD_EXCLSN_F,
        trt.CONSM_PRD_TREATMNT_CD,
        heloc.HELOC_F,
        pit.PIT_STATUS_CROSS_DEFAULT_ORIG AS PIT_STAT_VER_2_CD,
        expsr.REVISED_EXPSR_AMT,
        rs.RS_F,
        sml.SML_BUS_F,
        step.STEP_CD,
        trnst.TRNST_EXCLSN_F,
        accrl.ACCRL_STAT_F,
        ltv.LTV_TP_CD,
        bnkrpy.BNKRPY_F,
        pit90.PIT_STAT_VER_2_CD90,
        pit180.PIT_STAT_VER_2_CD180,
        teaf.TOTAL_EXPSR_ABOVE_LMT_F
    FROM base
    LEFT JOIN (
        SELECT BASEL_ACCT_ID, BASEL_PRD_CD, BASEL_PRD_DESC FROM features.BASEL_PRD_CD
        WHERE OBSN_DT = '{_RUNDATE}'
        QUALIFY ROW_NUMBER() OVER (PARTITION BY BASEL_ACCT_ID ORDER BY BASEL_PRD_CD DESC NULLS LAST) = 1
    ) prd ON prd.BASEL_ACCT_ID = base.BASEL_ACCT_ID
    LEFT JOIN (
        SELECT BASEL_ACCT_ID, PIT_STAT_VER_2_CD90 FROM features.PIT_STAT_VER_2_CD90
        WHERE OBSN_DT = '{_RUNDATE}'
        QUALIFY ROW_NUMBER() OVER (PARTITION BY BASEL_ACCT_ID ORDER BY PIT_STAT_VER_2_CD90 DESC NULLS LAST) = 1
    ) pit90 ON pit90.BASEL_ACCT_ID = base.BASEL_ACCT_ID
    LEFT JOIN (
        SELECT BASEL_ACCT_ID, PIT_STAT_VER_2_CD180 FROM features.PIT_STAT_VER_2_CD180
        WHERE OBSN_DT = '{_RUNDATE}'
        QUALIFY ROW_NUMBER() OVER (PARTITION BY BASEL_ACCT_ID ORDER BY PIT_STAT_VER_2_CD180 DESC NULLS LAST) = 1
    ) pit180 ON pit180.BASEL_ACCT_ID = base.BASEL_ACCT_ID
    LEFT JOIN (
        SELECT BASEL_ACCT_ID, PIT_STATUS_CROSS_DEFAULT_ORIG FROM features.PIT_STATUS_CROSS_DEFAULT_ORIG
        WHERE OBSN_DT = '{_RUNDATE}' AND SRC_SYS_CD = 'KS'
        QUALIFY ROW_NUMBER() OVER (PARTITION BY BASEL_ACCT_ID ORDER BY PIT_STATUS_CROSS_DEFAULT_ORIG DESC NULLS LAST) = 1
    ) pit ON pit.BASEL_ACCT_ID = base.BASEL_ACCT_ID
    LEFT JOIN (
        SELECT BASEL_ACCT_ID, REVISED_EXPSR_AMT FROM features.REVISED_EXPSR_AMT
        WHERE OBSN_DT = '{_RUNDATE}'
        QUALIFY ROW_NUMBER() OVER (PARTITION BY BASEL_ACCT_ID ORDER BY REVISED_EXPSR_AMT DESC NULLS LAST) = 1
    ) expsr ON expsr.BASEL_ACCT_ID = base.BASEL_ACCT_ID
    LEFT JOIN (
        SELECT BASEL_ACCT_ID, SML_BUS_F FROM features.SML_BUS_F
        WHERE OBSN_DT = '{_RUNDATE}'
        QUALIFY ROW_NUMBER() OVER (PARTITION BY BASEL_ACCT_ID ORDER BY SML_BUS_F DESC NULLS LAST) = 1
    ) sml ON sml.BASEL_ACCT_ID = base.BASEL_ACCT_ID
    LEFT JOIN (
        SELECT BASEL_ACCT_ID, TRNST_EXCLSN_F FROM features.TRNST_EXCLSN_F
        WHERE OBSN_DT = '{_RUNDATE}'
        QUALIFY ROW_NUMBER() OVER (PARTITION BY BASEL_ACCT_ID ORDER BY TRNST_EXCLSN_F DESC NULLS LAST) = 1
    ) trnst ON trnst.BASEL_ACCT_ID = base.BASEL_ACCT_ID
    LEFT JOIN (
        SELECT BASEL_ACCT_ID, RS_F FROM features.RS_F
        WHERE OBSN_DT = '{_RUNDATE}'
        QUALIFY ROW_NUMBER() OVER (PARTITION BY BASEL_ACCT_ID ORDER BY RS_F DESC NULLS LAST) = 1
    ) rs ON rs.BASEL_ACCT_ID = base.BASEL_ACCT_ID
    LEFT JOIN (
        SELECT BASEL_ACCT_ID, ACCRL_STAT_F FROM features.ACCRL_STAT_F
        WHERE OBSN_DT = '{_RUNDATE}'
        QUALIFY ROW_NUMBER() OVER (PARTITION BY BASEL_ACCT_ID ORDER BY ACCRL_STAT_F DESC NULLS LAST) = 1
    ) accrl ON accrl.BASEL_ACCT_ID = base.BASEL_ACCT_ID
    LEFT JOIN (
        SELECT BASEL_ACCT_ID, LTV_TP_CD FROM features.LTV_TP_CD
        WHERE OBSN_DT = '{_RUNDATE}'
        QUALIFY ROW_NUMBER() OVER (PARTITION BY BASEL_ACCT_ID ORDER BY LTV_TP_CD DESC NULLS LAST) = 1
    ) ltv ON ltv.BASEL_ACCT_ID = base.BASEL_ACCT_ID
    LEFT JOIN (
        SELECT BASEL_ACCT_ID, BNKRPY_F FROM features.BNKRPY_F
        WHERE OBSN_DT = '{_RUNDATE}'
        QUALIFY ROW_NUMBER() OVER (PARTITION BY BASEL_ACCT_ID ORDER BY BNKRPY_F DESC NULLS LAST) = 1
    ) bnkrpy ON bnkrpy.BASEL_ACCT_ID = base.BASEL_ACCT_ID
    LEFT JOIN (
        SELECT BASEL_ACCT_ID, CONSM_PRD_TREATMNT_CD FROM features.CONSM_PRD_TREATMNT_CD
        WHERE OBSN_DT = '{_RUNDATE}'
        QUALIFY ROW_NUMBER() OVER (PARTITION BY BASEL_ACCT_ID ORDER BY CONSM_PRD_TREATMNT_CD DESC NULLS LAST) = 1
    ) trt ON trt.BASEL_ACCT_ID = base.BASEL_ACCT_ID
    LEFT JOIN (
        SELECT BASEL_ACCT_ID, HELOC_F FROM features.HELOC_F
        WHERE OBSN_DT = '{_RUNDATE}'
        QUALIFY ROW_NUMBER() OVER (PARTITION BY BASEL_ACCT_ID ORDER BY HELOC_F DESC NULLS LAST) = 1
    ) heloc ON heloc.BASEL_ACCT_ID = base.BASEL_ACCT_ID
    LEFT JOIN (
        SELECT BASEL_ACCT_ID, STEP_CD FROM features.STEP_CD
        WHERE OBSN_DT = '{_RUNDATE}'
        QUALIFY ROW_NUMBER() OVER (PARTITION BY BASEL_ACCT_ID ORDER BY STEP_CD DESC NULLS LAST) = 1
    ) step ON step.BASEL_ACCT_ID = base.BASEL_ACCT_ID
    LEFT JOIN (
        SELECT BASEL_ACCT_ID, CONSM_SCORECRD_EXCLSN_F FROM features.CONSM_SCORECRD_EXCLSN_F
        WHERE OBSN_DT = '{_RUNDATE}'
        QUALIFY ROW_NUMBER() OVER (PARTITION BY BASEL_ACCT_ID ORDER BY CONSM_SCORECRD_EXCLSN_F DESC NULLS LAST) = 1
    ) csef ON csef.BASEL_ACCT_ID = base.BASEL_ACCT_ID
    LEFT JOIN (
        SELECT BASEL_ACCT_ID, TOTAL_EXPSR_ABOVE_LMT_F FROM features.TOTAL_EXPSR_ABOVE_LMT_F
        WHERE OBSN_DT = '{_RUNDATE}'
        QUALIFY ROW_NUMBER() OVER (PARTITION BY BASEL_ACCT_ID ORDER BY TOTAL_EXPSR_ABOVE_LMT_F DESC NULLS LAST) = 1
    ) teaf ON teaf.BASEL_ACCT_ID = base.BASEL_ACCT_ID
    """,
):
    pass


def duckdb_load(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} BY NAME
    SELECT *
    FROM read_parquet(
        '{{{{ task_instance.xcom_pull(task_ids="{_TASK_GROUP}.export_result", key="parquet") }}}}'
    )
    """,
):
    pass