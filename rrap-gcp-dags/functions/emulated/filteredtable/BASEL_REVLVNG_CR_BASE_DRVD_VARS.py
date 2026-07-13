
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


def export_result(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    SELECT
        '{_RUNDATE}' AS OBSN_DT,
        '{_STREAM}' AS STREAM,
        a.MTH_TM_ID,
        a.BASEL_ACCT_ID,
        a.PRIM_BASEL_CUST_ID AS BASEL_CUST_ID,
        a.ACCT_NUM,
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
    FROM ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT a
    LEFT JOIN features.BASEL_PRD_CD prd
        ON a.BASEL_ACCT_ID = prd.BASEL_ACCT_ID
       AND prd.OBSN_DT = '{_RUNDATE}'
    LEFT JOIN features.PIT_STAT_VER_2_CD90 pit90
        ON a.BASEL_ACCT_ID = pit90.BASEL_ACCT_ID
       AND pit90.OBSN_DT = '{_RUNDATE}'
    LEFT JOIN features.PIT_STAT_VER_2_CD180 pit180
        ON a.BASEL_ACCT_ID = pit180.BASEL_ACCT_ID
       AND pit180.OBSN_DT = '{_RUNDATE}'
    LEFT JOIN features.PIT_STATUS_CROSS_DEFAULT_ORIG pit
        ON a.BASEL_ACCT_ID = pit.BASEL_ACCT_ID
       AND pit.OBSN_DT = '{_RUNDATE}'
       AND pit.SRC_SYS_CD = 'KS'
    LEFT JOIN features.REVISED_EXPSR_AMT expsr
        ON a.BASEL_ACCT_ID = expsr.BASEL_ACCT_ID
       AND expsr.OBSN_DT = '{_RUNDATE}'
    LEFT JOIN features.SML_BUS_F sml
        ON a.BASEL_ACCT_ID = sml.BASEL_ACCT_ID
       AND sml.OBSN_DT = '{_RUNDATE}'
    LEFT JOIN features.TRNST_EXCLSN_F trnst
        ON a.BASEL_ACCT_ID = trnst.BASEL_ACCT_ID
       AND trnst.OBSN_DT = '{_RUNDATE}'
    LEFT JOIN features.RS_F rs
        ON a.BASEL_ACCT_ID = rs.BASEL_ACCT_ID
       AND rs.OBSN_DT = '{_RUNDATE}'
    LEFT JOIN features.ACCRL_STAT_F accrl
        ON a.BASEL_ACCT_ID = accrl.BASEL_ACCT_ID
       AND accrl.OBSN_DT = '{_RUNDATE}'
    LEFT JOIN features.LTV_TP_CD ltv
        ON a.BASEL_ACCT_ID = ltv.BASEL_ACCT_ID
       AND ltv.OBSN_DT = '{_RUNDATE}'
    LEFT JOIN features.BNKRPY_F bnkrpy
        ON a.BASEL_ACCT_ID = bnkrpy.BASEL_ACCT_ID
       AND bnkrpy.OBSN_DT = '{_RUNDATE}'
    LEFT JOIN features.CONSM_PRD_TREATMNT_CD trt
        ON a.BASEL_ACCT_ID = trt.BASEL_ACCT_ID
       AND trt.OBSN_DT = '{_RUNDATE}'
    LEFT JOIN features.HELOC_F heloc
        ON a.BASEL_ACCT_ID = heloc.BASEL_ACCT_ID
       AND heloc.OBSN_DT = '{_RUNDATE}'
    LEFT JOIN features.STEP_CD step
        ON a.BASEL_ACCT_ID = step.BASEL_ACCT_ID
       AND step.OBSN_DT = '{_RUNDATE}'
    LEFT JOIN features.CONSM_SCORECRD_EXCLSN_F csef
        ON a.BASEL_ACCT_ID = csef.BASEL_ACCT_ID
       AND csef.OBSN_DT = '{_RUNDATE}'
    LEFT JOIN features.TOTAL_EXPSR_ABOVE_LMT_F teaf
        ON a.BASEL_ACCT_ID = teaf.BASEL_ACCT_ID
       AND teaf.OBSN_DT = '{_RUNDATE}'
    WHERE a.MTH_TM_ID = {_MTH_TM_ID}
    """,
):
    pass


def export_result(
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