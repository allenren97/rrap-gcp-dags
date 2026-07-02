from airflow.sdk import get_current_context, Param
from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

from bns.rrap.helpers.model_utility import _get_segment


DAG_PARAMS = {
    "mth_tm_id": Param("", type=["null", "string"], description="MTH_TM_ID"),
    "rundate": Param("", type=["null", "string"], description="RUNDATE"),
}

MODEL = "standalone_heloc_lgdnd"

UPSTREAM_ASSET = [
    "features.MODEL_EXCL_F",
    "features.WRITTEN_OUT_F",
    "features.CR_LMT_AMT",
    "features.PIT_STATUS_ACCOUNT",
    "features.STEP_SUB_PORT",
    "features.TREATMENT_F",
    "features.TOT_NEW_BAL_AMT",
    "ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT",
]
DOWNSTREAM_ASSET = f"models.{MODEL.upper()}_SEGMENT"
DEPENDENCIES = {
    "export_acct_list": ["get_segment"],
    "get_segment": ["duckdb_delete"],
    "duckdb_delete": ["duckdb_load"],
}


def export_acct_list(
    duckdb_conn_id="duckdb-conn",
    sql=r"""
    WITH
        rvl AS (
            SELECT
                basel_acct_id,
                CASE
                    WHEN prim_basel_cust_id <= 0 THEN NULL
                    ELSE prim_basel_cust_id
                END AS BASEL_CUST_ID
            FROM
                ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT
            WHERE
                MTH_TM_ID = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}
        ),
        PIT_STATUS AS (
            SELECT * FROM features.PIT_STATUS_ACCOUNT WHERE obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
        ),
        MODEL_EXCL_F AS (
            SELECT * FROM features.MODEL_EXCL_F WHERE obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
        ),
        WRITTEN_OUT_F AS (
            SELECT * FROM features.WRITTEN_OUT_F WHERE obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
        ),
        TREATMENT_F AS (
            SELECT * FROM features.TREATMENT_F WHERE obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
        ),
        CR_LMT_AMT AS (
            SELECT * FROM features.CR_LMT_AMT WHERE obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
        ),
        STEP_SUB_PORT AS (
            SELECT * FROM features.STEP_SUB_PORT WHERE obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
        ),
        TOT_NEW_BAL_AMT AS (
            SELECT * FROM features.TOT_NEW_BAL_AMT WHERE obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
        )
    SELECT
        main.*,
        PIT_STATUS.PIT_STATUS_ACCOUNT,
        MODEL_EXCL_F.MODEL_EXCL_F,
        WRITTEN_OUT_F.WRITTEN_OUT_F,
        TREATMENT_F.TREATMENT_F,
        CR_LMT_AMT.CR_LMT_AMT,
        STEP_SUB_PORT.STEP_SUB_PORT,
        TOT_NEW_BAL_AMT.TOT_NEW_BAL_AMT,
        NULL as SCORE
    FROM
        rvl AS main
        LEFT JOIN PIT_STATUS ON main.BASEL_ACCT_ID = PIT_STATUS.BASEL_ACCT_ID
        LEFT JOIN MODEL_EXCL_F ON main.BASEL_ACCT_ID = MODEL_EXCL_F.BASEL_ACCT_ID
        LEFT JOIN WRITTEN_OUT_F ON main.BASEL_ACCT_ID = WRITTEN_OUT_F.BASEL_ACCT_ID
        LEFT JOIN TREATMENT_F ON main.BASEL_ACCT_ID = TREATMENT_F.BASEL_ACCT_ID
        LEFT JOIN CR_LMT_AMT ON main.BASEL_ACCT_ID = CR_LMT_AMT.BASEL_ACCT_ID
        LEFT JOIN STEP_SUB_PORT ON main.BASEL_ACCT_ID = STEP_SUB_PORT.BASEL_ACCT_ID
        LEFT JOIN TOT_NEW_BAL_AMT ON main.BASEL_ACCT_ID = TOT_NEW_BAL_AMT.BASEL_ACCT_ID
    WHERE
      TRIM(TREATMENT_F.TREATMENT_F) = 'A'
      AND TRIM(UPPER(PIT_STATUS.PIT_STATUS_ACCOUNT)) IN ('CUR')
      AND UPPER(STEP_SUB_PORT.STEP_SUB_PORT) = 'STANDALONE_HELOC'
    """,
):
    pass


def get_segment(pool="duckdb_pool", pool_slots=16):
    context = get_current_context()
    input_file = context["ti"].xcom_pull(
        task_ids="segmented__standalone_heloc_lgdnd.export_acct_list", key="parquet"
    )
    output_file = _get_segment(
        f"""{MODEL}_segmentation_config.csv""",
        input_file,
    )
    context["ti"].xcom_push(key="parquet", value=output_file)


def duckdb_delete(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        delete from {DOWNSTREAM_ASSET} where obsn_dt = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' and trim(upper(model)) = trim(upper('{MODEL}')) and trim(upper(stream)) = trim(upper('{{{{ dag.dag_id.rsplit("_",1)[0].upper() }}}}'))
    """,
):
    pass


def duckdb_load(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        INSERT INTO {DOWNSTREAM_ASSET} by name
        FROM (
            select *, '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' as OBSN_DT, '{MODEL}' as MODEL, '{{{{ dag.dag_id.rsplit("_",1)[0].upper() }}}}' as STREAM from '{{{{ task_instance.xcom_pull(task_ids="segmented__standalone_heloc_lgdnd.get_segment", key="parquet") }}}}'
        )
    """,
):
    pass
