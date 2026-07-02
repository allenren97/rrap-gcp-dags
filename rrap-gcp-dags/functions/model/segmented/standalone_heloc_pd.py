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

MODEL = "standalone_heloc_pd"

UPSTREAM_ASSET = [
    f"models.{MODEL.upper()}_SCORE",
    "features.PIT_STATUS_ACCOUNT",
    "features.MODEL_EXCL_F",
    "features.WRITTEN_OUT_F",
    "features.TREATMENT_F",
    "features.STEP_SUB_PORT",
    "features.CR_LMT_AMT",
    "features.TOT_NEW_BAL_AMT",
    "ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT",
]
DOWNSTREAM_ASSET = f"models.{MODEL.upper()}_SEGMENT"
DEPENDENCIES = {
    "export_acct_list": ["export_segment_input"],
    "export_segment_input": ["get_segment"],
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
            PIT_STATUS as (
                select PIT_STATUS_ACCOUNT, BASEL_ACCT_ID from features.PIT_STATUS_ACCOUNT where obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
            ),
            MODEL_EXCL_F as (
                select MODEL_EXCL_F, BASEL_ACCT_ID from features.MODEL_EXCL_F where obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
            ),
            WRITTEN_OUT_F as (
                select WRITTEN_OUT_F, BASEL_ACCT_ID from features.WRITTEN_OUT_F where obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
            ),
            TREATMENT_F as (
                select TREATMENT_F, BASEL_ACCT_ID from features.TREATMENT_F where obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
            ),
            CR_LMT_AMT as (
                select CR_LMT_AMT, BASEL_ACCT_ID from features.CR_LMT_AMT where obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
            ),
            TOT_NEW_BAL_AMT as (
                select TOT_NEW_BAL_AMT, BASEL_ACCT_ID from features.TOT_NEW_BAL_AMT where obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
            ),
            STEP_SUB_PORT as (
                select STEP_SUB_PORT, BASEL_ACCT_ID from features.STEP_SUB_PORT where obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
            )
        SELECT
            main.*,
            PIT_STATUS.PIT_STATUS_ACCOUNT,
            TREATMENT_F.TREATMENT_F,
            STEP_SUB_PORT.STEP_SUB_PORT,
            MODEL_EXCL_F.MODEL_EXCL_F,
            WRITTEN_OUT_F.WRITTEN_OUT_F,
            CR_LMT_AMT.CR_LMT_AMT,
            TOT_NEW_BAL_AMT.TOT_NEW_BAL_AMT
        FROM
            rvl AS main
            LEFT JOIN PIT_STATUS ON main.BASEL_ACCT_ID = PIT_STATUS.BASEL_ACCT_ID
            LEFT JOIN TREATMENT_F ON main.BASEL_ACCT_ID = TREATMENT_F.BASEL_ACCT_ID
            LEFT JOIN STEP_SUB_PORT ON main.BASEL_ACCT_ID = STEP_SUB_PORT.BASEL_ACCT_ID
            LEFT JOIN MODEL_EXCL_F ON main.BASEL_ACCT_ID = MODEL_EXCL_F.BASEL_ACCT_ID
            LEFT JOIN WRITTEN_OUT_F ON main.BASEL_ACCT_ID = WRITTEN_OUT_F.BASEL_ACCT_ID
            LEFT JOIN CR_LMT_AMT ON main.BASEL_ACCT_ID = CR_LMT_AMT.BASEL_ACCT_ID
            LEFT JOIN TOT_NEW_BAL_AMT ON main.BASEL_ACCT_ID = TOT_NEW_BAL_AMT.BASEL_ACCT_ID
        WHERE
            TRIM(PIT_STATUS.PIT_STATUS_ACCOUNT) IN ('CUR', 'DEF')
            AND TRIM(TREATMENT_F.TREATMENT_F) = 'A'
            AND UPPER(TRIM(STEP_SUB_PORT.STEP_SUB_PORT)) = 'STANDALONE_HELOC'
    """,
):
    pass


# TODO if big table change upstream
def export_segment_input(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        select b.*, a.SCORE
        from '{{{{ task_instance.xcom_pull(task_ids="segmented__standalone_heloc_pd.export_acct_list", key="parquet") }}}}' b
        left join (select BASEL_ACCT_ID, VAR_SCORE as SCORE from {UPSTREAM_ASSET[0]} where trim(VAR_NAME) = 'SCORE' and obsn_dt = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' and trim(upper(model)) = trim(upper('{MODEL}')) and trim(upper(stream)) = trim(upper('{{{{ dag.dag_id.rsplit("_",1)[0].upper() }}}}'))) a on a.BASEL_ACCT_ID = b.BASEL_ACCT_ID

    """,
):
    pass


def get_segment(pool="duckdb_pool", pool_slots=16):
    context = get_current_context()
    input_file = context["ti"].xcom_pull(
        task_ids="segmented__standalone_heloc_pd.export_segment_input", key="parquet"
    )
    output_file = _get_segment(
        f"""{MODEL}_segmentation_config.csv""",
        input_file,
    )
    context["ti"].xcom_push(key="parquet", value=output_file)


# TODO model name? if big table
def duckdb_delete(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        delete from {DOWNSTREAM_ASSET} where obsn_dt = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' and trim(upper(model)) = trim(upper('{MODEL}')) and trim(upper(stream)) = trim(upper('{{{{ dag.dag_id.rsplit("_",1)[0].upper() }}}}'))
    """,
):
    pass


# TODO model name? if big table
def duckdb_load(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        INSERT INTO {DOWNSTREAM_ASSET} by name
        FROM (
            select *, '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' as OBSN_DT, '{MODEL}' as MODEL, '{{{{ dag.dag_id.rsplit("_",1)[0].upper() }}}}' as STREAM from '{{{{ task_instance.xcom_pull(task_ids="segmented__standalone_heloc_pd.get_segment", key="parquet") }}}}'
        )
    """,
):
    pass
