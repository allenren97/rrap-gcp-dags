from bns.rrap.enums import ModelConfig
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

MODEL = "loc_pd"

UPSTREAM_ASSET = [
    f"models.{MODEL.upper()}_SCORE",
    "features.BASEL_PRD_CD",
    "features.PIT_STATUS_CROSS_DEFAULT_ORIG",
    "features.CONSM_SCORECRD_EXCLSN_F",
    "features.CONSM_PRD_TREATMNT_CD",
    "features.HELOC_F",
    "ingestion.BASEL_ACCT_PRFM_FACT",
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
    config_file=f"{ModelConfig.SEGMENT_POPN_SUBDIR.value}/{MODEL}_popn.sql", 
    config_type="model_population"
):
    pass


def export_segment_input(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        select b.*, a.SCORE
        from '{{{{ task_instance.xcom_pull(task_ids="segmented__loc_pd.export_acct_list", key="parquet") }}}}' b
        left join (select BASEL_ACCT_ID, VAR_SCORE as SCORE from {UPSTREAM_ASSET[0]} 
        where trim(VAR_NAME) = 'SCORE' 
        and obsn_dt = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' 
        and trim(upper(model)) = trim(upper('{MODEL}')) 
        and trim(upper(stream)) = trim(upper('{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}'))) a on a.BASEL_ACCT_ID = b.BASEL_ACCT_ID

    """,
):
    pass


def get_segment(pool="duckdb_pool", pool_slots=16):
    context = get_current_context()
    input_file = context["ti"].xcom_pull(
        task_ids="segmented__loc_pd.export_segment_input", key="parquet"
    )
    stream = context["ti"].xcom_pull(task_ids="handle_month_context", key="stream").lower()
    output_file = _get_segment(
        f"""{MODEL}_segmentation_config.csv""",
        input_file,
        stream = stream,
    )
    context["ti"].xcom_push(key="parquet", value=output_file)


def duckdb_delete(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        delete from {DOWNSTREAM_ASSET} where obsn_dt = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' 
        and trim(upper(model)) = trim(upper('{MODEL}')) and trim(upper(stream)) = trim(upper('{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}'))
    """,
):
    pass


def duckdb_load(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        INSERT INTO {DOWNSTREAM_ASSET} by name
        FROM (
            select *, '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' as OBSN_DT, '{MODEL}' as MODEL, 
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}' as STREAM from '{{{{ task_instance.xcom_pull(task_ids="segmented__loc_pd.get_segment", key="parquet") }}}}'
        )
    """,
):
    pass