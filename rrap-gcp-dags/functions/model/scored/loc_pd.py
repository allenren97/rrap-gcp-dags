from bns.rrap.enums import ModelConfig
from airflow.sdk import get_current_context, Param

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

from bns.rrap.helpers.model_utility import (
    _get_upstream_assets,
    _get_input_sql,
    _get_score,
)

MODEL = "loc_pd"


DAG_PARAMS = {
    "mth_tm_id": Param("", type=["null", "string"], description="MTH_TM_ID"),
    "rundate": Param("", type=["null", "string"], description="RUNDATE"),
}

UPSTREAM_ASSET = [
    "features.BASEL_PRD_CD",
    "features.CONSM_SCORECRD_EXCLSN_F",
    "features.HELOC_F",
    "features.PIT_STATUS_CROSS_DEFAULT_ORIG",
    "features.CONSM_PRD_TREATMNT_CD",
]

DOWNSTREAM_ASSET = f"models.{MODEL.upper()}_SCORE"

DEPENDENCIES = {
    "export_acct_list": ["export_acct_dv"],
    "export_acct_dv": ["get_score"],
    "get_score": ["duckdb_delete"],
    "duckdb_delete": ["duckdb_load"],
}


def export_acct_list(
    duckdb_conn_id="duckdb-conn",
    config_file=f"{ModelConfig.SCORE_POPN_SUBDIR.value}/{MODEL}_popn.sql",
    config_type="model_population"
):
    pass

def export_acct_dv(
    duckdb_conn_id="duckdb-conn",
    config_file=f"{MODEL}_scoring_config.csv",
    config_type="model_dv",
):
    pass


def get_score(pool="duckdb_pool", pool_slots=16):
    context = get_current_context()
    input_file = context["ti"].xcom_pull(
        task_ids=f"scored__loc_pd.export_acct_dv", key="parquet"
    )
    stream = (
        context["ti"].xcom_pull(task_ids="handle_month_context", key="stream").lower()
    )
    output_file = _get_score(
        filename=f"{MODEL}_scoring_config.csv",
        parquet=input_file,
        stream=stream,
    )
    context["ti"].xcom_push(key="parquet", value=output_file)    


def duckdb_delete(
    duckdb_conn_id="duckdb-conn",
    sql=rf"""
        delete from {DOWNSTREAM_ASSET} where obsn_dt = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' and trim(upper(model)) = trim(upper('{MODEL}')) and trim(upper(stream)) = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}'
    """,
):
    pass


def duckdb_load(
    duckdb_conn_id="duckdb-conn",
    sql=rf"""
        INSERT INTO {DOWNSTREAM_ASSET} by name
        FROM (
            select *, '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' as OBSN_DT, '{MODEL}' as MODEL, '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}' as STREAM from '{{{{ task_instance.xcom_pull(task_ids="scored__loc_pd.get_score", key="parquet") }}}}'
        )
    """,
):
    pass
