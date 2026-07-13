from airflow.sdk import get_current_context, Param

from bns.rrap.enums import ModelConfig

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

from bns.rrap.helpers.model_utility import (
    _get_upstream_assets,
    _get_input_sql,
    _get_score,
) 

MODEL = "cc_ead"

DAG_PARAMS = {
    "mth_tm_id": Param("", type=["null", "string"], description="MTH_TM_ID"),
    "rundate": Param("", type=["null", "string"], description="RUNDATE"),
} 

UPSTREAM_ASSET = [
    "ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT",
    "features.TREATMENT_F",
    "features.PIT_STATUS_CROSS_DEFAULT_ORIG",
    "features.MODEL_EXCL_F",
    "features.CONSM_SCORECRD_EXCLSN_F",
    "features.SUB_PPLTN_FLAG",
    "features.BASEL_PRD_CD",
    "features.HELOC_F",
    "features.CONSM_PRD_TREATMNT_CD"
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

# def export_handle_null_basel_cust_id(
#     # TODO
#     duckdb_conn_id="duckdb-conn",  
#     sql=f"""select 
#                 BASEL_ACCT_ID, 
#     BASEL_CUST_ID, 
#     HGST_DLQNT_DAY_COMB_MAX24M,
#     BR147_MAX6M,
#     CASE
#      WHEN BASEL_CUST_ID IS NULL THEN NULL
#      WHEN BASEL_CUST_ID = -1 THEN NULL
#      ELSE CSH_AD_CRNT_C_BAL_KSA_SUM6M
#     END AS CSH_AD_CRNT_C_BAL_KSA_SUM6M,
#     CASE
#      WHEN BASEL_CUST_ID IS NULL THEN NULL
#      WHEN BASEL_CUST_ID = -1 THEN NULL
#      ELSE UTIL_KSA_MAX12M
#     END AS UTIL_KSA_MAX12M,
#     AT21_MAX6M,
#     PRCH_CRNT_C_BAL_KSC_MAX3M,
#     SUB_PPLTN_FLAG
#             from 
#                 '{{{{ task_instance.xcom_pull(task_ids="scored__cc_ead.export_acct_dv", key="parquet") }}}}'
#     """
 
# ): 
#     pass

def get_score(pool="duckdb_pool", pool_slots=16):
    context = get_current_context()
    input_file = context["ti"].xcom_pull(
        task_ids=f"scored__cc_ead.export_acct_dv", key="parquet"
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
    sql=f"""
        delete from {DOWNSTREAM_ASSET} where obsn_dt = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' and trim(upper(model)) = trim(upper('{MODEL}')) and trim(upper(stream)) = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}'
    """,
):
    pass


def duckdb_load(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        INSERT INTO {DOWNSTREAM_ASSET} by name
        FROM (
            select *, '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' as OBSN_DT, '{MODEL}' as MODEL, '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}' as STREAM from '{{{{ task_instance.xcom_pull(task_ids="scored__cc_ead.get_score", key="parquet") }}}}'
        )
    """,
):
    pass
