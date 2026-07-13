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

MODEL = "itl_lgdnd"

DAG_PARAMS = {
    "mth_tm_id": Param("", type=["null", "string"], description="MTH_TM_ID"),
    "rundate": Param("", type=["null", "string"], description="RUNDATE"),
}

UPSTREAM_ASSET = [
    "ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT",
    "features.SUB_PORT_F",
    "features.PIT_STATUS_ACCOUNT",
    "features.MODEL_EXCL_F_V2",
    "features.TREATMENT_F",
    "features.WRITTEN_OUT_F",
]

DOWNSTREAM_ASSET = f"models.{MODEL.upper()}_SCORE"

DEPENDENCIES = {
    "export_acct_list": ["export_acct_dv"],
    "export_acct_dv": ["get_score"],
    "get_score": ["duckdb_delete"],
    "duckdb_delete": ["duckdb_load"],
}


# BASEL_PSNL_LOAN_ACCT_DRVD_VARS_2
def export_acct_list(
    duckdb_conn_id="duckdb-conn",
    sql=rf"""
        SELECT
            SS.BASEL_ACCT_ID,
            CASE
                WHEN SS.prim_basel_cust_id <= 0 THEN NULL
                ELSE SS.prim_basel_cust_id
            END AS BASEL_CUST_ID
        FROM
            {UPSTREAM_ASSET[0]} SS
            LEFT JOIN {UPSTREAM_ASSET[1]} A ON A.BASEL_ACCT_ID = SS.BASEL_ACCT_ID
            LEFT JOIN {UPSTREAM_ASSET[2]} B ON B.BASEL_ACCT_ID = SS.BASEL_ACCT_ID
            LEFT JOIN {UPSTREAM_ASSET[3]} C ON C.BASEL_ACCT_ID = SS.BASEL_ACCT_ID
            LEFT JOIN {UPSTREAM_ASSET[4]} D ON D.BASEL_ACCT_ID = SS.BASEL_ACCT_ID
            LEFT JOIN {UPSTREAM_ASSET[5]} E ON E.BASEL_ACCT_ID = SS.BASEL_ACCT_ID
        WHERE
            SS.MTH_TM_ID = {{{{task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id")}}}}
            AND SS.RECD_STAT_CD = 4
            AND A.SUB_PORT_F = 'INDIRECT'
            AND A.OBSN_DT = '{{{{task_instance.xcom_pull(task_ids="handle_month_context", key="rundate")}}}}'
            AND B.PIT_STATUS_ACCOUNT = 'CUR'
            AND B.OBSN_DT = '{{{{task_instance.xcom_pull(task_ids="handle_month_context", key="rundate")}}}}'
            AND C.MODEL_EXCL_F_V2 = 'N'
            AND C.OBSN_DT = '{{{{task_instance.xcom_pull(task_ids="handle_month_context", key="rundate")}}}}'
            AND D.TREATMENT_F = 'A'
            AND D.OBSN_DT = '{{{{task_instance.xcom_pull(task_ids="handle_month_context", key="rundate")}}}}'
            AND E.WRITTEN_OUT_F = 'N'
            AND E.OBSN_DT = '{{{{task_instance.xcom_pull(task_ids="handle_month_context", key="rundate")}}}}'
    """,
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
        task_ids=f"scored__itl_lgdnd.export_acct_dv", key="parquet"
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
        delete from {DOWNSTREAM_ASSET} where obsn_dt = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' and trim(upper(model)) = trim(upper('{MODEL}')) and trim(upper(stream)) = trim(upper('{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}'))
    """,
):
    pass


def duckdb_load(
    duckdb_conn_id="duckdb-conn",
    sql=rf"""
        INSERT INTO {DOWNSTREAM_ASSET} by name
        FROM (
            select *, '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' as OBSN_DT, '{MODEL}' as MODEL, upper(trim('{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}')) as STREAM from '{{{{ task_instance.xcom_pull(task_ids="scored__itl_lgdnd.get_score", key="parquet") }}}}'
        )
    """,
):
    pass

