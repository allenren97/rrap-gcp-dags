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

MODEL = "mor_lgdd"

DAG_PARAMS = {
    "mth_tm_id": Param("", type=["null", "string"], description="MTH_TM_ID"),
    "rundate": Param("", type=["null", "string"], description="RUNDATE"),
}

UPSTREAM_ASSET = [
    "features.PIT_STATUS_CROSS_DEFAULT_ORIG",
    "ingestion.MORT_MTH_SNAPSHOT",
] + (_get_upstream_assets(f"{MODEL}_scoring_config.csv"))

DOWNSTREAM_ASSET = f"models.{MODEL.upper()}_SCORE"

DEPENDENCIES = {
    "export_acct_list": ["export_acct_dv"],
    "export_acct_dv": ["get_score"],
    "get_score": ["duckdb_delete"],
    "duckdb_delete": ["duckdb_load"],
}

def export_acct_list(
    duckdb_conn_id="duckdb-conn",
    sql=r"""
SELECT
    main.basel_acct_id,
    main.mort_num,
    main.PRIM_BASEL_CUST_ID AS BASEL_CUST_ID
FROM
    (
        SELECT
            basel_acct_id,
            mort_num,
            PRIM_BASEL_CUST_ID
        FROM
            ingestion.MORT_MTH_SNAPSHOT
        WHERE
            mth_tm_id = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}
        GROUP BY
            basel_acct_id,
            mort_num,
            PRIM_BASEL_CUST_ID
    ) AS main
    LEFT JOIN (
        SELECT
            *
        FROM
            features.PIT_STATUS_CROSS_DEFAULT_ORIG
        WHERE
            obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
    ) AS PIT_STATUS ON main.BASeL_ACCT_ID = PIT_STATUS.BASEL_ACCT_ID
WHERE
    TRIM(PIT_STATUS.PIT_STATUS_CROSS_DEFAULT_ORIG) = 'DEF'
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
        task_ids="scored__mor_lgdd.export_acct_dv", key="parquet"
    )
    stream = (context["ti"].xcom_pull(task_ids="handle_month_context", key="stream").lower())
    output_file = _get_score(
        filename=f"{MODEL}_scoring_config.csv",
        parquet=input_file,
        stream=stream,
    )
    context["ti"].xcom_push(key="parquet", value=output_file)


def duckdb_delete(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        delete from {DOWNSTREAM_ASSET} 
        where obsn_dt = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' 
            and trim(upper(model)) = trim(upper('{MODEL}')) 
            and trim(upper(stream)) = trim(upper('{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}'))
    """,
):
    pass


def duckdb_load(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        INSERT INTO {DOWNSTREAM_ASSET} by name
        FROM (
            select *, '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' as OBSN_DT, '{MODEL}' as MODEL, trim(upper('{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}')) as STREAM 
            from '{{{{ task_instance.xcom_pull(task_ids="scored__mor_lgdd.get_score", key="parquet") }}}}'
        )
    """,
):
    pass
