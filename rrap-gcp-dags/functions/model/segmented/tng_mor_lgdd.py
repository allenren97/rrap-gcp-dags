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

MODEL = "tng_mor_lgdd"

UPSTREAM_ASSET = [
    "ingestion.BASEL_ACCT_DIM",
    "ingestion.TNG_ACCT_MO",
    "features.TNG_PIT_STATUS_CD",
    "features.MONTH_DEF_SINCE_LAST_DEF",
    "features.BALANCE_INTEREST",
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
    sql=f"""
SELECT
    null as BASEL_CUST_ID,
    b.BASEL_ACCT_ID,
    SUBSTR(a.account_id, 1, 3) as MTG_PROVIDER,
    d.MONTH_DEF_SINCE_LAST_DEF,
    e.BALANCE_INTEREST
FROM {UPSTREAM_ASSET[1]} a
INNER JOIN {UPSTREAM_ASSET[0]} b ON
    a.ACCOUNT_ID = b.SRC_APP_ID
    AND b.SRC_APP_CD ='TNG-MOR'
    AND b.SRC_SYS_DEL_F != 'Y'
INNER JOIN {UPSTREAM_ASSET[2]} c ON
    a.ACCOUNT_ID = c.ACCOUNT_ID
    AND a.MONTH_END_DT = c.OBSN_DT
LEFT OUTER JOIN {UPSTREAM_ASSET[3]} d ON
    b.BASEL_ACCT_ID = d.BASEL_ACCT_ID
    AND a.MONTH_END_DT = d.OBSN_DT
LEFT OUTER JOIN {UPSTREAM_ASSET[4]} e ON
    b.BASEL_ACCT_ID = e.BASEL_ACCT_ID
    AND a.MONTH_END_DT = e.OBSN_DT
WHERE
    a.month_end_dt = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    AND c.TNG_PIT_STATUS_CD = 'DEF'
GROUP BY
    b.basel_acct_id,
    SUBSTR(a.account_id, 1, 3),
    d.MONTH_DEF_SINCE_LAST_DEF,
    e.BALANCE_INTEREST
    """,
):
    pass


def export_segment_input(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        select b.*, null as SCORE
        from '{{{{ task_instance.xcom_pull(task_ids="segmented__tng_mor_lgdd.export_acct_list", key="parquet") }}}}' b
    """,
):
    pass


def get_segment(pool="duckdb_pool", pool_slots=16):
    context = get_current_context()
    input_file = context["ti"].xcom_pull(
        task_ids=f"segmented__tng_mor_lgdd.export_segment_input", key="parquet"
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
            select *, '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' as OBSN_DT, '{MODEL}' as MODEL, '{{{{ dag.dag_id.rsplit("_",1)[0].upper() }}}}' as STREAM from '{{{{ task_instance.xcom_pull(task_ids="segmented__tng_mor_lgdd.get_segment", key="parquet") }}}}'
        )
    """,
):
    pass
