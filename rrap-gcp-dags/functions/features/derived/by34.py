from airflow.sdk import get_current_context
from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

UPSTREAM_ASSET = ["ingestion.CR_BUREAU_DELI_MTH_SNAPSHOT", ]
DOWNSTREAM_ASSET = "features.BY34"
DEPENDENCIES = {
    "duckdb_delete_by34": ["duckdb_load_by34"],
}


def duckdb_delete_by34(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        DELETE FROM {DOWNSTREAM_ASSET}
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """,
):
    pass

def duckdb_load_by34(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        INSERT INTO {DOWNSTREAM_ASSET} BY NAME
        FROM (
            SELECT
                '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
                BASEL_CUST_ID,
                TOT_UTLTN_BNK_REVLVNG_LINE_AMT AS BY34
            FROM
                {UPSTREAM_ASSET[0]}
            WHERE
                MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
        )
    """,
):
    pass

