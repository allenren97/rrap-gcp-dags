from airflow.sdk import get_current_context
 
from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)
 
 
UPSTREAM_ASSET = ["features.BY34"]
DOWNSTREAM_ASSET = "features.BY34_MIN12M"
DEPENDENCIES = {
    "duckdb_delete": ["duckdb_load"],
}


def duckdb_delete(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        DELETE FROM {DOWNSTREAM_ASSET}
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """,
):
    pass


def duckdb_load(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        INSERT INTO {DOWNSTREAM_ASSET} BY NAME
        FROM (
            SELECT
                '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
                BASEL_CUST_ID,
                MIN(BY34) AS BY34_MIN12M
            FROM
                {UPSTREAM_ASSET[0]}    -- features.BY34
            WHERE
                OBSN_DT BETWEEN DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' - INTERVAL 1 YEAR 
                AND '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
            GROUP BY BASEL_CUST_ID
        )
    """,
):
    pass


