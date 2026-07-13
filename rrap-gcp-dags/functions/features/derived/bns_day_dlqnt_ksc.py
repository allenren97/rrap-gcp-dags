from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)


UPSTREAM_ASSET = [
    "ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT",
]
DOWNSTREAM_ASSET = "features.BNS_DAY_DLQNT_KSC"
DEPENDENCIES = {
    "duckdb_delete": ["duckdb_load"],
}


def duckdb_delete(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    delete from {DOWNSTREAM_ASSET} where obsn_dt = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """,
):
    pass

def duckdb_load(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} by name
    FROM (
        SELECT
            PRIM_BASEL_CUST_ID AS BASEL_CUST_ID,
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS obsn_dt,
            CAST(BNS_DLQNT_DAY AS INTEGER) AS BNS_DAY_DLQNT_KSC
        FROM
            ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT
        WHERE
            mth_tm_id = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    )
    """,
):
    pass


