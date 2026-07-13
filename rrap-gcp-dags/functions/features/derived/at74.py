from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)


UPSTREAM_ASSET = ["ingestion.CR_BUREAU_DELI_MTH_SNAPSHOT", ]
DOWNSTREAM_ASSET = "features.AT74"
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
                CAST(TRADE_WORST_EVER_90_DAY_DLQNT_CNT AS INTEGER) AS AT74
            FROM
                ingestion.CR_BUREAU_DELI_MTH_SNAPSHOT
            WHERE
                MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
        )
    """,
):
    pass


