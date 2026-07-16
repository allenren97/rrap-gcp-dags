from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)


UPSTREAM_ASSET = [
    "ingestion.MORT_MTH_SNAPSHOT",
]
DOWNSTREAM_ASSET = "features.CURRENT_BAL"
DEPENDENCIES = {
    "export_mor": ["duckdb_delete"],
    "duckdb_delete": ["duckdb_load"],
}


def export_mor(
    duckdb_conn_id="duckdb-conn",
    sql=rf"""
        SELECT
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            mor.BASEL_ACCT_ID,
            CAST(mor.CRNT_BAL_AMT AS DOUBLE) AS CURRENT_BAL,
            'MOR' AS SRC_SYS_CD
        FROM {UPSTREAM_ASSET[0]} mor
        WHERE mor.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    """,
):
    pass


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
        select * from read_parquet([
            '{{{{ task_instance.xcom_pull(task_ids="derived__current_bal.export_mor", key="parquet") }}}}'],
        union_by_name = true)
    )
    """,
):
    pass
