from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)


UPSTREAM_ASSET = [
    "ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT",
]
DOWNSTREAM_ASSET = "features.COMM_F_V2"
DEPENDENCIES = {
    "export_spl": ["duckdb_delete"],
    "duckdb_delete": ["duckdb_load"],
}


def export_spl(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        SELECT
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            BASEL_ACCT_ID,
            TRY_CAST(TRIM(COMM_LOAN_CD) AS INTEGER) AS COMM_F_V2,
            'SPL' AS SRC_SYS_CD
        FROM {UPSTREAM_ASSET[0]}
        WHERE MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
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
            '{{{{ task_instance.xcom_pull(task_ids="derived__comm_f_v2.export_spl", key="parquet") }}}}'],
        union_by_name = true)
    )
    """,
):
    pass
