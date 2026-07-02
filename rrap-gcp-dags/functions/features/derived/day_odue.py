from airflow.sdk import get_current_context
from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)


UPSTREAM_ASSET = ["ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT"]
DOWNSTREAM_ASSET = "features.DAY_ODUE"

DEPENDENCIES = {
    "export_day_odue_snapshot": ["duckdb_clear_day_odue"],
    "duckdb_clear_day_odue": ["duckdb_load_day_odue"],
}


def export_day_odue_snapshot(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    SELECT
        m.basel_acct_id AS BASEL_ACCT_ID,
        m.DAY_ODUE AS DAY_ODUE
    FROM ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT AS m
    WHERE m.mth_tm_id = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    """,
):
    pass

def duckdb_clear_day_odue(
    duckdb_conn_id="duckdb-conn",
    sql=rf"""
    DELETE FROM {DOWNSTREAM_ASSET}
    WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """,
):
    pass

def duckdb_load_day_odue(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} BY NAME
    FROM (
      SELECT
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        e.BASEL_ACCT_ID,
        e.DAY_ODUE
      FROM read_parquet('{{{{ task_instance.xcom_pull(task_ids="derived__day_odue.export_day_odue_snapshot", key="parquet") }}}}') e
    )
    """,
):
    pass


