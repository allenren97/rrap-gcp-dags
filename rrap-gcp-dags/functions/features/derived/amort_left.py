from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _push_asset_event_extras,
)


UPSTREAM_ASSET = [
    "ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT",
]
DOWNSTREAM_ASSET = "features.AMORT_LEFT"
DEPENDENCIES = {
    "duckdb_delete": ["duckdb_load"],
}
CHRG_OFF_LKP = "reference.CHRG_OFF_LKP"


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
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            BASEL_ACCT_ID,
            LOAN_TERM + DATE_DIFF(
                'MONTH', 
                DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}', 
                LAST_DAY(NOTE_DT)
            ) AS AMORT_LEFT
        FROM { UPSTREAM_ASSET[0] }
        WHERE MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    )
    """,
):
    pass
