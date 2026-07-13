from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)


UPSTREAM_ASSET = [
    "ingestion.MORT_MTH_SNAPSHOT",
]
DOWNSTREAM_ASSET = "features.TOTAL_BALANCE"
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
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            basel_acct_id,
            (
                CASE
                    WHEN crnt_bal_amt + intr_accr_amt != 0 THEN (crnt_bal_amt + intr_accr_amt)
                    ELSE coalesce(-tot_susp_bal_amt, 0)
                END
            ) AS TOTAL_BALANCE
        FROM
            ingestion.MORT_MTH_SNAPSHOT
        WHERE
            mth_tm_id = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    )
    """,
):
    pass


