from airflow.sdk import get_current_context
from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

UPSTREAM_ASSET = ["features.MAX_LEND_VALUE"]
DOWNSTREAM_ASSET = "features.STEP_TOT_LEND_VALUE_ACCT"
DEPENDENCIES = {
    "duckdb_delete_step_tot_lend_value_acct": ["duckdb_load_step_tot_lend_value_acct"],
}


def duckdb_delete_step_tot_lend_value_acct(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        DELETE FROM {DOWNSTREAM_ASSET}
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """,
):
    pass

def duckdb_load_step_tot_lend_value_acct(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} BY NAME
    FROM (
        with base as (
        SELECT
        STEP_PLN_AGRMNT_NUM,
        MAX(MAX_LEND_VALUE) AS STEP_TOT_LEND_VALUE_ACCT
        FROM {UPSTREAM_ASSET[0]}
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
        GROUP BY STEP_PLN_AGRMNT_NUM
        )
        select 
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' as OBSN_DT,
        STEP_PLN_AGRMNT_NUM,
        STEP_TOT_LEND_VALUE_ACCT
        FROM base
    )
    """
):
    pass



