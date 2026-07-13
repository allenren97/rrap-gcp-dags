from airflow.sdk import get_current_context
 
from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)
 
 
UPSTREAM_ASSET = ["features.RGSTRD_INVSTMNT_BAL_AMT", "features.NONREGINVBAL"]
DOWNSTREAM_ASSET = "features.INVSTM_ACCT_AMT_INV"
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
        INSERT INTO {DOWNSTREAM_ASSET} BY NAME (
            SELECT
                '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' as OBSN_DT,
                a.BASEL_CUST_ID,
                a.RGSTRD_INVSTMNT_BAL_AMT + b.NONREGINVBAL AS INVSTM_ACCT_AMT_INV
            FROM { UPSTREAM_ASSET[0] } a -- features.RGSTRD_INVSTMNT_BAL_AMT
            INNER JOIN { UPSTREAM_ASSET[1] } b -- features.NONREGINVBAL
            ON a.BASEL_CUST_ID = b.BASEL_CUST_ID
            AND a.OBSN_DT = b.OBSN_DT
            WHERE a.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
        )
    """,
):
    pass
 
 
