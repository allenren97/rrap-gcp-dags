from airflow.sdk import get_current_context
 
from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)
 
 
UPSTREAM_ASSET = ["features.UTIL_KSA"]
DOWNSTREAM_ASSET = "features.UTIL_KSA_MAX12M"
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
            WITH tmp AS (
                SELECT 
                    MAX(UTIL_KSA) AS UTIL_KSA_MAX12M,
                    BASEL_ACCT_ID
                FROM 
                    features.UTIL_KSA
                WHERE OBSN_DT BETWEEN DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' - INTERVAL 11 MONTH
                    AND DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
                GROUP BY BASEL_ACCT_ID
            )
            SELECT 
                '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
                util.BASEL_ACCT_ID,
                TRUNC(UTIL_KSA_MAX12M, 5) AS UTIL_KSA_MAX12M
            FROM features.UTIL_KSA util
            LEFT JOIN tmp ON util.BASEL_ACCT_ID = tmp.BASEL_ACCT_ID
            WHERE util.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
        )
    """,
):
    pass
 
 
