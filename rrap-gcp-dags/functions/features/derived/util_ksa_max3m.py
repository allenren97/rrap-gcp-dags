import pyarrow as pa
from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

UPSTREAM_ASSET = ["features.UTIL_KSA"]
DOWNSTREAM_ASSET = "features.UTIL_KSA_MAX3M"
DEPENDENCIES = {
    'duckdb_clear_util_ksa_max3m': ['duckdb_derive_util_ksa_max3m'],
}


def duckdb_clear_util_ksa_max3m(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
        DELETE FROM { DOWNSTREAM_ASSET } 
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass

def duckdb_derive_util_ksa_max3m(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
        INSERT INTO {DOWNSTREAM_ASSET} BY NAME
        FROM (
            WITH tmp AS (
                SELECT 
                    MAX(UTIL_KSA) AS UTIL_KSA_MAX3M,
                    BASEL_ACCT_ID
                FROM 
                    {UPSTREAM_ASSET[0]}
                WHERE OBSN_DT BETWEEN DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' - INTERVAL 2 MONTH
                    AND DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
                GROUP BY BASEL_ACCT_ID
            )
            SELECT 
                '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
                round(util.BASEL_ACCT_ID,4) as BASEL_ACCT_ID,
                UTIL_KSA_MAX3M
            FROM {UPSTREAM_ASSET[0]} util
            LEFT JOIN tmp ON util.BASEL_ACCT_ID = tmp.BASEL_ACCT_ID
            WHERE util.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
        )
    """,
):
    pass

