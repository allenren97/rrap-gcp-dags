import pyarrow as pa
from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

UPSTREAM_ASSET = [ 'features.TOT_NEW_BAL_AMT' ]
DOWNSTREAM_ASSET = 'features.TOT_NEW_BAL_SLP3M'
DEPENDENCIES = {
    'duckdb_clear_TOT_NEW_BAL_SLP3M': ['duckdb_derive_TOT_NEW_BAL_SLP3M'],
}


def duckdb_clear_TOT_NEW_BAL_SLP3M(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
        DELETE FROM { DOWNSTREAM_ASSET } 
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass


def duckdb_derive_TOT_NEW_BAL_SLP3M(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    INSERT INTO { DOWNSTREAM_ASSET }
    BY NAME FROM (
                WITH S4 AS (
                    SELECT
                        S4.BASEL_ACCT_ID,
                        S4.TOT_NEW_BAL_AMT 
                    FROM { UPSTREAM_ASSET[0] } S4
                    WHERE
                        S4.OBSN_DT = DATE_ADD(DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}', -INTERVAL 2 MONTH)
                ),

                SS AS (
                    SELECT
                        SS.BASEL_ACCT_ID,
                        SS.TOT_NEW_BAL_AMT 
                    FROM { UPSTREAM_ASSET[0] } SS
                    WHERE
                        SS.OBSN_DT ='{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
                )

                SELECT 
                    '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
                    A.BASEL_ACCT_ID,
                    CASE 
	                    WHEN COALESCE(A.TOT_NEW_BAL_AMT - B.TOT_NEW_BAL_AMT,0) <> 0 THEN ((A.TOT_NEW_BAL_AMT - B.TOT_NEW_BAL_AMT)/2) 
	                    ELSE 0 
                    END AS TOT_NEW_BAL_SLP3M
                FROM 
                    SS A
                LEFT JOIN
                    S4 B
                ON
                    A.BASEL_ACCT_ID = B.BASEL_ACCT_ID
    )
    """
):
    pass

