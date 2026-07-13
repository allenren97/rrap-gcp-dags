import os
from datetime import timedelta
import pendulum

from airflow.sdk import get_current_context

UPSTREAM_ASSET = ['features.CUST32']
DOWNSTREAM_ASSET = "features.CUST32_MIN6M"
DEPENDENCIES = {
    'duckdb_clear_cust32min6m': ['duckdb_derive_cust32min6m'],
}


def duckdb_clear_cust32min6m(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    DELETE FROM { DOWNSTREAM_ASSET } 
    WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """,
):
    pass

def duckdb_derive_cust32min6m(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} BY NAME
    WITH base AS (
        SELECT 
            BASEL_CUST_ID,
            MIN(CUST32) AS CUST32_MIN6M
        FROM 
            {UPSTREAM_ASSET[0]}
        WHERE 
            OBSN_DT BETWEEN 
                DATE_ADD(DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}', - INTERVAL 5 MONTH)
                AND
                '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
        GROUP BY 
            BASEL_CUST_ID
    )
    SELECT 
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate")  }}}}' AS OBSN_DT,
        BASEL_CUST_ID, 
        CUST32_MIN6M 
    FROM base
    """,
):
    pass

