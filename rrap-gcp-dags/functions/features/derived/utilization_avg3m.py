import pyarrow as pa
from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

UPSTREAM_ASSET = [ 'features.UTIL_KSA', 
                  'ingestion.BASEL_CUST_ACCT_RLTNP_SNAPSHOT', 
                  'ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT',
                  ]
DOWNSTREAM_ASSET = 'features.UTILIZATION_AVG3M'
DEPENDENCIES = {
    'duckdb_clear_utilization_avg3m': ['duckdb_derive_utilization_avg3m'],
}


def duckdb_clear_utilization_avg3m(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
        DELETE FROM { DOWNSTREAM_ASSET } 
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass


def duckdb_derive_utilization_avg3m(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    INSERT INTO { DOWNSTREAM_ASSET }
    WITH sum AS (
        select
        BASEL_ACCT_ID,
        TRUNC(SUM(UTIL_KSA),4) as UTILIZATION_AVG3M
        from { UPSTREAM_ASSET[0] }
        WHERE OBSN_DT BETWEEN
            DATE_ADD(DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}', -INTERVAL 2 MONTH) AND
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
        group by BASEL_ACCT_ID
    ),
    
    AGE AS (
            SELECT
                BASEL_CUST_ID,
                COUNT(1) AS BASEL_CUSTACCT_Age_3mths
            FROM (
                SELECT DISTINCT
                    A.MTH_TM_ID,
                    A.BASEL_CUST_ID
                FROM 
                    {UPSTREAM_ASSET[1]} A
                WHERE 
                    A.MTH_TM_ID BETWEEN 
                {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}-80
                AND  
                {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
            ) A
            GROUP BY BASEL_CUST_ID
    ),

    ACCT_TO_CUST AS (
        SELECT
            A.*,
            B.PRIM_BASEL_CUST_ID AS BASEL_CUST_ID
        FROM
            sum A
        INNER JOIN
            {UPSTREAM_ASSET[2]} B
        ON
            A.BASEL_ACCT_ID = B.BASEL_ACCT_ID
        WHERE
            B.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    )

    SELECT
    '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
    A.BASEL_ACCT_ID,
    CASE
        WHEN COALESCE(B.BASEL_CUSTACCT_Age_3mths, 0) <> 0 THEN ROUND(A.UTILIZATION_AVG3M/B.BASEL_CUSTACCT_Age_3mths, 4)
        ELSE NULL
    END AS UTILIZATION_AVG3M
    FROM 
        ACCT_TO_CUST A
    INNER JOIN
        AGE B
    ON
        A.BASEL_CUST_ID = B.BASEL_CUST_ID
    """
):
    pass

