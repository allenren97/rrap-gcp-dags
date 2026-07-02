from airflow.sdk import get_current_context
 
from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)
 
 
UPSTREAM_ASSET = [
    "ingestion.CR_BUREAU_DELI_MTH_SNAPSHOT",
    "ingestion.BASEL_CUST_ACCT_RLTNP_SNAPSHOT"
]
DOWNSTREAM_ASSET = "features.AT94_AVG3M"
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
            WITH snap AS (
                SELECT DISTINCT BASEL_CUST_ID
                FROM ingestion.BASEL_CUST_ACCT_RLTNP_SNAPSHOT
                WHERE MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
            ), deli AS (
                SELECT
                    A.BASEL_CUST_ID,
                    SUM(A.TOT_AVL_CR_NOT_UTILIZED_AMT) AS SUM_TOT_AVL_CR_NOT_UTILIZED_AMT
                FROM ingestion.CR_BUREAU_DELI_MTH_SNAPSHOT A
                INNER JOIN (
                    SELECT DISTINCT BASEL_CUST_ID, MTH_TM_ID
                    FROM ingestion.BASEL_CUST_ACCT_RLTNP_SNAPSHOT
                    WHERE MTH_TM_ID BETWEEN 
                        ({{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}} - 80)
                        AND ({{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}})
                ) B
                ON A.BASEL_CUST_ID = B.BASEL_CUST_ID AND A.MTH_TM_ID = B.MTH_TM_ID
                GROUP BY A.BASEL_CUST_ID
            ),
            rltnp AS (
                SELECT
                    BASEL_CUST_ID,
                    COUNT(DISTINCT MTH_TM_ID) AS ACCOUNT_AGE
                FROM ingestion.BASEL_CUST_ACCT_RLTNP_SNAPSHOT
                WHERE MTH_TM_ID BETWEEN 
                    ({{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}} - 80)
                    AND ({{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}})
                GROUP BY BASEL_CUST_ID
            )
            SELECT
                '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
                snap.BASEL_CUST_ID,
                ROUND(CAST(SUM_TOT_AVL_CR_NOT_UTILIZED_AMT * 1.0 / ACCOUNT_AGE AS DECIMAL(20, 6)), 3) AS AT94_AVG3M
            FROM snap
            LEFT JOIN deli ON snap.BASEL_CUST_ID = deli.BASEL_CUST_ID
            LEFT JOIN rltnp ON snap.BASEL_CUST_ID = rltnp.BASEL_CUST_ID
        )
    """,
):
    pass
 
 
