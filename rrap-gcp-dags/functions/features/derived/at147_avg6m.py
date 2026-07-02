from airflow.sdk import get_current_context
 
from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)
 
 
UPSTREAM_ASSET = [
    "ingestion.CR_BUREAU_DELI_MTH_SNAPSHOT",
    "ingestion.BASEL_CUST_ACCT_RLTNP_SNAPSHOT"
]
DOWNSTREAM_ASSET = "features.AT147_AVG6M"
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
            ), 
            L15 AS (
                -- sum of HIGHST_ACTV_UTLTN per BASEL_CUST_ID over months that exist in 
                -- BASEL_CUST_ACCT_RLTNP_SNAPSHOT within a rolling 6-month window
                SELECT
                    WSS.SUM_HIGHST_ACTV_UTLTN,
                    NSS.BASEL_CUST_ID
                FROM (
                    SELECT
                        A.BASEL_CUST_ID,
                        SUM(A.HIGHST_ACTV_UTLTN) AS SUM_HIGHST_ACTV_UTLTN
                    FROM ingestion.CR_BUREAU_DELI_MTH_SNAPSHOT A
                    JOIN (
                        SELECT DISTINCT BASEL_CUST_ID, MTH_TM_ID
                        FROM ingestion.BASEL_CUST_ACCT_RLTNP_SNAPSHOT
                        WHERE MTH_TM_ID BETWEEN 
                            {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}} - 200
                            AND {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
                    ) B
                        ON A.BASEL_CUST_ID = B.BASEL_CUST_ID
                        AND A.MTH_TM_ID = B.MTH_TM_ID
                    GROUP BY A.BASEL_CUST_ID
                ) AS WSS
                RIGHT JOIN (
                    SELECT BASEL_CUST_ID
                    FROM ingestion.CR_BUREAU_DELI_MTH_SNAPSHOT
                    WHERE MTH_TM_ID BETWEEN 
                        {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}} - 200
                        AND {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
                    GROUP BY BASEL_CUST_ID
                ) AS NSS
                    ON WSS.BASEL_CUST_ID = NSS.BASEL_CUST_ID
            ),
            L26 AS (
                -- count of distinct months per BASEL_CUST_ID in BASEL_CUST_ACCT_RLTNP_SNAPSHOT in the same window
                SELECT
                    BASEL_CUST_ID,
                    COUNT(DISTINCT MTH_TM_ID) AS CNT_BASEL_CUST_ID_26
                FROM ingestion.BASEL_CUST_ACCT_RLTNP_SNAPSHOT
                WHERE MTH_TM_ID BETWEEN 
                    {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}} - 200
                    AND {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
                GROUP BY BASEL_CUST_ID
            )
            SELECT
                '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' as OBSN_DT,
                snap.BASEL_CUST_ID,
                ROUND(L15.SUM_HIGHST_ACTV_UTLTN / L26.CNT_BASEL_CUST_ID_26, 3) AS AT147_AVG6M
            FROM snap
            LEFT JOIN L15 ON snap.BASEL_CUST_ID = L15.BASEL_CUST_ID
            LEFT JOIN L26 ON snap.BASEL_CUST_ID = L26.BASEL_CUST_ID
        )
    """,
):
    pass
 
 
