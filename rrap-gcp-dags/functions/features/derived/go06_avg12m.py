from airflow.sdk import get_current_context
 
from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)
 
 
UPSTREAM_ASSET = [
    "ingestion.CR_BUREAU_DELI_MTH_SNAPSHOT",
    "ingestion.BASEL_CUST_ACCT_RLTNP_SNAPSHOT"
]
DOWNSTREAM_ASSET = "features.GO06_AVG12M"
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
            L11 AS (
                -- sum of INQRY_PAST_6_MTH_CNT per BASEL_CUST_ID over months that exist in 
                -- BASEL_CUST_ACCT_RLTNP_SNAPSHOT within a rolling 12-month window
                SELECT
                    WSS.SUM_INQRY_PAST_6_MTH_CNT,
                    NSS.BASEL_CUST_ID
                FROM (
                    SELECT
                        A.BASEL_CUST_ID,
                        SUM(A.INQRY_PAST_6_MTH_CNT) AS SUM_INQRY_PAST_6_MTH_CNT
                    FROM ingestion.CR_BUREAU_DELI_MTH_SNAPSHOT A
                    JOIN (
                        SELECT DISTINCT BASEL_CUST_ID, MTH_TM_ID
                        FROM ingestion.BASEL_CUST_ACCT_RLTNP_SNAPSHOT
                        WHERE MTH_TM_ID BETWEEN 
                            {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}} - 440
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
                        {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}} - 440
                        AND {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
                    GROUP BY BASEL_CUST_ID
                ) AS NSS
                    ON WSS.BASEL_CUST_ID = NSS.BASEL_CUST_ID
            ),
            L23 AS (
                -- count of distinct months per BASEL_CUST_ID in BASEL_CUST_ACCT_RLTNP_SNAPSHOT in the same window
                SELECT
                    BASEL_CUST_ID,
                    COUNT(DISTINCT MTH_TM_ID) AS CNT_BASEL_CUST_ID_23
                FROM ingestion.BASEL_CUST_ACCT_RLTNP_SNAPSHOT
                WHERE MTH_TM_ID BETWEEN 
                    {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}} - 440
                    AND {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
                GROUP BY BASEL_CUST_ID
            )
            SELECT
                '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' as OBSN_DT,
                snap.BASEL_CUST_ID,
                ROUND(L11.SUM_INQRY_PAST_6_MTH_CNT / L23.CNT_BASEL_CUST_ID_23, 4) AS GO06_AVG12M
            FROM snap
            LEFT JOIN L11 ON snap.BASEL_CUST_ID = L11.BASEL_CUST_ID
            LEFT JOIN L23 ON snap.BASEL_CUST_ID = L23.BASEL_CUST_ID
        )
    """,
):
    pass
 
 
