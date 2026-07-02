from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)


UPSTREAM_ASSET = ["features.GO11"]
DOWNSTREAM_ASSET = "features.GO11_SLP6M"
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
            WITH curr AS (
                SELECT BASEL_CUST_ID, GO11 FROM features.GO11
                WHERE OBSN_DT = DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
                    AND BASEL_CUST_ID != -1
            ), prev AS (
                SELECT BASEL_CUST_ID, GO11 FROM features.GO11
                WHERE OBSN_DT = LAST_DAY(DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' - INTERVAL 5 MONTH)
                    AND BASEL_CUST_ID != -1
            ), s AS (
                SELECT BASEL_CUST_ID,
                    SUM(COALESCE(GO11, 0)) AS SUM_GO11
                FROM features.GO11
                WHERE OBSN_DT BETWEEN LAST_DAY(DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' - INTERVAL 4 MONTH)
                    AND DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' - INTERVAL 1 MONTH
                    AND BASEL_CUST_ID != -1
                GROUP BY BASEL_CUST_ID
            )
            SELECT 
                '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
                curr.BASEL_CUST_ID,
                CASE
                    WHEN prev.GO11 IS NULL OR curr.GO11 IS NULL THEN NULL
                    WHEN prev.GO11 = 0 AND curr.GO11 = 0 THEN 0
                    WHEN prev.GO11 = 0 AND s.SUM_GO11 = 0 AND curr.GO11 != 0 THEN curr.GO11
                    ELSE (curr.GO11 - prev.GO11) / 5
                END AS GO11_SLP6M
            FROM curr
            JOIN prev on curr.BASEL_CUST_ID = prev.BASEL_CUST_ID
            LEFT JOIN s on prev.BASEL_CUST_ID = s.BASEL_CUST_ID
        )
    """,
):
    pass


