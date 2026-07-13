from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)


UPSTREAM_ASSET = ["features.AT34"]
DOWNSTREAM_ASSET = "features.AT34_SLP6M"
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
                SELECT BASEL_CUST_ID, AT34 FROM features.AT34
                WHERE OBSN_DT = DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
                    AND BASEL_CUST_ID != -1
            ), prev AS (
                SELECT BASEL_CUST_ID, AT34 FROM features.AT34
                WHERE OBSN_DT = LAST_DAY(DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' - INTERVAL 5 MONTH)
                    AND BASEL_CUST_ID != -1
            ), s AS (
                SELECT BASEL_CUST_ID,
                    SUM(COALESCE(AT34, 0)) AS SUM_AT34
                FROM features.AT34
                WHERE OBSN_DT BETWEEN LAST_DAY(DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' - INTERVAL 4 MONTH)
                    AND LAST_DAY(DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'- INTERVAL 1 MONTH)
                    AND BASEL_CUST_ID != -1
                GROUP BY BASEL_CUST_ID
            )
            SELECT 
                '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
                curr.BASEL_CUST_ID,
                CASE
                    WHEN prev.AT34 IS NULL OR curr.AT34 IS NULL THEN NULL
                    WHEN prev.AT34 = 0 AND curr.AT34 = 0 THEN 0
                    WHEN prev.AT34 = 0 AND s.SUM_AT34 = 0 AND curr.AT34 != 0 THEN curr.AT34
                    ELSE (curr.AT34 - prev.AT34) / 5
                END AS AT34_SLP6M
            FROM curr
            JOIN prev on curr.BASEL_CUST_ID = prev.BASEL_CUST_ID
            LEFT JOIN s on prev.BASEL_CUST_ID = s.BASEL_CUST_ID
        )
    """,
):
    pass


