from airflow.sdk import get_current_context
from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

UPSTREAM_ASSET = ["ingestion.CR_BUREAU_DELI_MTH_SNAPSHOT", ]
DOWNSTREAM_ASSET = "features.AT65"
DEPENDENCIES = {
    "duckdb_delete_at65": ["duckdb_load_at65"],
}


def duckdb_delete_at65(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        DELETE FROM {DOWNSTREAM_ASSET}
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """,
):
    pass

def duckdb_load_at65(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        INSERT INTO {DOWNSTREAM_ASSET} BY NAME
        FROM(
            SELECT
                '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
                BASEL_CUST_ID,
                ACTV_TRADE_OV_90_RT_CNT AS AT65,
                ROW_NUMBER() OVER (
                    PARTITION BY BASEL_CUST_ID, MTH_TM_ID
                    ORDER BY SCORE_LAST_RECVD_DT DESC
                ) AS ROW_NUM
            FROM {UPSTREAM_ASSET[0]}
            WHERE BASEL_CUST_ID <> -1
            AND MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
            AND BASEL_CUST_ID IS NOT NULL
        )
    """,
):
    pass

