import pyarrow as pa
from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

UPSTREAM_ASSET = [ 
    'ingestion.IWD_CUST', 
    'ingestion.CUST_MODEL',
    'ingestion.CUST_XREF',
    'ingestion.BASEL_CUST_DIM'
]

DOWNSTREAM_ASSET = 'features.CUST_ACCT_CNT'

DEPENDENCIES = {
    'duckdb_clear_cust_acct_cnt': ['duckdb_derive_cust_acct_cnt'],
}


def duckdb_clear_cust_acct_cnt(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
        DELETE FROM { DOWNSTREAM_ASSET } 
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass


def duckdb_derive_cust_acct_cnt(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
        INSERT INTO { DOWNSTREAM_ASSET }
        BY NAME FROM (
            WITH LOOKUPXRF_0 AS (
                SELECT
                    A.CUST_ACCT_CNT AS CUST_BASE_KEY,
                    A.POPN_DT,
                    TRIM(C.CUST_ID) AS CUST_ID
                FROM ingestion.IWD_CUST A
                JOIN ingestion.CUST_MODEL B
                ON A.CUST_KEY = B.CUST_KEY
                JOIN ingestion.CUST_XREF C
                ON C.CUST_BASE_KEY = A.CUST_BASE_KEY
                WHERE B.TIME_KEY = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
            ),
            LOOKUPXRF AS (
                SELECT CUST_BASE_KEY, POPN_DT, CUST_ID
                FROM (
                    SELECT
                        L.*,
                        ROW_NUMBER() OVER (
                            PARTITION BY L.CUST_ID
                            ORDER BY L.CUST_BASE_KEY, L.POPN_DT DESC
                        ) AS rn
                    FROM LOOKUPXRF_0 AS L
                ) t
                WHERE rn = 1
            ),
            LOOKUP10 AS (
                SELECT
                    TRIM(CUST_CID) AS CUST_CID,
                    BASEL_CUST_ID
                FROM ingestion.BASEL_CUST_DIM
            )
            SELECT
                '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
                BASEL_CUST_ID,
                CASE
                    WHEN lp10.CUST_CID = '' THEN 0
                    WHEN rr.CUST_BASE_KEY IS NULL THEN 0
                    ELSE CUST_BASE_KEY
                END AS CUST_ACCT_CNT
            FROM LOOKUP10 lp10
            LEFT JOIN LOOKUPXRF rr
            ON TRIM(lp10.CUST_CID) = TRIM(rr.CUST_ID)
        )
    """
):
    pass

