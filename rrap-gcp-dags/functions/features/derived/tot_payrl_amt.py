import pyarrow as pa
from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

UPSTREAM_ASSET = [
    'ingestion.IWF_CUST_ACCT',
    'ingestion.IWF_ACTY_ROLLUP',
    'ingestion.IWD_CHNL',
    'ingestion.CUST_XREF',
    'ingestion.BASEL_CUST_DIM',
    ]
DOWNSTREAM_ASSET = 'features.TOT_PAYRL_AMT'
DEPENDENCIES = {
    'duckdb_clear_tot_payrl_amt': ['duckdb_derive_tot_payrl_amt'],
}


def duckdb_clear_tot_payrl_amt(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
        DELETE FROM { DOWNSTREAM_ASSET } 
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass


def duckdb_derive_tot_payrl_amt(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    INSERT INTO { DOWNSTREAM_ASSET }
    BY NAME FROM (
    	WITH cust_txn_raw AS (
		SELECT
            b.TIME_KEY,
            b.CUST_BASE_KEY,
            d.DLVY_KEY,
            d.ACTY_TOT_AMT,
            e.DLVY_KEY,
            e.TXN_GRP_KEY
        FROM {UPSTREAM_ASSET[0]} b
        INNER JOIN {UPSTREAM_ASSET[1]} d
          ON b.ACCT_BASE_KEY = d.ACCT_BASE_KEY
         AND b.TIME_KEY = d.TIME_KEY
         AND b.ACCT_KEY = d.ACCT_KEY
        INNER JOIN {UPSTREAM_ASSET[2]} e
        ON d.DLVY_KEY = e.DLVY_KEY
        WHERE b.TIME_KEY = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
          AND b.ACCT_LCST IN ('A','I','D') 
          AND b.SUM_SRVC_CODE IN ('CHQ', 'SAV')
    ),
    cust_txn_aggr AS (
        SELECT
            TIME_KEY,
            CUST_BASE_KEY,
            SUM(CASE 
                WHEN TXN_GRP_KEY IN (20600,18900,21700)
                THEN ACTY_TOT_AMT 
                ELSE 0 
            END) AS TOT_PAYRL_AMT 
        FROM cust_txn_raw
        GROUP BY TIME_KEY, CUST_BASE_KEY
    ),
    cust_id_map AS (
        SELECT x.CUST_BASE_KEY, c.BASEL_CUST_ID
        FROM {UPSTREAM_ASSET[3]} x
        JOIN {UPSTREAM_ASSET[4]} c ON x.CUST_ID = TRIM(c.CUST_CID)
    )
    SELECT
		'{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        b.BASEL_CUST_ID,
        a.TOT_PAYRL_AMT 
    FROM cust_txn_aggr a
    JOIN cust_id_map b ON a.CUST_BASE_KEY = b.CUST_BASE_KEY
    )
    """
):
    pass

