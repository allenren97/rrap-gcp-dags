from airflow.sdk import get_current_context
from bns.rrap.helpers.asset_event import _pull_asset_event_extras, _push_asset_event_extras

UPSTREAM_ASSET = [
    'ingestion.IWF_CUST_ACCT',
    'ingestion.IWF_ACTY_ROLLUP',
    'ingestion.IWD_CHNL',
    'ingestion.CUST_XREF',
    'ingestion.BASEL_CUST_DIM',
]
DOWNSTREAM_ASSET = 'features.NSF_CNT'
DEPENDENCIES = {
    'duckdb_delete': ['duckdb_load'],
}


def duckdb_delete(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
        DELETE FROM {DOWNSTREAM_ASSET}
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass

def duckdb_load(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
        INSERT INTO {DOWNSTREAM_ASSET} BY NAME
        FROM (
            WITH cust_acct_trxn AS (
                SELECT
                    c.CUST_BASE_KEY,
                    c.TIME_KEY,
                    a.ACTY_CNT,
                    d.TXN_GRP_KEY
                FROM ingestion.IWF_ACTY_ROLLUP a
                JOIN ingestion.IWF_CUST_ACCT c
                ON a.ACCT_BASE_KEY = c.ACCT_BASE_KEY AND a.TIME_KEY = c.TIME_KEY
                JOIN ingestion.IWD_CHNL d ON a.DLVY_KEY = d.DLVY_KEY
                WHERE c.TIME_KEY = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
                AND c.SUM_SRVC_CODE IN ('CHQ', 'SAV')
                AND c.ACCT_LCST IN ('A', 'I', 'D')
            ),
            cust_acct_trxn_aggr AS (
                SELECT
                    TIME_KEY,
                    CUST_BASE_KEY,
                    SUM(CASE WHEN TXN_GRP_KEY = 28000 THEN ACTY_CNT ELSE 0 END) AS NSF_CNT
                FROM cust_acct_trxn
                GROUP BY TIME_KEY, CUST_BASE_KEY
            )
            SELECT
                '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
                c.BASEL_CUST_ID,
                a.NSF_CNT
            FROM cust_acct_trxn_aggr a
            JOIN ingestion.CUST_XREF b
            ON a.CUST_BASE_KEY = b.CUST_BASE_KEY
            JOIN ingestion.BASEL_CUST_DIM c
            ON TRIM(b.CUST_ID) = TRIM(c.CUST_CID)
        )
    """
):
    pass


