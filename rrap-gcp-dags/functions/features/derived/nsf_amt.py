from airflow.sdk import get_current_context
from bns.rrap.helpers.asset_event import _pull_asset_event_extras, _push_asset_event_extras

UPSTREAM_ASSET = [
    'ingestion.IWF_CUST_ACCT',
    'ingestion.IWF_CUST_ACTY_RLP',
    'ingestion.CUST_XREF',
    'ingestion.BASEL_CUST_DIM',
    'reference.DLVY_KEY_TO_TXN_GRP_MAPPNG_LKP',
]
DOWNSTREAM_ASSET = 'features.NSF_AMT'
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
                    b.TIME_KEY,
                    b.CUST_BASE_KEY,
                    SUM(d.ACTY_TOT_AMT) as ACTY_TOT_AMT,
                    m.TXN_GRP_KEY AS TXN_GRP_KEY_RM
                FROM ingestion.IWF_CUST_ACCT b
                JOIN ingestion.IWF_CUST_ACTY_RLP d
                    ON b.CUST_BASE_KEY = d.CUST_BASE_KEY AND b.TIME_KEY = d.TIME_KEY AND b.ACCT_KEY = d.ACCT_KEY
                LEFT JOIN reference.DLVY_KEY_TO_TXN_GRP_MAPPNG_LKP m
                    ON d.DLVY_key  = m.DLVY_KEY
                WHERE b.TIME_KEY = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
                    AND b.ACCT_LCST IN ('A', 'I', 'D') AND b.PRIM_CUST_F = 'P'
                GROUP BY b.TIME_KEY, b.CUST_BASE_KEY, b.PRIM_CUST_F, d.DLVY_KEY, m.TXN_GRP_KEY
            ), cust_acct_trxn_aggr AS (
                SELECT
                    TIME_KEY,
                    CUST_BASE_KEY,
                    SUM(CASE WHEN TXN_GRP_KEY_RM = 'N' THEN ACTY_TOT_AMT ELSE 0 END) AS NSF_AMT
                FROM cust_acct_trxn
                GROUP BY TIME_KEY, CUST_BASE_KEY
            )
            SELECT
                '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
                c.BASEL_CUST_ID,
                a.NSF_AMT
            FROM cust_acct_trxn_aggr a
            JOIN ingestion.CUST_XREF b ON a.CUST_BASE_KEY = b.CUST_BASE_KEY 
            JOIN ingestion.BASEL_CUST_DIM c ON b.cust_id = c.cust_cid
        )
    """
):
    pass


