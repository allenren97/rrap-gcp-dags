import pyarrow as pa
from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

UPSTREAM_ASSET = [ 
    "ingestion.BASEL_ACCT_DIM",
    "ingestion.TNG_ACCT_MO",
    "features.TNG_PIT_STATUS_CD",]
DOWNSTREAM_ASSET = 'features.END_PRINCIPAL_BALANCE'
DEPENDENCIES = {
    'duckdb_clear_end_principal_balance': ['duckdb_derive_end_principal_balance'],
}


def duckdb_clear_end_principal_balance(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
        DELETE FROM { DOWNSTREAM_ASSET } 
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass


def duckdb_derive_end_principal_balance(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    INSERT INTO { DOWNSTREAM_ASSET }
    BY NAME FROM (
        WITH ACCT_LIST AS (SELECT
            a.ACCOUNT_ID,
            b.basel_acct_id
        FROM {UPSTREAM_ASSET[1]}  a
        INNER JOIN {UPSTREAM_ASSET[0]}  b ON
            a.ACCOUNT_ID = b.SRC_APP_ID
            AND b.SRC_APP_CD ='TNG-MOR'
            AND b.SRC_SYS_DEL_F != 'Y'
        INNER JOIN {UPSTREAM_ASSET[2]}  c ON
            c.TNG_PIT_STATUS_CD != 'CLO'
            AND a.ACCOUNT_ID = c.ACCOUNT_ID
            AND a.MONTH_END_DT = c.OBSN_DT
        WHERE
            a.month_end_dt = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
        )

        SELECT 
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            A.end_principal_balance,
            B.basel_acct_id
        FROM
            {UPSTREAM_ASSET[1]} A
        INNER JOIN
            ACCT_LIST B
        ON
            A.ACCOUNT_ID = B.ACCOUNT_ID
        WHERE
            a.month_end_dt = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    )
    """
):
    pass

