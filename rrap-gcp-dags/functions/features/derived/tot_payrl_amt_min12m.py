import pyarrow as pa
from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

UPSTREAM_ASSET = ['features.TOT_PAYRL_AMT']
DOWNSTREAM_ASSET = 'features.TOT_PAYRL_AMT_MIN12M'
DEPENDENCIES = {
    'duckdb_clear_tot_payrl_amt_min12m': ['duckdb_derive_tot_payrl_amt_min12m'],
}


def duckdb_clear_tot_payrl_amt_min12m(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
        DELETE FROM { DOWNSTREAM_ASSET } 
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass


def duckdb_derive_tot_payrl_amt_min12m(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    INSERT INTO { DOWNSTREAM_ASSET }
    BY NAME FROM (
    SELECT
		'{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        BASEL_CUST_ID,
        MIN(TOT_PAYRL_AMT) AS TOT_PAYRL_AMT_MIN12M
    FROM {UPSTREAM_ASSET[0]}
    WHERE OBSN_DT BETWEEN 
        DATE_ADD(DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}', -INTERVAL 11 MONTH) 
        AND
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    GROUP BY BASEL_CUST_ID
    )
    """
):
    pass

