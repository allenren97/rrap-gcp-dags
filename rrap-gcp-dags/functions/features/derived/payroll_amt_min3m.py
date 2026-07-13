import pyarrow as pa
from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

UPSTREAM_ASSET = [ 'features.PAYROLL_AMT' ]
DOWNSTREAM_ASSET = 'features.PAYROLL_AMT_MIN3M'
DEPENDENCIES = {
    'duckdb_clear_payroll_amt_min3m': ['duckdb_derive_payroll_amt_min3m'],
}


def duckdb_clear_payroll_amt_min3m(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
        DELETE FROM { DOWNSTREAM_ASSET } 
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass


# TODO: Eventually refactor this to use the merged mortgage_mth_snapshot table.
def duckdb_derive_payroll_amt_min3m(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
    INSERT INTO { DOWNSTREAM_ASSET }
    BY NAME FROM (
        select
            min(PAYROLL_AMT) as payroll_amt_min3m,
            BASEL_CUST_ID,
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT
        from { UPSTREAM_ASSET[0] }
        WHERE OBSN_DT BETWEEN
            DATE_ADD(DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}', -INTERVAL 2 MONTH) AND
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
         group by basel_cust_id
    )
    """
):
    pass

