import pyarrow as pa
from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

UPSTREAM_ASSET = [ 'features.IND_DELQ' ]
DOWNSTREAM_ASSET = 'features.IND_DELQ_MAX24M'
DEPENDENCIES = {
    'duckdb_clear_ind_delq_max24m': ['duckdb_derive_ind_delq_max24m'],
}


def duckdb_clear_ind_delq_max24m(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
        DELETE FROM { DOWNSTREAM_ASSET } 
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass


def duckdb_derive_ind_delq_max24m(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
    INSERT INTO { DOWNSTREAM_ASSET }
    WITH base AS (
        SELECT 
            BASEL_ACCT_ID,
            MAX(IND_DELQ) AS IND_DELQ_MAX24M
        FROM 
            {UPSTREAM_ASSET[0]}
        WHERE 
            OBSN_DT BETWEEN 
                DATE_ADD(DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}', - INTERVAL 23 MONTH)
                AND
                '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
        GROUP BY 
            BASEL_ACCT_ID
    )
    SELECT 
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate")  }}}}' AS OBSN_DT,
        BASEL_ACCT_ID, 
        IND_DELQ_MAX24M 
    FROM base
    """
):
    pass

