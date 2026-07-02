import os
from datetime import timedelta
import pendulum

from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import _pull_asset_event_extras, _push_asset_event_extras


UPSTREAM_ASSET = [ 'features.DLQNT_DAY']
DOWNSTREAM_ASSET = "features.DLQNT_DAY_AVG24M"
DEPENDENCIES = {
    'duckdb_clear_derive_dlqnt_dayavg24m': ['duckdb_derive_dlqnt_dayavg24m'],
}


def duckdb_clear_derive_dlqnt_dayavg24m(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    DELETE FROM { DOWNSTREAM_ASSET } 
    WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass

def duckdb_derive_dlqnt_dayavg24m(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} 
    with BASE as (
        select BASEL_ACCT_ID, 
        AVG(DLQNT_DAY) AS DLQNT_DAY_AVG24M
        from  {UPSTREAM_ASSET[0]}
        WHERE OBSN_DT BETWEEN
            DATE_ADD(DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}', -INTERVAL 23 MONTH) AND
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
         group by BASEL_ACCT_ID
        
        )
    select BASEL_ACCT_ID,  
    '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' as OBSN_DT, 
    DLQNT_DAY_AVG24M 
    from BASE
    """

):
    pass

