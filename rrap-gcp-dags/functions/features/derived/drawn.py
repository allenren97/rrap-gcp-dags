import os
from datetime import timedelta
import pendulum

from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import _pull_asset_event_extras, _push_asset_event_extras


UPSTREAM_ASSET = [ 
   'features.ADJUSTED_OS_BAL_AMT',
    ]
DOWNSTREAM_ASSET = "features.DRAWN"
DEPENDENCIES = {
    'duckdb_clear': ['duckdb_load']
}


def duckdb_clear(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    DELETE FROM { DOWNSTREAM_ASSET } 
    WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass


def duckdb_load(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} BY NAME
    FROM (
    select
    basel_acct_id, 
    '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' as OBSN_DT,
    case 
        when (ADJUSTED_OS_BAL_AMT) <=0 then 0 
        else (ADJUSTED_OS_BAL_AMT) 
    end
    as DRAWN
    FROM 
    {UPSTREAM_ASSET[0]}
    WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'    )
    """
):
    pass
