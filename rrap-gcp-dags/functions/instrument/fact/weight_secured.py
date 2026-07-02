import os
from datetime import timedelta
import pendulum

from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import _pull_asset_event_extras, _push_asset_event_extras


UPSTREAM_ASSET = [ 
      'instruments.EXPOSURE_SECURED',
      'instruments.EXPOSURE_SECURED_MAXIMUM'
    ]
DOWNSTREAM_ASSET = "instruments.WEIGHT_SECURED"
DEPENDENCIES = {
    'duckdb_clear': ['duckdb_load'],
}



def duckdb_clear(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    DELETE FROM { DOWNSTREAM_ASSET } 
    WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    AND STREAM =  '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}'
    
    """
):
    pass



def duckdb_load(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} BY NAME
    FROM (
            with base as (
                select a.basel_acct_id, EXPOSURE_SECURED,EXPOSURE_SECURED_MAXIMUM  
                from 
                ( 
                    select basel_acct_id, EXPOSURE_SECURED from {UPSTREAM_ASSET[0]} 
                    where obsn_dt='{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' 
                    and stream = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}'
                ) a 
                inner join 
                ( 
                    select basel_acct_id, EXPOSURE_SECURED_MAXIMUM from {UPSTREAM_ASSET[1]} 
                    where obsn_dt='{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
                    and stream = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}'
                ) b
                on a.basel_acct_id=b.basel_acct_id
            ),final as (
                select
                basel_acct_id,
                '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' as OBSN_DT,
                '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}' as STREAM,
                (EXPOSURE_SECURED/EXPOSURE_SECURED_MAXIMUM) as WEIGHT_SECURED
                from base   
            )
                select 
                basel_acct_id,
                obsn_dt,
                stream,
                case 
                    when isnan(WEIGHT_SECURED)then NULL
                    else WEIGHT_SECURED end
                as WEIGHT_SECURED
                from final    
        )
    """   
):
    pass