import os
from datetime import timedelta
import pendulum

from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import _pull_asset_event_extras, _push_asset_event_extras


UPSTREAM_ASSET = [ 
    'reference.BASEL_SEG',
    
    ]
DOWNSTREAM_ASSET = "instruments.FINAL_RTO"
DEPENDENCIES = {
    'duckdb_clear': ['export_result'],
    'export_result':['duckdb_load'],
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

def export_result(
    duckdb_conn_id="duckdb-conn",
    config_file="final_rto.export_result.sql",
    config_type="instrument",
):
    pass

def duckdb_load(
    trigger_rule="none_failed_min_one_success",
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} BY NAME
    FROM (
        SELECT 
            OBSN_DT, 
            BASEL_MODEL_ID,  
            SEG_NUM,
            FINAL_RTO, 
            STREAM 
            from 
            read_parquet('{{{{ task_instance.xcom_pull(task_ids="fact__final_rto.export_result", key="parquet") }}}}')    
    )
    """   
):
    pass