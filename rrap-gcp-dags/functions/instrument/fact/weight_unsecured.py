import os
from datetime import timedelta
import pendulum

from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import _pull_asset_event_extras, _push_asset_event_extras


UPSTREAM_ASSET = [ 
      'instruments.EXPOSURE_UNSECURED',
      'instruments.EXPOSURE_SECURED_MAXIMUM'
    ]
DOWNSTREAM_ASSET = "instruments.WEIGHT_UNSECURED"
DEPENDENCIES = {
    'duckdb_clear': ['export_result'],
    'export_result': ['duckdb_load'],
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
    config_file="weight_unsecured.export_result.sql",
    config_type="instrument",
):
    pass

def duckdb_load(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} BY NAME
    FROM (
        SELECT 
            OBSN_DT, 
            BASEL_ACCT_ID,
            WEIGHT_UNSECURED,
            STREAM 
            from
            read_parquet(
                '{{{{ task_instance.xcom_pull(task_ids="fact__weight_unsecured.export_result", key="parquet") }}}}' 
            )
        )
    """   
):
    pass