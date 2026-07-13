import os
from datetime import timedelta
import pendulum

from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import _pull_asset_event_extras, _push_asset_event_extras


UPSTREAM_ASSET = [ 
    'instruments.LGD_BASEL_SEG_NUM',
    'instruments.FINAL_RTO'
    ]
DOWNSTREAM_ASSET = "instruments.LGD_FINAL_RPTG_RTO"
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
    config_file="lgd_final_rptg_rto.export_result.sql",
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
            BASEL_ACCT_ID,
            LGD_FINAL_RPTG_RTO,
            STREAM 
            FROM 
            read_parquet('{{{{ task_instance.xcom_pull(task_ids="fact__lgd_final_rptg_rto.export_result", key="parquet") }}}}')    
        )
    """   
):
    pass