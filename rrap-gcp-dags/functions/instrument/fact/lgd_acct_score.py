import os
from datetime import timedelta
import pendulum

from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import _pull_asset_event_extras, _push_asset_event_extras


UPSTREAM_ASSET = [ 
    'features.BASEL_ACCT_ID',
    'instruments.PIT_STAT_CD',
    
    'models.MOR_LGDND_SCORE',
    'models.MOR_LGDD_SCORE',
    
    'models.CC_LGDND_SCORE',
    
    'models.HELOC_LGDND_SCORE',
    'models.HELOC_LGDD_SCORE',
    
    'models.LOC_LGDND_SCORE',
    'models.LOC_LGDD_SCORE',
    
    'models.DTL_LGDD_SCORE',
    
    'models.ITL_LGDND_SCORE',
    'models.ITL_LGDD_SCORE',

    'models.STEP_MIX_MOR_LGDD_SCORE',
    'models.STEP_MIX_MOR_LGDND_SCORE',

    ]
DOWNSTREAM_ASSET = "instruments.LGD_ACCT_SCORE"
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
    config_file="lgd_acct_score.export_result.sql",
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
            LGD_ACCT_SCORE,
            STREAM 
            FROM 
            read_parquet('{{{{ task_instance.xcom_pull(task_ids="fact__lgd_acct_score.export_result", key="parquet") }}}}')    
        )
    """   
):
    pass