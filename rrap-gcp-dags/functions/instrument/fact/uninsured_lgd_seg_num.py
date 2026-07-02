import os
from datetime import timedelta
import pendulum

from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import _pull_asset_event_extras, _push_asset_event_extras


UPSTREAM_ASSET = [ 
        'ingestion.MORT_MTH_SNAPSHOT', #0
        'ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT',#1
        'ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT',#2
        'ingestion.TNG_ACCT_MO',#3
        'ingestion.BASEL_ACCT_DIM',#4

        'instruments.LGD_BASEL_SEG_NUM',#5
        'instruments.CCAR_BASEL_PRD_TP_NM', #6
        'instruments.LGD_MODEL_NM',  #7

        'reference.BASEL_MODEL', #8
        'reference.BASEL_SEG_RPTG_PARM',#9
        'reference.BASEL_SEG' #9
    
    ]
DOWNSTREAM_ASSET = "instruments.UNINSURED_LGD_SEG_NUM"
DEPENDENCIES = {
    'duckdb_clear': ['export_mor','export_non_mor'],
    'export_mor':['duckdb_load'],
    'export_non_mor':['duckdb_load'],
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

def export_mor(
    duckdb_conn_id="duckdb-conn",
    config_file="uninsured_lgd_seg_num.export_mor.sql",
    config_type="instrument",
):
    pass

def export_non_mor(
    duckdb_conn_id="duckdb-conn",
    config_file="uninsured_lgd_seg_num.export_non_mor.sql",
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
            UNINSURED_LGD_SEG_NUM,
            STREAM 
            from
            read_parquet([
            '{{{{ task_instance.xcom_pull(task_ids="fact__uninsured_lgd_seg_num.export_mor", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="fact__uninsured_lgd_seg_num.export_non_mor", key="parquet") }}}}'
            ], union_by_name=true)    
        )
    """   
):
    pass