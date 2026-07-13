import os
from datetime import timedelta
import pendulum

from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import _pull_asset_event_extras, _push_asset_event_extras


UPSTREAM_ASSET = [ 
    'ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT',
    'features.PRD_CD',
    'features.SUB_PRD_CD',
    'features.HELOC_F',

    'features.REVISED_EXPSR_OV_125K_F',
    'features.BASEL_PRD_CD',

    'reference.BASEL_RPTG_PRD_LKP',
    'reference.BASEL_EGL_LKP_NZ ',

    'instruments.PD_BASEL_SEG_NUM',
    'instruments.LGD_BASEL_SEG_NUM',
    'instruments.EAD_BASEL_SEG_NUM',

    'ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT',
    'features.PRD_ID',
    'reference.PSNL_LOAN_RPTG_PRD_LKP',

    'ingestion.MORT_MTH_SNAPSHOT',
    'features.BULK_IND',
    'reference.MORT_RPTG_PRD_LKP',

    'ingestion.TNG_ACCT_MO',
    'ingestion.BASEL_ACCT_DIM',
    'features.SRC_SYS_CD',
   
    ]
DOWNSTREAM_ASSET = "instruments.CCAR_BASEL_PRD_TP_NM"
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
    config_file="ccar_basel_prd_tp_nm.export_result.sql",
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
            CCAR_BASEL_PRD_TP_NM,
            STREAM 
            FROM 
            read_parquet('{{{{ task_instance.xcom_pull(task_ids="fact__ccar_basel_prd_tp_nm.export_result", key="parquet") }}}}')    
        )
    """   
):
    pass