import os
from datetime import timedelta
import pendulum

from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import _pull_asset_event_extras, _push_asset_event_extras


UPSTREAM_ASSET = [ 
        'ingestion.TNG_ACCT_MO',
        'ingestion.BASEL_ACCT_DIM',
        'ingestion.MORT_MTH_SNAPSHOT',
        'ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT',
        'ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT',

        'instruments.DLGD_F',
        'features.SCRTY_TP_DESC',
        'instruments.LNG_RUN_LGD_ADD_ON_RTO',
        'instruments.LGD_FLR',
        'instruments.UNINSURED_LGD_RTO'
        'instruments.PMI_LGD_INSURED_RPTG_RTO'
        'instruments.PMI_LGD_UNADJUSTED_RPTG_RTO'
    ]



DOWNSTREAM_ASSET = "instruments.UNINSURED_DLGD_RTO"

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
    config_file="uninsured_dlgd_rto.export_result.sql",
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
            UNINSURED_DLGD_RTO,
            STREAM 
            from
            read_parquet(
            '{{{{ task_instance.xcom_pull(task_ids="fact__uninsured_dlgd_rto.export_result", key="parquet") }}}}'
            )    
        )
    """   
):
    pass