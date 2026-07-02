import os
from datetime import timedelta
import pendulum

from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import _pull_asset_event_extras, _push_asset_event_extras


UPSTREAM_ASSET = [ 
        "features.BASEL_ACCT_ID",               #[0]
        "features.BASEL_PRD_TP_CD",             #[1]
        "reference.BASEL_SEG_RPTG_PARM",        #[2]
        "reference.BASEL_SEG",                  #[3]
        "reference.BASEL_MODEL",                #[4]
        "instruments.UNINSURED_LGD_SEG_NUM",    #[5]
        "instruments.LGD_BASEL_SEG_NUM",        #[6]
        "instruments.LGD_MODEL_NM"              #[7]
    ]

DOWNSTREAM_ASSET = "instruments.PMI_LGD_UNADJUSTED_RPTG_RTO"
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
    config_file="pmi_lgd_unadjusted_rptg_rto.export_result.sql",
    config_type="instrument",
):
    pass

def duckdb_load(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} BY NAME
    FROM (
        SELECT 
            *
            FROM 
            read_parquet('{{{{ task_instance.xcom_pull(task_ids="fact__pmi_lgd_unadjusted_rptg_rto.export_result", key="parquet") }}}}')    
        )
    """   
):
    pass