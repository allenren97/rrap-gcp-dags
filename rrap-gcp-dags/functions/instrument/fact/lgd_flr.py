from airflow.sdk import get_current_context
from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

UPSTREAM_ASSET = ["features.BASEL_ACCT_ID",
                    "features.ASST_CL_NUM",
                    "features.CMHC_F",
                    "features.COLLATERAL_TYPE",
                    "features.LGD_S",
                    "features.LGD_U",
                    "instruments.FULLY_SECURED_F",
                    "instruments.WEIGHT_SECURED",
                    "instruments.WEIGHT_UNSECURED",
                    "instruments.EXPOSURE_SECURED_MAXIMUM",
                    "instruments.LGD_FINAL_RPTG_RTO"]

DOWNSTREAM_ASSET = "instruments.LGD_FLR"

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
    config_file="lgd_flr.export_result.sql",
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
            read_parquet('{{{{ task_instance.xcom_pull(task_ids="fact__lgd_flr.export_result", key="parquet") }}}}')    
        )
    """   
):
    pass