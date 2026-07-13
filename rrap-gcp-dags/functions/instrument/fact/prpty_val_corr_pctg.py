from airflow.sdk import get_current_context
from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

UPSTREAM_ASSET = ["ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT",
        "ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT",
        "ingestion.MORT_MTH_SNAPSHOT",
        "ingestion.TNG_ACCT_MO",
        "ingestion.BASEL_ACCT_DIM",
        "instruments.CRNT_PRPTY_VAL_AMT",
        "instruments.PREV_12_QTR_PRPTY_VAL_AMT",
        "ingestion.TM_DIM",
        "instruments.DLGD_F",
        "features.METRPL_BREACH_F",
        "instruments.INDEX_TERANETV_CMA"]

DOWNSTREAM_ASSET = "instruments.PRPTY_VAL_CORR_PCTG"

DEPENDENCIES = {
    'duckdb_clear': ['export_result', 'export_mor'],
    'export_mor': ['duckdb_load'],
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
    config_file="prpty_val_corr_pctg.export_result.sql",
    config_type="instrument",
):
    pass

def export_mor(
    duckdb_conn_id="duckdb-conn",
    config_file="prpty_val_corr_pctg.export_mor.sql",
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
            SRC_SYS_CD,
            PRPTY_VAL_CORR_PCTG,
            STREAM 
            FROM 
            read_parquet(['{{{{ task_instance.xcom_pull(task_ids="fact__prpty_val_corr_pctg.export_result", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="fact__prpty_val_corr_pctg.export_mor", key="parquet") }}}}'])    
        )
    """   
):
    pass
