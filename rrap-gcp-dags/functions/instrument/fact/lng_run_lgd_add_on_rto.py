from airflow.sdk import get_current_context
from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

UPSTREAM_ASSET = ["ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT",
                  "ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT",
                  "features.BASEL_ACCT_ID",
                  "instruments.PRPTY_VAL_CORR_PCTG",
                  "instruments.CRNT_LTV_RTO",
                  "instruments.DLGD_F",
                  "instruments.INDEXED_CCLTV_RTO",
                  "features.PRD_ID",
                  "features.ACCT_SENRTY_CD",
                  "features.NOTE_DT",
                  "reference.BASEL_SEG",
                  "reference.BASEL_SEG_RPTG_PARM",
                  "instruments.PIT_STAT_CD",
                  "ingestion.TM_ID"]

DOWNSTREAM_ASSET = "instruments.LNG_RUN_LGD_ADD_ON_RTO"

DEPENDENCIES = {
    "export_ks": ["duckdb_clear"],
    "export_spl": ["duckdb_clear"],
    "export_mor_tng": ["duckdb_clear"],
    "duckdb_clear": ["duckdb_load"],
}

def export_ks(
    duckdb_conn_id="duckdb-conn",
    config_file="lng_run_lgd_add_on_rto.export_ks.sql",
    config_type="instrument",
):
    pass

def export_spl(
    duckdb_conn_id="duckdb-conn",
    config_file="lng_run_lgd_add_on_rto.export_spl.sql",
    config_type="instrument",
):
    pass

def export_mor_tng(
    duckdb_conn_id="duckdb-conn",
    config_file="lng_run_lgd_add_on_rto.export_mor_tng.sql",
    config_type="instrument",
):
    pass

def duckdb_clear(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    DELETE FROM { DOWNSTREAM_ASSET } 
    WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    AND STREAM =  '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}'
    
    """
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
            read_parquet([
            '{{{{ task_instance.xcom_pull(task_ids="fact__lng_run_lgd_add_on_rto.export_ks", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="fact__lng_run_lgd_add_on_rto.export_mor_tng", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="fact__lng_run_lgd_add_on_rto.export_spl", key="parquet") }}}}'], union_by_name=true)
    )
    """
):
    pass