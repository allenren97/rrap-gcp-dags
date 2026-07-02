import os
from datetime import timedelta
import pendulum

from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import _pull_asset_event_extras, _push_asset_event_extras


UPSTREAM_ASSET = [ 
    "ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT",

    "ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT",

    "ingestion.BASEL_MORT_MTH_SNAPSHOT",

    "ingestion.TNG_ACCT_MO",
    "ingestion.BASEL_ACCT_DIM",

    "instruments.DLGD_F",
    "features.SCRTY_TP_DESC",
    "instruments.LNG_RUN_LGD_ADD_ON_RTO",
    "instruments.PRE_INSURANCE_LGD",
    "instruments.LGD_UNADJUSTED_RPTG_RTO",
    "instruments.LGD_FINAL_RPTG_RTO",
    ]
DOWNSTREAM_ASSET = "instruments.DLGD_FLR"
DEPENDENCIES = {
	'export_spl':['duckdb_clear'],
	'export_ks':['duckdb_clear'],
	'export_mor':['duckdb_clear'],
	'export_tng':['duckdb_clear'],
    'duckdb_clear':['duckdb_load']
}

def export_ks(
    duckdb_conn_id="duckdb-conn",
    config_file="dlgd_flr.export_ks.sql",
    config_type="instrument",
):
    pass

def export_spl(
    duckdb_conn_id="duckdb-conn",
    config_file="dlgd_flr.export_spl.sql",
    config_type="instrument",
):
    pass

def export_mor(
    duckdb_conn_id="duckdb-conn",
    config_file="dlgd_flr.export_mor.sql",
    config_type="instrument",
):
    pass

def export_tng(
    duckdb_conn_id="duckdb-conn",
    config_file="dlgd_flr.export_tng.sql",
    config_type="instrument",
):
    pass

def duckdb_clear(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    DELETE FROM { DOWNSTREAM_ASSET } 
    WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    AND STREAM = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}'
    """
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
            DLGD_FLR,
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}' AS STREAM
        FROM 
            read_parquet([
            '{{{{ task_instance.xcom_pull(task_ids="fact__dlgd_flr.export_ks", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="fact__dlgd_flr.export_tng", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="fact__dlgd_flr.export_spl", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="fact__dlgd_flr.export_mor", key="parquet") }}}}'
            ], union_by_name=true)
        )
    """
):
    pass
