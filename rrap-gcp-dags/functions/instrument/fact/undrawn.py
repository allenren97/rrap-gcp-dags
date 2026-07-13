import os
from datetime import timedelta
import pendulum

from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import _pull_asset_event_extras, _push_asset_event_extras


UPSTREAM_ASSET = ['ingestion.BASEL_ACCT_PRFM_FACT',
                  'ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT',
                  'ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT',
                  'ingestion.MORT_MTH_SNAPSHOT',
                  'features.SRC_SYS_CD',
                  'features.HELOC_F',
                  'features.BASEL_PRD_CD',
                  'features.TOTAL_EXPSR_ABOVE_LMT_F',
                  'ingestion.TNG_ACCT_MO',
                  'ingestion.BASEL_ACCT_DIM',
                  'reference.BASEL_RPTG_PRD_LKP',
                  'reference.BASEL_EGL_LKP_NZ',
                  'features.PRD_ID',
                  'reference.PSNL_LOAN_RPTG_PRD_LKP',
                  'reference.MORT_RPTG_PRD_LKP',
                  'features.BULK_IND',
                  'features.REVISED_EXPSR_AMT',
                  'features.AF_ZERO_NET_UNDRAWN_AMT',
                  'instruments.PD_BAND']

DOWNSTREAM_ASSET = 'instruments.UNDRAWN'
DEPENDENCIES = {
    'export_ks': ['duckdb_clear'],
    'export_spl': ['duckdb_clear'],
    'export_mor': ['duckdb_clear'],
    'export_tng': ['duckdb_clear'],
    'duckdb_clear': ['duckdb_load']
}

def export_ks(
    duckdb_conn_id="duckdb-conn",
    config_file="undrawn.export_ks.sql",
    config_type="instrument",
):
    pass

def export_spl(
    duckdb_conn_id="duckdb-conn",
    config_file="undrawn.export_spl.sql",
    config_type="instrument",
):
    pass

def export_mor(
    duckdb_conn_id="duckdb-conn",
    config_file="undrawn.export_mor.sql",
    config_type="instrument",
):
    pass

def export_tng(
    duckdb_conn_id="duckdb-conn",
    config_file="undrawn.export_tng.sql",
    config_type="instrument",
):
    pass

def duckdb_clear(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
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
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            BASEL_ACCT_ID,
            UNDRAWN,
            SRC_SYS_CD,
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}' AS STREAM
        FROM read_parquet([
            '{{{{ task_instance.xcom_pull(task_ids="fact__undrawn.export_ks", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="fact__undrawn.export_spl", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="fact__undrawn.export_mor", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="fact__undrawn.export_tng", key="parquet") }}}}'
        ], union_by_name=true)
    )
    """
):
    pass
