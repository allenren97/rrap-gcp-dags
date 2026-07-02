import os
from datetime import timedelta
import pendulum

from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import _pull_asset_event_extras, _push_asset_event_extras


UPSTREAM_ASSET = [
    'ingestion.SPL_SOURCE_FILE_AMOUNT',
    'ingestion.CC_SOURCE_FILE_SECURITiZATION',
    'ingestion.CL_SOURCE_FILE_SECURITiZATION'
]
DOWNSTREAM_ASSET = "features.SECURITIZEDAMOUNT"
DEPENDENCIES = {
    'export_auto': ['duckdb_clear_securitizedamount'],
    'export_cc': ['duckdb_clear_securitizedamount'],
    'export_cl': ['duckdb_clear_securitizedamount'],
    'duckdb_clear_securitizedamount': ['duckdb_load_securitizedamount'],
}


def export_auto(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
        SELECT
            'AUTO' as SECRTZTN_TP_CD,
            NULL AS SECURITIZEDAMOUNT
        FROM {UPSTREAM_ASSET[0]} auto
        WHERE auto.EFFECTIVE_DATE = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass


def export_cc(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
        SELECT
            'CC' as SECRTZTN_TP_CD,
            SECURITIZED_AMOUNT AS SECURITIZEDAMOUNT
        FROM {UPSTREAM_ASSET[1]} cc
        WHERE cc.EFFECTIVE_DATE = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass


def export_cl(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
        SELECT
            'CL' as SECRTZTN_TP_CD,
            SECURITIZED_AMOUNT AS SECURITIZEDAMOUNT
        FROM {UPSTREAM_ASSET[2]} cl
        WHERE cl.EFFECTIVE_DATE = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass


def duckdb_clear_securitizedamount(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    DELETE FROM { DOWNSTREAM_ASSET } 
    WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass


def duckdb_load_securitizedamount(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
        INSERT INTO {DOWNSTREAM_ASSET} by name 
            FROM (
                SELECT 
                    '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
                    SECRTZTN_TP_CD,
                    SECURITIZEDAMOUNT
                FROM read_parquet([
                    '{{{{ task_instance.xcom_pull(task_ids="derived__securitizedamount.export_auto", key="parquet") }}}}',
                    '{{{{ task_instance.xcom_pull(task_ids="derived__securitizedamount.export_cc", key="parquet") }}}}',
                    '{{{{ task_instance.xcom_pull(task_ids="derived__securitizedamount.export_cl", key="parquet") }}}}'
                ], union_by_name=true)
            ) 
    """
):
    pass