import os
from datetime import timedelta
import pendulum

from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import _pull_asset_event_extras, _push_asset_event_extras


UPSTREAM_ASSET = [
    'ingestion.SPL_SOURCE_FILE_AMOUNT',
    'ingestion.CC_SOURCE_FILE_AMOUNT',
    'ingestion.CL_SOURCE_FILE_AMOUNT'
]
DOWNSTREAM_ASSET = "features.TOT_ACCT_CNT_RECVD"
DEPENDENCIES = {
    'export_auto': ['duckdb_clear_tot_acct_cnt_recvd'],
    'export_cc': ['duckdb_clear_tot_acct_cnt_recvd'],
    'export_cl': ['duckdb_clear_tot_acct_cnt_recvd'],
    'duckdb_clear_tot_acct_cnt_recvd': ['duckdb_load_tot_acct_cnt_recvd'],
}


def export_auto(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
        SELECT
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            'AUTO' as SECRTZTN_TP_CD,
            auto.NUM_ACCT AS TOT_ACCT_CNT_RECVD
        FROM {UPSTREAM_ASSET[0]} auto
    """
):
    pass


def export_cc(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
        SELECT
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            'CC' as SECRTZTN_TP_CD,
            cc.NUM_ACCT AS TOT_ACCT_CNT_RECVD
        FROM {UPSTREAM_ASSET[1]} cc
        
    """
):
    pass


def export_cl(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
        SELECT
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            'CL' as SECRTZTN_TP_CD,
            cl.NUM_ACCT AS TOT_ACCT_CNT_RECVD
        FROM {UPSTREAM_ASSET[2]} cl
    """
):
    pass


def duckdb_clear_tot_acct_cnt_recvd(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    DELETE FROM { DOWNSTREAM_ASSET } 
    WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass


def duckdb_load_tot_acct_cnt_recvd(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
        INSERT INTO {DOWNSTREAM_ASSET} by name 
            FROM (
                SELECT 
                    '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
                    SECRTZTN_TP_CD,
                    TOT_ACCT_CNT_RECVD
                FROM read_parquet([
                    '{{{{ task_instance.xcom_pull(task_ids="derived__tot_acct_cnt_recvd.export_auto", key="parquet") }}}}',
                    '{{{{ task_instance.xcom_pull(task_ids="derived__tot_acct_cnt_recvd.export_cc", key="parquet") }}}}',
                    '{{{{ task_instance.xcom_pull(task_ids="derived__tot_acct_cnt_recvd.export_cl", key="parquet") }}}}'
                ], union_by_name=true)
            ) 
    """
):
    pass