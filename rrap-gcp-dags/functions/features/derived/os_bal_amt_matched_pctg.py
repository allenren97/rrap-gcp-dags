import pyarrow as pa
from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

UPSTREAM_ASSET = ['features.TOT_OS_BAL_MATCHED',
                  'ingestion.SPL_SOURCE_FILE_AMOUNT',
                  'ingestion.CC_SOURCE_FILE_AMOUNT',
                  'ingestion.CL_SOURCE_FILE_AMOUNT']

DOWNSTREAM_ASSET = 'features.OS_BAL_AMT_MATCHED_PCTG'
DEPENDENCIES = {
    'export_auto': ['duckdb_clear_os_bal_amt_matched_pctg'],
    'export_cc': ['duckdb_clear_os_bal_amt_matched_pctg'],
    'export_cl': ['duckdb_clear_os_bal_amt_matched_pctg'],
    'duckdb_clear_os_bal_amt_matched_pctg': ['duckdb_derive_os_bal_amt_matched_pctg']
}

def export_auto(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
        SELECT
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            bal.SECRTZTN_TP_CD,
            (bal.TOT_OS_BAL_MATCHED) / a.outstanding_bal*100 AS OS_BAL_AMT_MATCHED_PCTG
            FROM {UPSTREAM_ASSET[1]} a,
            {UPSTREAM_ASSET[0]} bal
        WHERE 
            bal.SECRTZTN_TP_CD = 'AUTO'
            AND bal.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass

def export_cc(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
        SELECT
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            bal.SECRTZTN_TP_CD,
            (bal.TOT_OS_BAL_MATCHED) / a.outstanding_bal*100 AS OS_BAL_AMT_MATCHED_PCTG
            FROM {UPSTREAM_ASSET[2]} a,
            {UPSTREAM_ASSET[0]} bal
        WHERE 
            bal.SECRTZTN_TP_CD = 'CC'
            AND bal.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass

def export_cl(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
        SELECT
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            bal.SECRTZTN_TP_CD,
            (bal.TOT_OS_BAL_MATCHED) / NULLIF(a.outstanding_bal, 0)*100 AS OS_BAL_AMT_MATCHED_PCTG
            FROM {UPSTREAM_ASSET[3]} a,
            {UPSTREAM_ASSET[0]} bal
        WHERE 
            bal.SECRTZTN_TP_CD = 'CL'
            AND bal.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass

def duckdb_clear_os_bal_amt_matched_pctg(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
        DELETE FROM { DOWNSTREAM_ASSET } 
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass

def duckdb_derive_os_bal_amt_matched_pctg(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    INSERT INTO { DOWNSTREAM_ASSET } BY NAME 
    FROM (
        SELECT
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            SECRTZTN_TP_CD,
            OS_BAL_AMT_MATCHED_PCTG
        FROM read_parquet([
            '{{{{ task_instance.xcom_pull(task_ids="derived__os_bal_amt_matched_pctg.export_auto", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__os_bal_amt_matched_pctg.export_cc", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__os_bal_amt_matched_pctg.export_cl", key="parquet") }}}}'
        ], union_by_name=true)
    )
    """
):
    pass