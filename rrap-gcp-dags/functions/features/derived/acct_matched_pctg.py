import os
from datetime import timedelta
import pendulum

from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import _pull_asset_event_extras, _push_asset_event_extras

UPSTREAM_ASSET = [
    'features.TOT_ACCT_CNT_MATCHED',
    'ingestion.SPL_SOURCE_FILE_AMOUNT',
    'ingestion.CC_SOURCE_FILE_AMOUNT',
    'ingestion.CL_SOURCE_FILE_AMOUNT',
]
DOWNSTREAM_ASSET = "features.ACCT_MATCHED_PCTG"
DEPENDENCIES = {
    'export_auto': ['duckdb_clear_acct_matched_pctg'],
    'export_cc': ['duckdb_clear_acct_matched_pctg'],
    'export_cl': ['duckdb_clear_acct_matched_pctg'],
    'duckdb_clear_acct_matched_pctg': ['duckdb_load_acct_matched_pctg'],
}

CC_INPUT_PREFIX = "/bns/rrap/homes/rraprun/securitization/cc/cc_edwext_securitization_acct_mthly"
AUTO_INPUT_PREFIX = "/bns/rrap/homes/rraprun/securitization/cc/autoloan_securitization_acct_mthly"


def export_auto(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
        SELECT
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            tot.SECRTZTN_TP_CD,
            (tot.TOT_ACCT_CNT_MATCHED / autoloan.NUM_ACCT)*100 AS ACCT_MATCHED_PCTG
        FROM
            {UPSTREAM_ASSET[0]} tot,
            {UPSTREAM_ASSET[1]} autoloan
        WHERE 
            tot.SECRTZTN_TP_CD = 'AUTO'
            AND tot.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass

def export_cc(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
        SELECT
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            'CC' AS SECRTZTN_TP_CD,
            (tot.TOT_ACCT_CNT_MATCHED / cc.NUM_ACCT)*100 AS ACCT_MATCHED_PCTG
        FROM
            {UPSTREAM_ASSET[0]} tot,
            {UPSTREAM_ASSET[2]} cc
        WHERE 
            tot.SECRTZTN_TP_CD = 'CC'
            AND tot.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass

def export_cl(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
        SELECT
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            'CL' AS SECRTZTN_TP_CD,
            (tot.TOT_ACCT_CNT_MATCHED / NULLIF(cl.NUM_ACCT, 0))*100 AS ACCT_MATCHED_PCTG
        FROM
            {UPSTREAM_ASSET[0]} tot,
            {UPSTREAM_ASSET[3]} cl
        WHERE 
            tot.SECRTZTN_TP_CD = 'CL'
            AND tot.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass


def duckdb_clear_acct_matched_pctg(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    DELETE FROM { DOWNSTREAM_ASSET } 
    WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass

def duckdb_load_acct_matched_pctg(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} BY NAME
    FROM(
        SELECT
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            SECRTZTN_TP_CD,
            ACCT_MATCHED_PCTG
        FROM read_parquet([
            '{{{{ task_instance.xcom_pull(task_ids="derived__acct_matched_pctg.export_auto", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__acct_matched_pctg.export_cc", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__acct_matched_pctg.export_cl", key="parquet") }}}}'
        ], union_by_name=true)
    )
    """
):
    pass