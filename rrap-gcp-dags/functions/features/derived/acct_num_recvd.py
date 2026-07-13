import os
from datetime import timedelta
import pendulum

from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import _pull_asset_event_extras, _push_asset_event_extras


UPSTREAM_ASSET = [
    'ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT',
    'ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT',
    'ingestion.CC_SOURCE_FILE_ACCOUNTS',
    'ingestion.SPL_SOURCE_FILE_ACCOUNTS',
]
DOWNSTREAM_ASSET = "features.ACCT_NUM_RECVD"
DEPENDENCIES = {
    'export_ks': ['duckdb_clear_acct_num_recvd'],
    'export_spl': ['duckdb_clear_acct_num_recvd'],
    'duckdb_clear_acct_num_recvd': ['duckdb_load_acct_num_recvd'],
}


def export_ks(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
        SELECT
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            a.BASEL_ACCT_ID,
            b.VISA_ACCT_NUM as ACCT_NUM_RECVD
        FROM {UPSTREAM_ASSET[0]} a
        INNER JOIN {UPSTREAM_ASSET[2]} b ON
            LTRIM(TRIM(a.ACCT_NUM), '0') = b.VISA_ACCT_NUM
            AND b.EFFECTIVE_DATE = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
        WHERE
            a.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    """
):
    pass


def export_spl(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
        SELECT
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            a.BASEL_ACCT_ID,
            b.ACCOUNT_NUMBER as ACCT_NUM_RECVD
        FROM {UPSTREAM_ASSET[1]} a
        INNER JOIN {UPSTREAM_ASSET[3]} b ON
            TRIM(a.CRNT_BR_LOCTN_TRNST) || TRIM(a.LOAN_NUM) = b.ACCOUNT_NUMBER
            AND b.EFFECTIVE_DATE = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
        WHERE
            a.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    """
):
    pass


def duckdb_clear_acct_num_recvd(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    DELETE FROM { DOWNSTREAM_ASSET } 
    WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass

def duckdb_load_acct_num_recvd(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} BY NAME
    FROM (
        SELECT
            OBSN_DT,
            BASEL_ACCT_ID,
            ACCT_NUM_RECVD
        FROM read_parquet([
            '{{{{ task_instance.xcom_pull(task_ids="derived__acct_num_recvd.export_ks", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__acct_num_recvd.export_spl", key="parquet") }}}}'
        ], union_by_name=true)
    )
    """
):
    pass