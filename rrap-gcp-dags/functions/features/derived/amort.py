import pyarrow as pa
from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

UPSTREAM_ASSET = ["ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT",
                  "ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT",
                  "ingestion.MORT_MTH_SNAPSHOT",
                  "ingestion.TNG_ACCT_MO",
                  "ingestion.BASEL_ACCT_DIM"]

DOWNSTREAM_ASSET = 'features.AMORT'

DEPENDENCIES = {
    'export_ks': ['duckdb_clear_amort'],
    'export_spl': ['duckdb_clear_amort'],
    'export_mor': ['duckdb_clear_amort'],
    'export_tng': ['duckdb_clear_amort'],
    'duckdb_clear_amort': ['duckdb_derive_amort'],
}


def export_ks(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
    SELECT
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        'KS' AS SRC_SYS_CD,
        BASEL_ACCT_ID,
        NULL AS AMORT
    FROM {UPSTREAM_ASSET[0]}
    WHERE MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    """
):
    pass

def export_spl(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
    SELECT
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        'SPL' AS SRC_SYS_CD,
        BASEL_ACCT_ID,
        LOAN_TERM AS AMORT
    FROM {UPSTREAM_ASSET[1]}
    WHERE MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    """
):
    pass

def export_mor(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
    SELECT
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        'MOR' AS SRC_SYS_CD,
        BASEL_ACCT_ID,
        AMORT_MTH AS AMORT
    FROM {UPSTREAM_ASSET[2]}
    WHERE MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    """
):
    pass

def export_tng(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
    SELECT
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        'TNG-MOR' AS SRC_SYS_CD,
        dim.BASEL_ACCT_ID,
        tng.REMAIN_AMORT AS AMORT
    FROM {UPSTREAM_ASSET[3]} tng
    INNER JOIN {UPSTREAM_ASSET[4]} dim ON
        dim.SRC_APP_CD = 'TNG-MOR'
        AND dim.SRC_SYS_DEL_F != 'Y'
        AND TRIM(tng.ACCOUNT_ID) = TRIM(dim.SRC_APP_ID)
    WHERE tng.MONTH_END_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass

def duckdb_clear_amort(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
        DELETE FROM { DOWNSTREAM_ASSET } 
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass


def duckdb_derive_amort(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    INSERT INTO { DOWNSTREAM_ASSET } BY NAME FROM (
        SELECT
            OBSN_DT,
            BASEL_ACCT_ID,
            SRC_SYS_CD,
            AMORT
        FROM read_parquet([
            '{{{{ task_instance.xcom_pull(task_ids="derived__amort.export_ks", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__amort.export_spl", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__amort.export_mor", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__amort.export_tng", key="parquet") }}}}'
        ], union_by_name=true)
    )
    """,
):
    pass