import pyarrow as pa
from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

UPSTREAM_ASSET = ['ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT',
                  'ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT',
                  'ingestion.BASELAYER_MOR',
                  'ingestion.MORT_MTH_SNAPSHOT',
                  'ingestion.TNG_ACCT_MO',
                  'ingestion.BASEL_ACCT_DIM',]

DOWNSTREAM_ASSET = 'features.LEGAL_ENTITY'

DEPENDENCIES = {
    'export_ks': ['duckdb_clear_legal_entity'],
    'export_spl': ['duckdb_clear_legal_entity'],
    'export_mor': ['duckdb_clear_legal_entity'],
    'export_tng': ['duckdb_clear_legal_entity'],
    'duckdb_clear_legal_entity': ['duckdb_derive_legal_entity']
}

def export_ks(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
        SELECT
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            BASEL_ACCT_ID,
            'KS' AS SRC_SYS_CD,
            NULL AS LEGAL_ENTITY
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
            BASEL_ACCT_ID,
            'SPL' AS SRC_SYS_CD,
            'DOM-RETAIL-OTHER' AS LEGAL_ENTITY
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
            BASEL_ACCT_ID,
            'MOR' AS SRC_SYS_CD,
            base.LEGAL_ENTITY
        FROM {UPSTREAM_ASSET[2]} base
        LEFT JOIN {UPSTREAM_ASSET[3]} mor ON
            base.MORT_NUM = mor.MORT_NUM
        WHERE 
            mor.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
            AND base.MTH_END_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass

def export_tng(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
        SELECT
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            dim.BASEL_ACCT_ID,
            'TNG-MOR' AS SRC_SYS_CD,
            'DOM-SUB-TNG' AS LEGAL_ENTITY
        FROM {UPSTREAM_ASSET[4]} tng
        INNER JOIN {UPSTREAM_ASSET[5]} dim ON
            dim.SRC_APP_CD = 'TNG-MOR'
            AND dim.SRC_SYS_DEL_F != 'Y'
            AND trim(tng.ACCOUNT_ID) = TRIM(dim.SRC_APP_ID)
        WHERE tng.MONTH_END_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass

def duckdb_clear_legal_entity(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
        DELETE FROM { DOWNSTREAM_ASSET } 
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass

def duckdb_derive_legal_entity(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    INSERT INTO { DOWNSTREAM_ASSET } BY NAME 
    FROM (
        SELECT
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            BASEL_ACCT_ID,
            SRC_SYS_CD,
            LEGAL_ENTITY
        FROM read_parquet([
            '{{{{ task_instance.xcom_pull(task_ids="derived__legal_entity.export_ks", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__legal_entity.export_spl", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__legal_entity.export_mor", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__legal_entity.export_tng", key="parquet") }}}}'
        ], union_by_name=true)
    )
    """,
):
    pass