import pyarrow as pa
from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

UPSTREAM_ASSET = ['ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT',
                  'ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT',
                  'ingestion.MORT_MTH_SNAPSHOT',
                  'ingestion.TNG_ACCT_MO',
                  'ingestion.BASEL_ACCT_DIM']

DOWNSTREAM_ASSET = 'features.PD_OFF_DT'
DEPENDENCIES = {
    'export_ks': ['duckdb_clear_pd_off_dt'],
    'export_spl': ['duckdb_clear_pd_off_dt'],
    'export_mor': ['duckdb_clear_pd_off_dt'],
    'export_tng': ['duckdb_clear_pd_off_dt'],
    'duckdb_clear_pd_off_dt': ['duckdb_derive_pd_off_dt']
}

def export_ks(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
        SELECT
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            ks.BASEL_ACCT_ID,
            'KS' AS SRC_SYS_CD,
            NULL AS PD_OFF_DT
        FROM {UPSTREAM_ASSET[0]} ks
        WHERE
            ks.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    """
):
    pass

def export_spl(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
        SELECT
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            spl.BASEL_ACCT_ID,
            'SPL' AS SRC_SYS_CD,
            NULL AS PD_OFF_DT
        FROM {UPSTREAM_ASSET[1]} spl
        WHERE 
            spl.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    """
):
    pass

def export_mor(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
        SELECT
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            mor.BASEL_ACCT_ID,
            'MOR' AS SRC_SYS_CD,
            NULL AS PD_OFF_DT
        FROM {UPSTREAM_ASSET[2]} mor
        WHERE mor.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
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
            CASE 
                WHEN DATE_TRUNC('DAY', tng.CLOSE_DT) IS NOT NULL THEN tng.CLOSE_DT
                ELSE NULL
            END AS PD_OFF_DT
        FROM {UPSTREAM_ASSET[4]} dim
        LEFT JOIN {UPSTREAM_ASSET[3]} tng ON
            dim.SRC_APP_CD = 'TNG-MOR'
            AND dim.SRC_SYS_DEL_F != 'Y'
            AND TRIM(tng.ACCOUNT_ID) = TRIM(dim.SRC_APP_ID)
        WHERE 
            tng.MONTH_END_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass

def duckdb_clear_pd_off_dt(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
        DELETE FROM { DOWNSTREAM_ASSET } 
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass

def duckdb_derive_pd_off_dt(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    INSERT INTO { DOWNSTREAM_ASSET } BY NAME 
    FROM (
        SELECT
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            BASEL_ACCT_ID,
            SRC_SYS_CD,
            PD_OFF_DT
        FROM read_parquet([
            '{{{{ task_instance.xcom_pull(task_ids="derived__pd_off_dt.export_ks", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__pd_off_dt.export_spl", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__pd_off_dt.export_mor", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__pd_off_dt.export_tng", key="parquet") }}}}'
        ], union_by_name=true)
    )
    """,
):
    pass
