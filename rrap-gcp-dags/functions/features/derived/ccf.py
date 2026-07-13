import pyarrow as pa
from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

UPSTREAM_ASSET = ['reference.RPTG_CCF_LKP',
                  'ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT',
                  'ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT',
                  'ingestion.MORT_MTH_SNAPSHOT',
                  'ingestion.TNG_ACCT_MO',
                  'ingestion.BASEL_ACCT_DIM',
                  'features.BASEL_PRD_TP_CD']

DOWNSTREAM_ASSET = 'features.CCF'
DEPENDENCIES = {
    'export_ks': ['duckdb_clear_ccf'],
    'export_spl': ['duckdb_clear_ccf'],
    'export_mor': ['duckdb_clear_ccf'],
    'export_tng': ['duckdb_clear_ccf'],
    'duckdb_clear_ccf': ['duckdb_derive_ccf']
}

def export_ks(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
    SELECT
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        ks.BASEL_ACCT_ID,
        'KS' AS SRC_SYS_CD,
        CCF
    FROM {UPSTREAM_ASSET[1]} ks
    LEFT JOIN {UPSTREAM_ASSET[6]} prd_cd ON
        ks.BASEL_ACCT_ID = prd_cd.BASEL_ACCT_ID
        AND prd_cd.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    LEFT JOIN {UPSTREAM_ASSET[0]} ccf ON
        prd_cd.BASEL_PRD_TP_CD = ccf.BASEL_PRD_TP_CD
        WHERE ks.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
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
        CCF
    FROM {UPSTREAM_ASSET[2]} spl
    LEFT JOIN {UPSTREAM_ASSET[6]} prd_cd ON
        spl.BASEL_ACCT_ID = prd_cd.BASEL_ACCT_ID
        AND prd_cd.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    LEFT JOIN {UPSTREAM_ASSET[0]} ccf ON
        prd_cd.BASEL_PRD_TP_CD = ccf.BASEL_PRD_TP_CD
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
        NULL AS CCF
    FROM {UPSTREAM_ASSET[3]} mor
    WHERE mor.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    """
):
    pass

def export_tng(
    duckdb_conn_id = "duckdb-conn",
    sql=f"""
    SELECT
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        dim.BASEL_ACCT_ID,
        'TNG-MOR' AS SRC_SYS_CD,
        NULL AS CCF
    FROM {UPSTREAM_ASSET[4]} tng
    INNER JOIN {UPSTREAM_ASSET[5]} dim ON
        TRIM(dim.SRC_APP_CD) = 'TNG-MOR'
        AND dim.SRC_SYS_DEL_F != 'Y'
        AND trim(tng.ACCOUNT_ID) = TRIM(dim.SRC_APP_ID)
    WHERE tng.MONTH_END_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass

def duckdb_clear_ccf(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
        DELETE FROM { DOWNSTREAM_ASSET } 
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass

def duckdb_derive_ccf(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    INSERT INTO { DOWNSTREAM_ASSET } BY NAME 
    FROM (
        SELECT
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            BASEL_ACCT_ID,
            SRC_SYS_CD,
            CCF
        FROM read_parquet([
            '{{{{ task_instance.xcom_pull(task_ids="derived__ccf.export_ks", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__ccf.export_spl", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__ccf.export_mor", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__ccf.export_tng", key="parquet") }}}}'
        ], union_by_name=true)
    )
    """
):
    pass