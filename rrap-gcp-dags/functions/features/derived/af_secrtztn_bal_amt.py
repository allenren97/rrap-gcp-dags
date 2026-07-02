import pyarrow as pa
from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

UPSTREAM_ASSET = ['ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT',
                  'ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT',
                  'features.AF_ADJ_OS_BAL_AMT',
                  'ingestion.MORT_MTH_SNAPSHOT',
                  'ingestion.TNG_ACCT_MO',
                  'ingestion.BASEL_ACCT_DIM',
                  'features.ADJUSTED_OS_BAL_AMT',
                  'features.GENL_LEDGER_BALCNG_ADJ_AMT',
                  'features.ADJUSTED_OS_BAL_AMT_SECURITIZATION']

DOWNSTREAM_ASSET = 'features.AF_SECRTZTN_BAL_AMT'

DEPENDENCIES = {
    'export_ks': ['duckdb_clear_af_secrtztn_bal_amt'],
    'export_spl': ['duckdb_clear_af_secrtztn_bal_amt'],
    'export_mor': ['duckdb_clear_af_secrtztn_bal_amt'],
    'export_tng': ['duckdb_clear_af_secrtztn_bal_amt'],
    'duckdb_clear_af_secrtztn_bal_amt': ['duckdb_derive_af_secrtztn_bal_amt']
}

def export_ks(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
    WITH dv_prep AS(
        SELECT
            ks.BASEL_ACCT_ID,
            CASE 
                WHEN af.AF_ADJ_OS_BAL_AMT <= 0 OR af.AF_ADJ_OS_BAL_AMT IS NULL
                THEN 0.0
                ELSE ROUND(af.AF_ADJ_OS_BAL_AMT, 4)
            END AS AF_SECRTZTN_BAL_AMT
        FROM {UPSTREAM_ASSET[0]} ks
        LEFT JOIN {UPSTREAM_ASSET[2]} af ON
            ks.BASEL_ACCT_ID = af.BASEL_ACCT_ID
            AND af.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
        WHERE 
            ks.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
            AND TRIM(af.SRC_SYS_CD) = 'KS'
        )
        SELECT
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            'KS' AS SRC_SYS_CD,
            ks.BASEL_ACCT_ID,
            AF_SECRTZTN_BAL_AMT
        FROM {UPSTREAM_ASSET[0]} ks
        LEFT JOIN dv_prep dv ON
            ks.BASEL_ACCT_ID = dv.BASEL_ACCT_ID
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
            CASE
                WHEN adj.ADJUSTED_OS_BAL_AMT_SECURITIZATION < 0 THEN 0
                ELSE adj.ADJUSTED_OS_BAL_AMT_SECURITIZATION
            END AS AF_SECRTZTN_BAL_AMT
        FROM {UPSTREAM_ASSET[1]} spl
        LEFT JOIN {UPSTREAM_ASSET[8]} adj ON
            spl.BASEL_ACCT_ID = adj.BASEL_ACCT_ID
            AND adj.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
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
            mor.CRNT_BAL_AMT + genl.GENL_LEDGER_BALCNG_ADJ_AMT AS AF_SECRTZTN_BAL_AMT
        FROM {UPSTREAM_ASSET[3]} mor 
        LEFT JOIN {UPSTREAM_ASSET[7]} genl ON
            mor.BASEL_ACCT_ID = genl.BASEL_ACCT_ID
            AND genl.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
        WHERE 
            mor.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
            AND TRIM(genl.SRC_SYS_CD) = 'MOR'
    """
):
    pass

def export_tng(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
        SELECT
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            'TNG-MOR' AS SRC_SYS_CD,
            BASEL_ACCT_ID,
            ADJUSTED_OS_BAL_AMT AS AF_SECRTZTN_BAL_AMT
        FROM {UPSTREAM_ASSET[6]}
        WHERE 
            OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
            AND TRIM(SRC_SYS_CD) = 'TNG'
    """
):
    pass

def duckdb_clear_af_secrtztn_bal_amt(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
        DELETE FROM { DOWNSTREAM_ASSET } 
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass

def duckdb_derive_af_secrtztn_bal_amt(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    INSERT INTO { DOWNSTREAM_ASSET } BY NAME 
    FROM (
        SELECT
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            BASEL_ACCT_ID,
            SRC_SYS_CD,
            AF_SECRTZTN_BAL_AMT
        FROM read_parquet([
            '{{{{ task_instance.xcom_pull(task_ids="derived__af_secrtztn_bal_amt.export_ks", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__af_secrtztn_bal_amt.export_spl", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__af_secrtztn_bal_amt.export_mor", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__af_secrtztn_bal_amt.export_tng", key="parquet") }}}}'
        ], union_by_name=true)
    )
    """,
):
    pass
