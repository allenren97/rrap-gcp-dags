import pyarrow as pa
from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

UPSTREAM_ASSET = ['ingestion.BASEL_ACCT_PRFM_FACT',
                  'ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT',
                  'features.SRC_SYS_CD',
                  'ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT',
                  'ingestion.MORT_MTH_SNAPSHOT',
                  'ingestion.TNG_ACCT_MO',
                  'ingestion.BASEL_ACCT_DIM']

DOWNSTREAM_ASSET = 'features.CURRENCY_MISMATCH_F'
DEPENDENCIES = {
    'export_ks': ['duckdb_clear_currency_mismatch_f'],
    'export_spl': ['duckdb_clear_currency_mismatch_f'],
    'export_mor': ['duckdb_clear_currency_mismatch_f'],
    'export_tng': ['duckdb_clear_currency_mismatch_f'],
    'duckdb_clear_currency_mismatch_f': ['duckdb_derive_currency_mismatch_f']
}

def export_ks(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
        SELECT
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            ks.BASEL_ACCT_ID,
            'KS' AS SRC_SYS_CD,
            CASE 
                WHEN TRIM(CURRENCY_MISMATCH_F) IS NOT NULL THEN TRIM(CURRENCY_MISMATCH_F)
                ELSE NULL
            END AS CURRENCY_MISMATCH_F
        FROM {UPSTREAM_ASSET[1]} ks
        LEFT JOIN {UPSTREAM_ASSET[0]} fact ON
            ks.BASEL_ACCT_ID = fact.BASEL_ACCT_ID
            AND fact.MTH_END_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
        LEFT JOIN {UPSTREAM_ASSET[2]} sys_cd ON
            ks.BASEL_ACCT_ID = sys_cd.BASEL_ACCT_ID
            AND sys_cd.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
        WHERE
            ks.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
            AND sys_cd.SRC_SYS_CD = 'KS'
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
                WHEN TRIM(CURRENCY_MISMATCH_F) IS NOT NULL THEN TRIM(CURRENCY_MISMATCH_F)
                ELSE NULL
            END AS CURRENCY_MISMATCH_F
        FROM {UPSTREAM_ASSET[3]} spl
        LEFT JOIN {UPSTREAM_ASSET[0]} fact ON
            spl.BASEL_ACCT_ID = fact.BASEL_ACCT_ID
            AND spl.MTH_TM_ID = fact.MTH_TM_ID
        LEFT JOIN {UPSTREAM_ASSET[2]} sys_cd ON
            spl.BASEL_ACCT_ID = sys_cd.BASEL_ACCT_ID
            AND sys_cd.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
        WHERE 
            spl.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
            AND sys_cd.SRC_SYS_CD = 'SPL'
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
            CASE 
                WHEN TRIM(CURRENCY_MISMATCH_F) IS NOT NULL THEN TRIM(CURRENCY_MISMATCH_F)
                ELSE NULL
            END AS CURRENCY_MISMATCH_F
        FROM {UPSTREAM_ASSET[4]} mor
        LEFT JOIN {UPSTREAM_ASSET[0]} fact ON
            mor.BASEL_ACCT_ID = fact.BASEL_ACCT_ID
            AND fact.MTH_END_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
        LEFT JOIN {UPSTREAM_ASSET[2]} sys_cd ON
            mor.BASEL_ACCT_ID = sys_cd.BASEL_ACCT_ID
            AND sys_cd.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
            WHERE 
                mor.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
                AND sys_cd.SRC_SYS_CD = 'MOR'
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
                WHEN TRIM(CURRENCY_MISMATCH_F) IS NOT NULL THEN TRIM(CURRENCY_MISMATCH_F)
                ELSE NULL
            END AS CURRENCY_MISMATCH_F
        FROM {UPSTREAM_ASSET[6]} dim
        LEFT JOIN {UPSTREAM_ASSET[5]} tng ON
            dim.SRC_APP_CD = 'TNG-MOR'
            AND dim.SRC_SYS_DEL_F != 'Y'
            AND TRIM(tng.ACCOUNT_ID) = TRIM(dim.SRC_APP_ID)
        LEFT JOIN {UPSTREAM_ASSET[0]} fact ON
            dim.BASEL_ACCT_ID = fact.BASEL_ACCT_ID
            AND fact.MTH_END_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
        WHERE 
            tng.MONTH_END_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass

def duckdb_clear_currency_mismatch_f(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
        DELETE FROM { DOWNSTREAM_ASSET } 
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass

def duckdb_derive_currency_mismatch_f(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    INSERT INTO { DOWNSTREAM_ASSET } BY NAME 
    FROM (
        SELECT
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            BASEL_ACCT_ID,
            SRC_SYS_CD,
            CURRENCY_MISMATCH_F
        FROM read_parquet([
            '{{{{ task_instance.xcom_pull(task_ids="derived__currency_mismatch_f.export_ks", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__currency_mismatch_f.export_spl", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__currency_mismatch_f.export_mor", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__currency_mismatch_f.export_tng", key="parquet") }}}}'
        ], union_by_name=true)
    )
    """,
):
    pass
