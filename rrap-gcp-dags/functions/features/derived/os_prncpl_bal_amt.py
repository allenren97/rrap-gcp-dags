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
                  'features.BASEL_ACCT_ID_CCAR_MATCHED',
                  'features.SECRTZTN_OS_ADJ_FACTR',
                  "features.GENL_LEDGER_BALCNG_ADJ_AMT",
                  "features.TOT_NEW_BAL_AMT"]

DOWNSTREAM_ASSET = 'features.OS_PRNCPL_BAL_AMT'

DEPENDENCIES = {
    'export_ks': ['duckdb_delete_os_prncpl_bal_amt'],
    'export_spl': ['duckdb_delete_os_prncpl_bal_amt'],
    'export_mor': ['duckdb_delete_os_prncpl_bal_amt'],
    'export_tng': ['duckdb_delete_os_prncpl_bal_amt'],
    'duckdb_delete_os_prncpl_bal_amt': ['duckdb_derive_os_prncpl_bal_amt']
}

def export_ks(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
    WITH GENL_LEDGER_BALCNG_ADJ_AMT AS 
        (
			SELECT
            BASEL_ACCT_ID,
            GENL_LEDGER_BALCNG_ADJ_AMT
            FROM
            {UPSTREAM_ASSET[8]}
            where OBSN_DT='{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
            and TRIM(SRC_SYS_CD) = 'KS'
            AND GENL_LEDGER_BALCNG_ADJ_AMT IS NOT NULL
        ),

		TOT_NEW_BAL_AMT as 
			(
            SELECT
            BASEL_ACCT_ID,
            TOT_NEW_BAL_AMT
            FROM 
            {UPSTREAM_ASSET[9]}
            where OBSN_DT='{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
			),
            
		final AS (
            SELECT
            T2.BASEL_ACCT_ID,
                CAST(ROUND(COALESCE(T2.TOT_NEW_BAL_AMT, 0)
            + COALESCE(T1.GENL_LEDGER_BALCNG_ADJ_AMT, 0), 8) AS DECIMAL(38,8))
                AS AF_ADJ_OS_BAL_AMT
            FROM GENL_LEDGER_BALCNG_ADJ_AMT T1
            JOIN TOT_NEW_BAL_AMT T2
            ON T1.BASEL_ACCT_ID = T2.BASEL_ACCT_ID
            ),
        
        af_adj_os_bal_amt AS(
            select
            BASEL_ACCT_ID, 
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            'KS' AS SRC_SYS_CD,
            TRUNC(AF_ADJ_OS_BAL_AMT, 3) AS AF_ADJ_OS_BAL_AMT
            from final
    )

        SELECT
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            ks.BASEL_ACCT_ID,
            'KS' AS SRC_SYS_CD,
            CASE 
                WHEN matched.BASEL_ACCT_ID_CCAR_MATCHED IS NOT NULL and ks.MTH_TM_ID>=16516 
                THEN ROUND(TRUNC(ks.TOT_NEW_BAL_AMT - af.AF_ADJ_OS_BAL_AMT*sec.SECRTZTN_OS_ADJ_FACTR,3), 8)
                ELSE ROUND(ks.TOT_NEW_BAL_AMT, 8)
            END AS OS_PRNCPL_BAL_AMT
        FROM {UPSTREAM_ASSET[0]} ks
        LEFT JOIN af_adj_os_bal_amt af ON
            ks.BASEL_ACCT_ID = af.BASEL_ACCT_ID
            AND af.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
        LEFT JOIN {UPSTREAM_ASSET[6]} matched ON
            ks.BASEL_ACCT_ID = matched.BASEL_ACCT_ID_CCAR_MATCHED
            AND matched.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
        LEFT JOIN {UPSTREAM_ASSET[7]} sec ON
            matched.SECRTZTN_TP_CD = sec.SECRTZTN_TP_CD
            AND sec.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
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
            NULL AS OS_PRNCPL_BAL_AMT
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
            NULL AS OS_PRNCPL_BAL_AMT
        FROM {UPSTREAM_ASSET[3]} mor 
        WHERE 
            mor.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
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
            NULL AS OS_PRNCPL_BAL_AMT
        FROM {UPSTREAM_ASSET[4]} tng
        INNER JOIN {UPSTREAM_ASSET[5]} dim ON
            dim.SRC_APP_CD = 'TNG-MOR'
            AND dim.SRC_SYS_DEL_F != 'Y'
            AND TRIM(tng.ACCOUNT_ID) = TRIM(dim.SRC_APP_ID)
        WHERE 
            MONTH_END_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass

def duckdb_delete_os_prncpl_bal_amt(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
        DELETE FROM { DOWNSTREAM_ASSET } 
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass

def duckdb_derive_os_prncpl_bal_amt(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    INSERT INTO { DOWNSTREAM_ASSET } BY NAME 
    FROM (
        SELECT
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            BASEL_ACCT_ID,
            SRC_SYS_CD,
            OS_PRNCPL_BAL_AMT
        FROM read_parquet([
            '{{{{ task_instance.xcom_pull(task_ids="derived__os_prncpl_bal_amt.export_ks", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__os_prncpl_bal_amt.export_spl", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__os_prncpl_bal_amt.export_mor", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__os_prncpl_bal_amt.export_tng", key="parquet") }}}}'
        ], union_by_name=true)
    )
    """,
):
    pass
