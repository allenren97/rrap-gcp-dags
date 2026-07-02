import pyarrow as pa
from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

UPSTREAM_ASSET = ['features.AF_ADJ_OS_BAL_AMT',
                  'features.OS_BAL_AMT_V2',
                  'features.TREATMENT_F',
                  'features.TRNST_EXCLSN_F',
                  'features.PIT_STATUS_CROSS_DEFAULT_ORIG',
                  'ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT',
                  'features.PRD_ID']

DOWNSTREAM_ASSET = 'features.ADJUSTED_OS_BAL_AMT_SECURITIZATION'

DEPENDENCIES = {
    'duckdb_clear_adjusted_os_bal_securitization': ['duckdb_load_adjusted_os_bal_securitization']
}

def duckdb_clear_adjusted_os_bal_securitization(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
        DELETE FROM { DOWNSTREAM_ASSET } 
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass

def duckdb_load_adjusted_os_bal_securitization(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} BY NAME
    FROM (
        SELECT
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            spl.BASEL_ACCT_ID,
            CAST(TRIM(spl.CRNT_BR_LOCTN_TRNST) || TRIM(spl.LOAN_NUM) AS BIGINT) AS account_number,
            CASE 
                WHEN TRIM(pit.PIT_STATUS_CROSS_DEFAULT_ORIG) = 'CUR' 
                    AND TRIM(treatm.TREATMENT_F) = 'A' 
                    AND TRIM(exclsn.TRNST_EXCLSN_F) = 'N' 
                    AND TRIM(prd.PRD_ID) IN ('S09', 'S10')
                THEN af.AF_ADJ_OS_BAL_AMT 
                ELSE COALESCE(bal.OS_BAL_AMT_V2, 0)
            END AS ADJUSTED_OS_BAL_AMT_SECURITIZATION
        FROM {UPSTREAM_ASSET[5]} spl
        LEFT JOIN (SELECT * FROM {UPSTREAM_ASSET[3]} WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}') exclsn ON
            spl.BASEL_ACCT_ID = exclsn.BASEL_ACCT_ID
        LEFT JOIN (SELECT * FROM {UPSTREAM_ASSET[1]} WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}') bal ON
            spl.BASEL_ACCT_ID = bal.BASEL_ACCT_ID
        LEFT JOIN (SELECT * FROM {UPSTREAM_ASSET[4]} WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}') pit ON
            spl.BASEL_ACCT_ID = pit.BASEL_ACCT_ID
        LEFT JOIN (SELECT * FROM {UPSTREAM_ASSET[2]} WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}') treatm ON
            spl.BASEL_ACCT_ID = treatm.BASEL_ACCT_ID
        LEFT JOIN (SELECT * FROM {UPSTREAM_ASSET[6]} WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}') prd ON
            spl.BASEL_ACCT_ID = prd.BASEL_ACCT_ID
        LEFT JOIN {UPSTREAM_ASSET[0]} af ON
            spl.BASEL_ACCT_ID = af.BASEL_ACCT_ID
            AND af.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
        WHERE 
            spl.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id")  }}}}
    )
    """
):
        pass

