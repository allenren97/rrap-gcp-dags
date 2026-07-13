import pyarrow as pa
from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

UPSTREAM_ASSET = ['ingestion.BASEL_ACCT_PRFM_FACT',
                  'ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT',
                  'ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT',
                  'ingestion.MORT_MTH_SNAPSHOT',
                  'features.SRC_SYS_CD',
                  'features.HELOC_F',
                  'features.BASEL_PRD_CD',
                  'features.TOTAL_EXPSR_ABOVE_LMT_F',
                  'ingestion.TNG_ACCT_MO',
                  'ingestion.BASEL_ACCT_DIM',
                  'reference.BASEL_RPTG_PRD_LKP']

DOWNSTREAM_ASSET = 'features.TRANSACTOR_FLAG_QRR'
DEPENDENCIES = {
    'export_ks': ['duckdb_clear_transactor_flag_qrr'],
    'export_spl': ['duckdb_clear_transactor_flag_qrr'],
    'export_mor': ['duckdb_clear_transactor_flag_qrr'],
    'export_tng': ['duckdb_clear_transactor_flag_qrr'],
    'duckdb_clear_transactor_flag_qrr': ['duckdb_derive_transactor_flag_qrr']
}

def export_ks(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
    SELECT
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        ks.BASEL_ACCT_ID,
        sys_cd.SRC_SYS_CD,
        CASE
            WHEN TRIM(rptg.ASST_CL_DESC) = 'QRR' THEN COALESCE(TRIM(trans_f.TRNSCTR_IND), 'N')
            ELSE NULL
        END AS TRANSACTOR_FLAG_QRR
    FROM {UPSTREAM_ASSET[1]} ks
    LEFT JOIN (SELECT * FROM {UPSTREAM_ASSET[5]} WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}') heloc ON
        ks.BASEL_ACCT_ID = heloc.BASEL_ACCT_ID
    LEFT JOIN (SELECT * FROM {UPSTREAM_ASSET[6]} WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}') prd_cd ON
        ks.BASEL_ACCT_ID = prd_cd.BASEL_ACCT_ID
    LEFT JOIN (SELECT * FROM {UPSTREAM_ASSET[7]} WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}') expsr ON
        ks.BASEL_ACCT_ID = expsr.BASEL_ACCT_ID
    LEFT JOIN {UPSTREAM_ASSET[10]} rptg ON
        TRIM(ks.PRD_CD) = TRIM(rptg.PRD_CD)
        AND TRIM(ks.SUB_PRD_CD) = TRIM(rptg.SUB_PRD_CD)
        AND TRIM(heloc.HELOC_F) = TRIM(rptg.HELOC_F)
        AND TRIM(prd_cd.BASEL_PRD_CD) = TRIM(rptg.BASEL_PRD_CD)
        AND TRIM(expsr.TOTAL_EXPSR_ABOVE_LMT_F) = TRIM(rptg.REVISED_EXPSR_OV_125K_F)
    LEFT JOIN {UPSTREAM_ASSET[0]} trans_f ON
        ks.BASEL_ACCT_ID = trans_f.BASEL_ACCT_ID
        AND ks.MTH_TM_ID = trans_f.MTH_TM_ID
    LEFT JOIN {UPSTREAM_ASSET[4]} sys_cd ON
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
        NULL AS TRANSACTOR_FLAG_QRR
    FROM {UPSTREAM_ASSET[2]} spl
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
        NULL AS TRANSACTOR_FLAG_QRR
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
        dim.BASEL_ACCT_ID,
        'TNG-MOR' AS SRC_SYS_CD,
        NULL AS TRANSACTOR_FLAG_QRR
    FROM {UPSTREAM_ASSET[8]} tng
    INNER JOIN {UPSTREAM_ASSET[9]} dim ON
        dim.SRC_APP_CD = 'TNG-MOR'
        AND dim.SRC_SYS_DEL_F != 'Y'
        AND trim(tng.ACCOUNT_ID) = TRIM(dim.SRC_APP_ID)
    WHERE 
        tng.MONTH_END_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass

def duckdb_clear_transactor_flag_qrr(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
        DELETE FROM { DOWNSTREAM_ASSET } 
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass

def duckdb_derive_transactor_flag_qrr(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    INSERT INTO { DOWNSTREAM_ASSET } BY NAME 
    FROM (
        SELECT
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            BASEL_ACCT_ID,
            SRC_SYS_CD,
            TRANSACTOR_FLAG_QRR
        FROM read_parquet([
            '{{{{ task_instance.xcom_pull(task_ids="derived__transactor_flag_qrr.export_ks", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__transactor_flag_qrr.export_spl", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__transactor_flag_qrr.export_mor", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__transactor_flag_qrr.export_tng", key="parquet") }}}}'
        ], union_by_name=true)
    )
    """
):
    pass
