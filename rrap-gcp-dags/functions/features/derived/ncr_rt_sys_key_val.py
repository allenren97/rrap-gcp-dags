import pyarrow as pa
from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

UPSTREAM_ASSET = [
    "ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT", # 0
    "ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT",  # 1
    "ingestion.MORT_MTH_SNAPSHOT",             # 2
    "ingestion.BASEL_ACCT_DIM",                # 3
    "ingestion.BASELAYER_MOR",                 # 4
    "ingestion.TNG_ACCT_MO",                   # 5
    "features.PIT_STATUS_CROSS_DEFAULT_ORIG",  # 6
    "features.TRNST_EXCLSN_F",                 # 7
    "features.PRD_ID",                         # 8
    "features.TREATMENT_F",                    # 9
    "reference.PSNL_LOAN_RPTG_PRD_LKP",        # 10
    "features.TOTAL_EXPSR_ABOVE_LMT_F",        # 11
    "features.HELOC_F",                        # 12
    "features.BASEL_PRD_CD",                   # 13
    "reference.BASEL_RPTG_PRD_LKP",            # 14
    "features.SML_BUS_F"                       # 15
]

DOWNSTREAM_ASSET = 'features.NCR_RT_SYS_KEY_VAL'
DEPENDENCIES = {
    'export_ks': ['duckdb_clear_ncr_rt_sys_key_val'],
    'export_spl': ['duckdb_clear_ncr_rt_sys_key_val'],
    'export_mor': ['duckdb_clear_ncr_rt_sys_key_val'],
    'export_tng': ['duckdb_clear_ncr_rt_sys_key_val'],
    'duckdb_clear_ncr_rt_sys_key_val': ['duckdb_derive_ncr_rt_sys_key_val']
}

def export_ks(
    duckdb_conn_id='duckdb-conn',
    sql = rf"""
    WITH ks AS (
        SELECT
            PRD_CD,
            MTH_TM_ID,
            SUB_PRD_CD,
            BASEL_ACCT_ID
        FROM {UPSTREAM_ASSET[0]}
        WHERE MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    )
    SELECT
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        ks.BASEL_ACCT_ID,
        'KS' AS SRC_SYS_CD,
        CASE
            WHEN TRIM(sml.SML_BUS_F) = 'N' THEN '0201'
            ELSE NULL
        END AS NCR_RT_SYS_KEY_VAL
    FROM ks
    LEFT JOIN (SELECT * FROM {UPSTREAM_ASSET[11]} WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}') expsr
        ON ks.BASEL_ACCT_ID = expsr.BASEL_ACCT_ID
    LEFT JOIN (SELECT * FROM {UPSTREAM_ASSET[12]} WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}') heloc
        ON ks.BASEL_ACCT_ID = heloc.BASEL_ACCT_ID
    LEFT JOIN (SELECT * FROM {UPSTREAM_ASSET[13]} WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}') prd
        ON ks.BASEL_ACCT_ID = prd.BASEL_ACCT_ID
    LEFT JOIN {UPSTREAM_ASSET[14]} rptg
        ON TRIM(ks.PRD_CD) = TRIM(rptg.PRD_CD)
        AND TRIM(ks.SUB_PRD_CD) = TRIM(rptg.SUB_PRD_CD)
        AND TRIM(expsr.TOTAL_EXPSR_ABOVE_LMT_F) = TRIM(rptg.REVISED_EXPSR_OV_125K_F)
        AND TRIM(rptg.HELOC_F) = TRIM(heloc.HELOC_F)
        AND TRIM(rptg.BASEL_PRD_CD) = TRIM(prd.BASEL_PRD_CD)
    LEFT JOIN (SELECT * FROM features.SML_BUS_F WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}') sml
        ON ks.BASEL_ACCT_ID = sml.BASEL_ACCT_ID
    """
    ):
    pass

def export_spl(
    duckdb_conn_id='duckdb-conn',
    sql = rf"""
    SELECT
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        BASEL_ACCT_ID,
        '0201' AS NCR_RT_SYS_KEY_VAL,
        'SPL' AS SRC_SYS_CD
    FROM {UPSTREAM_ASSET[1]}
    WHERE MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    """   
    ):
    pass

def export_mor(
    duckdb_conn_id='duckdb-conn',
    sql = rf"""
        SELECT
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            BASEL_ACCT_ID,
            'MOR' AS SRC_SYS_CD,
            '0201' AS NCR_RT_SYS_KEY_VAL
        FROM {UPSTREAM_ASSET[2]}
        WHERE MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    """
    ):
    pass

def export_tng(
    duckdb_conn_id='duckdb-conn',
    sql = rf"""
    WITH tng AS (
            SELECT
                ACCOUNT_ID,
                MONTH_END_DT,
                RATE_TYPE_DESC
            FROM {UPSTREAM_ASSET[5]}
            WHERE MONTH_END_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
        )
        SELECT
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            'TNG-MOR' AS SRC_SYS_CD,
            dim.BASEL_ACCT_ID,
            '0207' AS NCR_RT_SYS_KEY_VAL
        FROM tng
        INNER JOIN {UPSTREAM_ASSET[3]} dim
            ON dim.SRC_APP_CD = 'TNG-MOR'
            AND dim.SRC_SYS_DEL_F != 'Y'
            AND TRIM(tng.ACCOUNT_ID) = TRIM(dim.SRC_APP_ID)
        WHERE MONTH_END_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
    ):
    pass

def duckdb_clear_ncr_rt_sys_key_val(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
        DELETE FROM { DOWNSTREAM_ASSET } 
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
    ):
    pass

def duckdb_derive_ncr_rt_sys_key_val(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
    INSERT INTO { DOWNSTREAM_ASSET } BY NAME 
    FROM (
        SELECT
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            BASEL_ACCT_ID,
            SRC_SYS_CD,
            NCR_RT_SYS_KEY_VAL
        FROM read_parquet([
            '{{{{ task_instance.xcom_pull(task_ids="derived__ncr_rt_sys_key_val.export_ks", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__ncr_rt_sys_key_val.export_spl", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__ncr_rt_sys_key_val.export_mor", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__ncr_rt_sys_key_val.export_tng", key="parquet") }}}}'
        ], union_by_name=true)
    )
    """
    ):
    pass