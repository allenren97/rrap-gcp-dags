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
    "reference.BASEL_RPTG_PRD_LKP"             # 14
]

DOWNSTREAM_ASSET = 'features.NCR_RT_KEY_VAL'
DEPENDENCIES = {
    'export_ks': ['duckdb_clear_ncr_rt_key_val'],
    'export_spl': ['duckdb_clear_ncr_rt_key_val'],
    'export_mor': ['duckdb_clear_ncr_rt_key_val'],
    'export_tng': ['duckdb_clear_ncr_rt_key_val'],
    'duckdb_clear_ncr_rt_key_val': ['duckdb_derive_ncr_rt_key_val']
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
        TRIM(rptg.RT_KEY_VAL) AS NCR_RT_KEY_VAL
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
    """
    ):
    pass

def export_spl(
    duckdb_conn_id='duckdb-conn',
    sql = rf"""
        WITH RPTG_DRVD_VARS AS (
            SELECT
                spl.BASEL_ACCT_ID,
                lkp.NCR_RT_KEY_VAL
            FROM {UPSTREAM_ASSET[1]} spl
            INNER JOIN {UPSTREAM_ASSET[7]} tef ON
                spl.BASEL_ACCT_ID = tef.BASEL_ACCT_ID
                AND tef.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
                AND tef.TRNST_EXCLSN_F = 'N'
            INNER JOIN {UPSTREAM_ASSET[8]} prd ON
                spl.BASEL_ACCT_ID = prd.BASEL_ACCT_ID
                AND prd.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
            INNER JOIN {UPSTREAM_ASSET[9]} tf ON
                spl.BASEL_ACCT_ID = tf.BASEL_ACCT_ID
                AND tf.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
                AND tf.TREATMENT_F = 'A'
            INNER JOIN {UPSTREAM_ASSET[6]} pit ON
                spl.BASEL_ACCT_ID = pit.BASEL_ACCT_ID
                AND pit.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
                AND pit.PIT_STATUS_CROSS_DEFAULT_ORIG IN ('DEF', 'CUR')
            LEFT JOIN {UPSTREAM_ASSET[10]} lkp ON
                prd.PRD_ID = lkp.PRD_ID
                AND TRIM(lkp.SRC_SYS_CD) = 'SPL'
            WHERE spl.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
        )
        SELECT
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            spl.BASEL_ACCT_ID,
            COALESCE(rptg.NCR_RT_KEY_VAL, '0798') AS NCR_RT_KEY_VAL,
            'SPL' AS SRC_SYS_CD
        FROM {UPSTREAM_ASSET[1]} spl
        LEFT JOIN RPTG_DRVD_VARS rptg ON
            spl.BASEL_ACCT_ID = rptg.BASEL_ACCT_ID
        WHERE spl.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    """   
    ):
    pass

def export_mor(
    duckdb_conn_id='duckdb-conn',
    sql = rf"""
        SELECT
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            mor.BASEL_ACCT_ID,
            'MOR' AS SRC_SYS_CD,
            TRIM(base.NCR_RATE) AS NCR_RT_KEY_VAL
        FROM {UPSTREAM_ASSET[2]} mor
        INNER JOIN {UPSTREAM_ASSET[4]} base
            ON mor.MORT_NUM = base.MORT_NUM
            AND mor.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
            AND base.MTH_END_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
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
            CASE
                WHEN TRIM(UPPER(tng.RATE_TYPE_DESC)) = 'FIXED' THEN '0701'
                WHEN TRIM(UPPER(tng.RATE_TYPE_DESC)) = 'VARIABLE' THEN '0702'
            END AS NCR_RT_KEY_VAL
        FROM tng
        INNER JOIN {UPSTREAM_ASSET[3]} dim
            ON dim.SRC_APP_CD = 'TNG-MOR'
            AND dim.SRC_SYS_DEL_F != 'Y'
            AND TRIM(tng.ACCOUNT_ID) = TRIM(dim.SRC_APP_ID)
        WHERE MONTH_END_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
    ):
    pass

def duckdb_clear_ncr_rt_key_val(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
        DELETE FROM { DOWNSTREAM_ASSET } 
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
    ):
    pass

def duckdb_derive_ncr_rt_key_val(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
    INSERT INTO { DOWNSTREAM_ASSET } BY NAME 
    FROM (
        SELECT
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            BASEL_ACCT_ID,
            SRC_SYS_CD,
            NCR_RT_KEY_VAL
        FROM read_parquet([
            '{{{{ task_instance.xcom_pull(task_ids="derived__ncr_rt_key_val.export_ks", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__ncr_rt_key_val.export_spl", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__ncr_rt_key_val.export_mor", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__ncr_rt_key_val.export_tng", key="parquet") }}}}'
        ], union_by_name=true)
    )
    """
    ):
    pass