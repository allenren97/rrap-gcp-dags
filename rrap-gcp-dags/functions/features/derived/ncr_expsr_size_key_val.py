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
    "features.TREATMENT_F",                    # 8
    "reference.BASEL_NCR_EXPSR_SIZE_DIM",      # 9
    "features.CONSM_PRD_TREATMNT_CD",          # 10
    "features.SML_BUS_F",                      # 11
    "features.OS_BAL_AMT_V2"                   # 12
]

DOWNSTREAM_ASSET = 'features.NCR_EXPSR_SIZE_KEY_VAL'
DEPENDENCIES = {
    'export_ks': ['duckdb_clear_ncr_expsr_size_key_val'],
    'export_spl': ['duckdb_clear_ncr_expsr_size_key_val'],
    'export_mor': ['duckdb_clear_ncr_expsr_size_key_val'],
    'export_tng': ['duckdb_clear_ncr_expsr_size_key_val'],
    'duckdb_clear_ncr_expsr_size_key_val': ['duckdb_derive_ncr_expsr_size_key_val']
}

def export_ks(
    duckdb_conn_id='duckdb-conn',
    sql = rf"""
        WITH ks AS (
            SELECT
                MTH_TM_ID,
                BASEL_ACCT_ID,
                TOT_NEW_BAL_AMT
            FROM {UPSTREAM_ASSET[0]}
            WHERE MTH_TM_ID = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}'
        ), dim AS (
            SELECT
                dim.NCR_KEY_VAL,
                ks.BASEL_ACCT_ID
            FROM {UPSTREAM_ASSET[9]} dim
            INNER JOIN ks
                ON dim.MIN_BAL_AMT <= ks.TOT_NEW_BAL_AMT
                AND dim.MAX_BAL_AMT > ks.TOT_NEW_BAL_AMT
            INNER JOIN (SELECT * FROM {UPSTREAM_ASSET[10]} WHERE OBSN_DT='{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}') cd
                ON ks.BASEL_ACCT_ID = cd.BASEL_ACCT_ID
            INNER JOIN (SELECT * FROM {UPSTREAM_ASSET[11]} WHERE OBSN_DT='{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}') bus
                ON ks.BASEL_ACCT_ID = bus.BASEL_ACCT_ID
            INNER JOIN (SELECT * FROM {UPSTREAM_ASSET[6]} WHERE OBSN_DT='{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}') pit
                ON ks.BASEL_ACCT_ID = pit.BASEL_ACCT_ID
            INNER JOIN (SELECT * FROM {UPSTREAM_ASSET[7]} WHERE OBSN_DT='{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}') trn
                ON ks.BASEL_ACCT_ID = trn.BASEL_ACCT_ID
            WHERE dim.FRS_CD IS NOT NULL
                AND trn.TRNST_EXCLSN_F='N'
                AND pit.PIT_STATUS_CROSS_DEFAULT_ORIG IN ('CUR','DEF')
                AND bus.SML_BUS_F='N'
                AND cd.CONSM_PRD_TREATMNT_CD='A'
        )
        SELECT
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            ks.BASEL_ACCT_ID,
            'KS' AS SRC_SYS_CD,
            dim.NCR_KEY_VAL AS NCR_EXPSR_SIZE_KEY_VAL
        FROM dim
        RIGHT JOIN ks
            ON ks.BASEL_ACCT_ID = dim.BASEL_ACCT_ID
    """
    ):
    pass

def export_spl(
    duckdb_conn_id='duckdb-conn',
    sql = rf"""
        WITH spl AS (
            SELECT
                BASEL_ACCT_ID
            FROM {UPSTREAM_ASSET[1]}
            WHERE MTH_TM_ID = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}'
        ), rptg AS (
            SELECT
                spl.BASEL_ACCT_ID,
                os.OS_BAL_AMT_V2
            FROM spl
            INNER JOIN (SELECT * FROM {UPSTREAM_ASSET[7]} WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AND TRNST_EXCLSN_F = 'N') tef
                ON spl.BASEL_ACCT_ID = tef.BASEL_ACCT_ID
            INNER JOIN (SELECT * FROM {UPSTREAM_ASSET[8]} WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AND TREATMENT_F = 'A') tf
                ON spl.BASEL_ACCT_ID = tf.BASEL_ACCT_ID
            INNER JOIN (SELECT * FROM {UPSTREAM_ASSET[6]} WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AND PIT_STATUS_CROSS_DEFAULT_ORIG IN ('DEF', 'CUR')) pit
                ON spl.BASEL_ACCT_ID = pit.BASEL_ACCT_ID
            LEFT JOIN (SELECT * FROM {UPSTREAM_ASSET[12]} WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}') os
                ON spl.BASEL_ACCT_ID = os.BASEL_ACCT_ID
        ), dim AS (
            SELECT
                rptg.BASEL_ACCT_ID,
                dim.NCR_EXPSR_SIZE_ID,
                dim.NCR_KEY_VAL
            FROM rptg
            LEFT JOIN {UPSTREAM_ASSET[9]} dim
                ON dim.MIN_BAL_AMT <= COALESCE(rptg.OS_BAL_AMT_V2, 0)
                AND dim.MAX_BAL_AMT > COALESCE(rptg.OS_BAL_AMT_V2, 0)
            WHERE dim.FRS_CD IS NOT NULL
                AND '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="yyyymm") }}}}' BETWEEN dim.EFF_FROM_YR_MTH AND dim.EFF_TO_YR_MTH
        )
        SELECT
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            spl.BASEL_ACCT_ID,
            COALESCE(dim.NCR_KEY_VAL, '1598') AS NCR_EXPSR_SIZE_KEY_VAL,
            'SPL' AS SRC_SYS_CD
        FROM spl
        LEFT JOIN dim ON
            spl.BASEL_ACCT_ID = dim.BASEL_ACCT_ID
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
            TRIM(base.NCR_EXPOSURE_SIZE) AS NCR_EXPSR_SIZE_KEY_VAL
        FROM {UPSTREAM_ASSET[2]} mor
        INNER JOIN {UPSTREAM_ASSET[4]} base
            ON mor.MORT_NUM = base.MORT_NUM
            AND mor.MTH_TM_ID = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}'
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
                END_PRINCIPAL_BALANCE
            FROM {UPSTREAM_ASSET[5]}
            WHERE MONTH_END_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
        )
        SELECT
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            'TNG-MOR' AS SRC_SYS_CD,
            acc.BASEL_ACCT_ID,
            dim.NCR_KEY_VAL AS NCR_EXPSR_SIZE_KEY_VAL
        FROM tng
        INNER JOIN {UPSTREAM_ASSET[3]} acc
            ON acc.SRC_APP_CD = 'TNG-MOR'
            AND acc.SRC_SYS_DEL_F != 'Y'
            AND TRIM(tng.ACCOUNT_ID) = TRIM(acc.SRC_APP_ID)
        LEFT JOIN {UPSTREAM_ASSET[9]} dim
            ON dim.MIN_BAL_AMT <= tng.END_PRINCIPAL_BALANCE
            AND dim.MAX_BAL_AMT > tng.END_PRINCIPAL_BALANCE
        WHERE tng.MONTH_END_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
            AND dim.FRS_CD IS NOT NULL
    """
    ):
    pass

def duckdb_clear_ncr_expsr_size_key_val(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
        DELETE FROM { DOWNSTREAM_ASSET } 
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
    ):
    pass

def duckdb_derive_ncr_expsr_size_key_val(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
    INSERT INTO { DOWNSTREAM_ASSET } BY NAME 
    FROM (
        SELECT
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            BASEL_ACCT_ID,
            SRC_SYS_CD,
            NCR_EXPSR_SIZE_KEY_VAL
        FROM read_parquet([
            '{{{{ task_instance.xcom_pull(task_ids="derived__ncr_expsr_size_key_val.export_ks", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__ncr_expsr_size_key_val.export_spl", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__ncr_expsr_size_key_val.export_mor", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__ncr_expsr_size_key_val.export_tng", key="parquet") }}}}'
        ], union_by_name=true)
    )
    """
    ):
    pass
