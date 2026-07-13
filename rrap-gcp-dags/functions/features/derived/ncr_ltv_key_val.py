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
    "ingestion.ORG_UNIT_DIM",                  # 5
    "ingestion.TNG_ACCT_MO",                   # 6
    "features.PIT_STATUS_CROSS_DEFAULT_ORIG",  # 7
    "features.TRNST_EXCLSN_F",                 # 8
    "features.CONSM_PRD_TREATMNT_CD",          # 9
    "features.SML_BUS_F",                      # 10
    "reference.PROVINCE_REF",                  # 11
    "reference.BASEL_NCR_LTV_DIM",             # 12
    "features.PRD_ID",                         # 13
    "features.TREATMENT_F",                    # 14
    "reference.PSNL_LOAN_RPTG_PRD_LKP",        # 15
    "reference.SRC_PRD_LKP",                   # 16
    "features.OS_BAL_AMT",                     # 17
    "ingestion.BASEL_STEP_PLN_MTH_SNAPSHOT"    # 18
]

DOWNSTREAM_ASSET = 'features.NCR_LTV_KEY_VAL'
DEPENDENCIES = {
    'duckdb_clear_ncr_ltv_key_val': ['export_ltvz'],
    'export_ltvz': ['export_ltvr'],
    'export_ltvr': ['export_ks'],
    'export_ks': ['export_spl'],
    'export_spl': ['export_mor'],
    'export_mor': ['export_tng'],
    'export_tng': ['duckdb_derive_ncr_ltv_key_val'],
}

def export_ltvz( # NCR_LTV_KEY_VAL0
    duckdb_conn_id='duckdb-conn',
    sql = rf"""
    WITH ks AS (
        SELECT BASEL_ACCT_ID
        FROM {UPSTREAM_ASSET[0]}
        WHERE MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    ),
    sml AS (
        SELECT * FROM {UPSTREAM_ASSET[10]}
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    ),
    prd AS (
        SELECT * FROM {UPSTREAM_ASSET[9]}
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    ),
    pit AS (
        SELECT * FROM {UPSTREAM_ASSET[7]}
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    )
    SELECT
        ks.BASEL_ACCT_ID,
        CASE
            WHEN TRIM(sml.SML_BUS_F) = 'N'
            AND TRIM(prd.CONSM_PRD_TREATMNT_CD) = 'A'
            AND TRIM(pit.PIT_STATUS_CROSS_DEFAULT_ORIG) IN ('CUR', 'DEF')
            THEN '0405'
            ELSE NULL
        END AS NCR_LTV_KEY_VAL
    FROM ks
    LEFT JOIN sml ON ks.BASEL_ACCT_ID = sml.BASEL_ACCT_ID
    LEFT JOIN prd ON ks.BASEL_ACCT_ID = prd.BASEL_ACCT_ID
    LEFT JOIN pit ON ks.BASEL_ACCT_ID = pit.BASEL_ACCT_ID
    """
    ):
    pass

def export_ltvr( # LOAN_TO_VAL_WITHOUT_INDEX_RTO
    duckdb_conn_id='duckdb-conn',
    sql = rf"""
    WITH drvd AS (
        SELECT
            ks.TOT_NEW_BAL_AMT,
            ks.STEP_PLN_AGRMNT_NUM,
            ks.STEP_PLN_SNAPSHOT_ID,
            ks.BASEL_ACCT_ID,
            lkp.LTV_TP_CD
        FROM {UPSTREAM_ASSET[0]} ks
        LEFT JOIN (SELECT * FROM {UPSTREAM_ASSET[10]} WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}') bus
            ON bus.BASEL_ACCT_ID = ks.BASEL_ACCT_ID
        AND ks.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
        LEFT JOIN {UPSTREAM_ASSET[16]} lkp
            ON TRIM(ks.PRD_CD) = TRIM(lkp.SRC_PRD_CD)
        AND TRIM(ks.SUB_PRD_CD) = TRIM(lkp.SRC_SUB_PRD_CD)
        AND TRIM(lkp.PRD_SYS_CD) = 'KS'
        AND TRIM(lkp.CRNT_F) = 'Y'
        WHERE bus.SML_BUS_F = 'N'
    ),
    rvl AS (
        SELECT
            STEP_PLN_AGRMNT_NUM,
            SUM(TOT_NEW_BAL_AMT) AS SUM_TOT_NEW_BAL_AMT
        FROM {UPSTREAM_ASSET[0]}
        WHERE MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
        AND STEP_PLN_SNAPSHOT_ID NOT IN(-1,-2)
            AND TRIM(PRD_CD) IN (
                SELECT SRC_PRD_CD
				FROM {UPSTREAM_ASSET[16]}
				WHERE PRD_SYS_CD = 'KS' AND CRNT_F = 'Y' AND LTV_TP_CD = 'LOC'
            )
        GROUP BY STEP_PLN_AGRMNT_NUM
    ),
    psnl AS (
        SELECT
            spl.STEP_PLN_AGRMNT_NUM,
            SUM(os.OS_BAL_AMT) AS SUM_OS_BAL_AMT
        FROM {UPSTREAM_ASSET[1]} spl
        LEFT JOIN (SELECT * FROM {UPSTREAM_ASSET[17]} WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}') os
            ON os.BASEL_ACCT_ID = spl.BASEL_ACCT_ID
        WHERE spl.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
        GROUP BY spl.STEP_PLN_AGRMNT_NUM
    ),
    mort AS (
        SELECT
            mor.STEP_PLN_AGRMNT_NUM,
            SUM(os.OS_BAL_AMT) AS SUM_OS_BAL_AMT
        FROM {UPSTREAM_ASSET[2]} mor
        LEFT JOIN (SELECT * FROM {UPSTREAM_ASSET[17]} WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}') os
            ON os.BASEL_ACCT_ID = mor.BASEL_ACCT_ID
        WHERE mor.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
        GROUP BY mor.STEP_PLN_AGRMNT_NUM
    ),
    step AS (
        SELECT
            APRSD_VAL,
            STEP_PLN_SNAPSHOT_ID
        FROM {UPSTREAM_ASSET[18]}
        WHERE MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    )
    SELECT
        drvd.BASEL_ACCT_ID,
        CASE
            WHEN step.APRSD_VAL IS NULL THEN NULL
            WHEN step.APRSD_VAL = 0 THEN 0
            ELSE ROUND((
                COALESCE(mort.SUM_OS_BAL_AMT, 0)
            + COALESCE(psnl.SUM_OS_BAL_AMT, 0)
            + CASE
                    WHEN TRIM(drvd.LTV_TP_CD) = 'LOC'
                        THEN drvd.TOT_NEW_BAL_AMT
                    WHEN TRIM(drvd.LTV_TP_CD) = 'VISA'
                        THEN drvd.TOT_NEW_BAL_AMT + COALESCE(rvl.SUM_TOT_NEW_BAL_AMT, 0)
                    ELSE 0
                END
            ) / step.APRSD_VAL, 4)
        END AS LOAN_TO_VAL_WITHOUT_INDEX_RTO
    FROM drvd
    LEFT JOIN step ON step.STEP_PLN_SNAPSHOT_ID = drvd.STEP_PLN_SNAPSHOT_ID
    LEFT JOIN mort ON TRIM(mort.STEP_PLN_AGRMNT_NUM) = TRIM(drvd.STEP_PLN_AGRMNT_NUM)
    LEFT JOIN psnl ON TRIM(psnl.STEP_PLN_AGRMNT_NUM) = TRIM(drvd.STEP_PLN_AGRMNT_NUM)
    LEFT JOIN rvl ON TRIM(rvl.STEP_PLN_AGRMNT_NUM) = TRIM(drvd.STEP_PLN_AGRMNT_NUM)
    """
    ):
    pass

def export_ks(
    duckdb_conn_id='duckdb-conn',
    sql = rf"""
    WITH ks AS (
        SELECT BASEL_ACCT_ID
        FROM {UPSTREAM_ASSET[0]}
        WHERE MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    )
    SELECT
        COALESCE(dim.NCR_LTV_KEY_VAL, ltvz.NCR_LTV_KEY_VAL) AS NCR_LTV_KEY_VAL,
        ks.BASEL_ACCT_ID,
        'KS' AS SRC_SYS_CD,
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT
    FROM ks
    LEFT JOIN '{{{{ task_instance.xcom_pull(task_ids="derived__ncr_ltv_key_val.export_ltvz", key="parquet") }}}}' ltvz
        ON ks.BASEL_ACCT_ID = ltvz.BASEL_ACCT_ID
    LEFT JOIN '{{{{ task_instance.xcom_pull(task_ids="derived__ncr_ltv_key_val.export_ltvr", key="parquet") }}}}' ltvr
        ON ks.BASEL_ACCT_ID = ltvr.BASEL_ACCT_ID
    LEFT JOIN {UPSTREAM_ASSET[12]} dim
        ON dim.LTV_RTO_MIN_VAL <= ltvr.LOAN_TO_VAL_WITHOUT_INDEX_RTO
        AND dim.LTV_RTO_MAX_VAL > ltvr.LOAN_TO_VAL_WITHOUT_INDEX_RTO
    """
    ):
    pass

def export_spl(
    duckdb_conn_id='duckdb-conn',
    sql = rf"""
    SELECT
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        BASEL_ACCT_ID,
        '0405' AS NCR_LTV_KEY_VAL,
        'SPL' AS SRC_SYS_CD
    FROM {UPSTREAM_ASSET[1]}
    WHERE MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    """   
    ):
    pass

def export_mor(
    duckdb_conn_id='duckdb-conn',
    sql = rf"""
    WITH mor AS (
        SELECT
            BASEL_ACCT_ID,
            MTH_TM_ID,
            CASE
                WHEN LND_VAL = 0 THEN NULL
                WHEN LND_VAL IS NULL THEN NULL
                ELSE CASE
                    WHEN CRNT_BAL_AMT + INTR_ACCR_AMT != 0
                        THEN (CRNT_BAL_AMT + INTR_ACCR_AMT) / LND_VAL
                    ELSE (-TOT_SUSP_BAL_AMT) / LND_VAL
                END
            END AS LOAN_TO_VAL_RTO
        FROM {UPSTREAM_ASSET[2]}
        WHERE MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    )
    SELECT
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        mor.BASEL_ACCT_ID,
        COALESCE(dim.NCR_LTV_KEY_VAL, '0405') AS NCR_LTV_KEY_VAL,
        'MOR' AS SRC_SYS_CD
    FROM mor
    LEFT JOIN {UPSTREAM_ASSET[12]} dim
        ON mor.LOAN_TO_VAL_RTO >= dim.LTV_RTO_MIN_VAL
    AND mor.LOAN_TO_VAL_RTO < dim.LTV_RTO_MAX_VAL
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
            CASE
                WHEN LOAN_TO_VALUE_RATIO / 100 > 1 THEN 1
                ELSE LOAN_TO_VALUE_RATIO / 100
            END AS LOAN_TO_VAL_RTO
        FROM {UPSTREAM_ASSET[6]}
        WHERE MONTH_END_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    )
    SELECT
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        'TNG-MOR' AS SRC_SYS_CD,
        dim.BASEL_ACCT_ID,
        COALESCE(ltv.NCR_LTV_KEY_VAL, '0405') AS NCR_LTV_KEY_VAL
    FROM tng
    INNER JOIN {UPSTREAM_ASSET[3]} dim
        ON dim.SRC_APP_CD = 'TNG-MOR'
    AND dim.SRC_SYS_DEL_F != 'Y'
    AND TRIM(tng.ACCOUNT_ID) = TRIM(dim.SRC_APP_ID)
    LEFT JOIN {UPSTREAM_ASSET[12]} ltv
        ON tng.LOAN_TO_VAL_RTO >= ltv.LTV_RTO_MIN_VAL
    AND tng.LOAN_TO_VAL_RTO < ltv.LTV_RTO_MAX_VAL
    WHERE MONTH_END_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
    ):
    pass

def duckdb_clear_ncr_ltv_key_val(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
        DELETE FROM { DOWNSTREAM_ASSET } 
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
    ):
    print(sql)

def duckdb_derive_ncr_ltv_key_val(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
    INSERT INTO { DOWNSTREAM_ASSET } BY NAME 
    FROM (
        SELECT
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            BASEL_ACCT_ID,
            SRC_SYS_CD,
            NCR_LTV_KEY_VAL
        FROM read_parquet([
            '{{{{ task_instance.xcom_pull(task_ids="derived__ncr_ltv_key_val.export_ks", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__ncr_ltv_key_val.export_spl", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__ncr_ltv_key_val.export_mor", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__ncr_ltv_key_val.export_tng", key="parquet") }}}}'
        ], union_by_name=true)
    )
    """
    ):
    pass