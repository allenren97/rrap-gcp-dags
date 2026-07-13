import os
from datetime import timedelta
import pendulum

from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import _pull_asset_event_extras, _push_asset_event_extras


UPSTREAM_ASSET = ['ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT',
                  'ingestion.TM_DIM',
                  'ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT',
                  'ingestion.SPL_SOURCE_FILE_ACCOUNTS',
                  'ingestion.CC_SOURCE_FILE_ACCOUNTS',
                  'ingestion.CL_SOURCE_FILE_ACCOUNTS',
                  'features.CONSM_PRD_TREATMNT_CD',
                  'features.PIT_STATUS_CROSS_DEFAULT_ORIG',
                  'features.SML_BUS_F',
                  'features.TRNST_EXCLSN_F',
                  'features.HELOC_F',
                  'features.BASEL_PRD_CD',
                  'features.REVISED_EXPSR_OV_125K_F',
                  'reference.BASEL_RPTG_PRD_LKP',
                  'features.ADJUSTED_OS_BAL_AMT_SECURITIZATION',
                  'features.AF_ADJ_OS_BAL_AMT'
]

DOWNSTREAM_ASSET = "features.TOT_ADJUSTED_OS_BAL_IN_CCAR"
DEPENDENCIES = {
    'export_auto': ['duckdb_clear_tot_adjusted_os_bal_in_ccar'],
    'export_cc': ['duckdb_clear_tot_adjusted_os_bal_in_ccar'],
    'export_cl': ['duckdb_clear_tot_adjusted_os_bal_in_ccar'],
    'duckdb_clear_tot_adjusted_os_bal_in_ccar': ['duckdb_load_tot_adjusted_os_bal_in_ccar'],
}


def export_auto(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
        SELECT
            OBSN_DT,
            'AUTO' AS SECRTZTN_TP_CD,
            TRUNC(SUM(ADJUSTED_OS_BAL_AMT_SECURITIZATION), 2) AS TOT_ADJUSTED_OS_BAL_IN_CCAR
        FROM {UPSTREAM_ASSET[14]} spl
        INNER JOIN {UPSTREAM_ASSET[3]} b ON 
            b.ACCOUNT_NUMBER = spl.ACCOUNT_NUMBER
            AND spl.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
        WHERE b.EFFECTIVE_DATE = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
        GROUP BY OBSN_DT
    """
):
    pass


def export_cc(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    WITH CC_ACCOUNTS_FROM_INSTR_FACT AS (
        SELECT
            MTH_TM_ID,
            BASEL_ACCT_ID,
            TRY_CAST(SUBSTR(CONCAT('CA','0201',TRIM(ACCT_NUM)), 17, 13) AS DECIMAL(15,0)) AS UNIQUE_ACCOUNTS
        FROM (
            SELECT
                ks.MTH_TM_ID,
                ks.BASEL_ACCT_ID,
                ACCT_NUM
            FROM {UPSTREAM_ASSET[2]} ks
            LEFT JOIN {UPSTREAM_ASSET[6]} consm ON
                ks.BASEL_ACCT_ID = consm.BASEL_ACCT_ID
                AND consm.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
            LEFT JOIN {UPSTREAM_ASSET[7]} pit ON
                ks.BASEL_ACCT_ID = pit.BASEL_ACCT_ID
                AND pit.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
            LEFT JOIN {UPSTREAM_ASSET[8]} sml ON
                ks.BASEL_ACCT_ID = sml.BASEL_ACCT_ID
                AND sml.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
            LEFT JOIN {UPSTREAM_ASSET[9]} trnst ON
                ks.BASEL_ACCT_ID = trnst.BASEL_ACCT_ID
                AND trnst.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
            LEFT JOIN (SELECT * FROM {UPSTREAM_ASSET[10]} WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}') heloc ON
                ks.BASEL_ACCT_ID = heloc.BASEL_ACCT_ID
            LEFT JOIN (SELECT * FROM {UPSTREAM_ASSET[11]} WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}') prd_cd ON
                ks.BASEL_ACCT_ID = prd_cd.BASEL_ACCT_ID
            LEFT JOIN (SELECT * FROM {UPSTREAM_ASSET[12]} WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}') expsr ON
                ks.BASEL_ACCT_ID = expsr.BASEL_ACCT_ID
            LEFT JOIN {UPSTREAM_ASSET[13]} rptg ON
                TRIM(ks.PRD_CD) = TRIM(rptg.PRD_CD)
                AND TRIM(ks.SUB_PRD_CD) = TRIM(rptg.SUB_PRD_CD)
                AND TRIM(heloc.HELOC_F) = TRIM(rptg.HELOC_F)
                AND TRIM(prd_cd.BASEL_PRD_CD) = TRIM(rptg.BASEL_PRD_CD)
                AND TRIM(rptg.REVISED_EXPSR_OV_125K_F) = TRIM(expsr.REVISED_EXPSR_OV_125K_F)
            WHERE 
                ks.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
                AND ks.TOT_NEW_BAL_AMT >= 0
                AND consm.CONSM_PRD_TREATMNT_CD = 'A'
                AND sml.SML_BUS_F = 'N'
                AND pit.PIT_STATUS_CROSS_DEFAULT_ORIG IN ('CUR','DEF')
                AND trnst.TRNST_EXCLSN_F = 'N'
                AND rptg.BASEL_PRD_TP_CD = 'CARD'
            )
        )

        SELECT
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            'CC' as SECRTZTN_TP_CD,
            TRUNC(SUM(af.AF_ADJ_OS_BAL_AMT), 2) AS TOT_ADJUSTED_OS_BAL_IN_CCAR
        FROM {UPSTREAM_ASSET[4]} cc 
        INNER JOIN CC_ACCOUNTS_FROM_INSTR_FACT cc_if ON
            cc.VISA_ACCT_NUM = UNIQUE_ACCOUNTS
            AND cc.EFFECTIVE_DATE = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
        LEFT JOIN {UPSTREAM_ASSET[15]} af ON
            cc_if.BASEL_ACCT_ID = af.BASEL_ACCT_ID
            AND af.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
        
    """
):
    pass

def export_cl(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
        SELECT
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            'CL' AS SECRTZTN_TP_CD,
            TRUNC(SUM(af.AF_ADJ_OS_BAL_AMT), 2) AS TOT_ADJUSTED_OS_BAL_IN_CCAR
        FROM {UPSTREAM_ASSET[5]} cl 
        INNER JOIN {UPSTREAM_ASSET[2]} ks ON
            cl.VISA_ACCT_NUM = CAST(LTRIM(TRIM(ks.ACCT_NUM), '0') AS BIGINT)
        LEFT JOIN {UPSTREAM_ASSET[6]} consm ON
            ks.BASEL_ACCT_ID = consm.BASEL_ACCT_ID
            AND consm.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
        LEFT JOIN {UPSTREAM_ASSET[7]} pit ON
            ks.BASEL_ACCT_ID = pit.BASEL_ACCT_ID
            AND pit.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
        LEFT JOIN {UPSTREAM_ASSET[8]} sml ON
            ks.BASEL_ACCT_ID = sml.BASEL_ACCT_ID
            AND sml.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
        LEFT JOIN {UPSTREAM_ASSET[9]} trnst ON
            ks.BASEL_ACCT_ID = trnst.BASEL_ACCT_ID
            AND trnst.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
        LEFT JOIN {UPSTREAM_ASSET[15]} af ON
            ks.BASEL_ACCT_ID = af.BASEL_ACCT_ID
            AND af.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
        LEFT OUTER JOIN (SELECT * FROM {UPSTREAM_ASSET[10]} WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}') heloc ON
            ks.BASEL_ACCT_ID = heloc.BASEL_ACCT_ID
        LEFT OUTER JOIN (SELECT * FROM {UPSTREAM_ASSET[11]} WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}') prd_cd ON
            ks.BASEL_ACCT_ID = prd_cd.BASEL_ACCT_ID
        LEFT OUTER JOIN (SELECT * FROM {UPSTREAM_ASSET[12]} WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}') expsr ON
            ks.BASEL_ACCT_ID = expsr.BASEL_ACCT_ID
        LEFT OUTER JOIN {UPSTREAM_ASSET[13]} rptg ON
            TRIM(ks.PRD_CD) = TRIM(rptg.PRD_CD)
            AND TRIM(ks.SUB_PRD_CD) = TRIM(rptg.SUB_PRD_CD)
            AND TRIM(heloc.HELOC_F) = TRIM(rptg.HELOC_F)
            AND TRIM(prd_cd.BASEL_PRD_CD) = TRIM(rptg.BASEL_PRD_CD)
            AND TRIM(rptg.REVISED_EXPSR_OV_125K_F) = TRIM(expsr.REVISED_EXPSR_OV_125K_F)
        WHERE 
            cl.EFFECTIVE_DATE = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' 
            AND ks.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
            AND ks.TOT_NEW_BAL_AMT >= 0
            AND consm.CONSM_PRD_TREATMNT_CD = 'A'
            AND sml.SML_BUS_F = 'N'
            AND pit.PIT_STATUS_CROSS_DEFAULT_ORIG IN ('CUR','DEF')
            AND trnst.TRNST_EXCLSN_F = 'N'
            AND rptg.PRD_ID IN ('KS33','KS35','KS123','KS125')
    """
):
    pass


def duckdb_clear_tot_adjusted_os_bal_in_ccar(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    DELETE FROM { DOWNSTREAM_ASSET } 
    WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass


def duckdb_load_tot_adjusted_os_bal_in_ccar(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
        INSERT INTO {DOWNSTREAM_ASSET} by name 
            FROM (
                SELECT 
                    OBSN_DT,
                    SECRTZTN_TP_CD,
                    TOT_ADJUSTED_OS_BAL_IN_CCAR
                FROM read_parquet([
                    '{{{{ task_instance.xcom_pull(task_ids="derived__tot_adjusted_os_bal_in_ccar.export_auto", key="parquet") }}}}',
                    '{{{{ task_instance.xcom_pull(task_ids="derived__tot_adjusted_os_bal_in_ccar.export_cc", key="parquet") }}}}',
                    '{{{{ task_instance.xcom_pull(task_ids="derived__tot_adjusted_os_bal_in_ccar.export_cl", key="parquet") }}}}'
                ], union_by_name=true)
            ) 
    """
):
    pass