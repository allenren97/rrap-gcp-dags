from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

UPSTREAM_ASSET = ['ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT',     #[0]
                  'ingestion.TM_DIM',                           #[1]
                  'ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT',    #[2]
                  'ingestion.SPL_SOURCE_FILE_ACCOUNTS',         #[3]
                  'ingestion.CC_SOURCE_FILE_ACCOUNTS',          #[4]
                  'ingestion.CL_SOURCE_FILE_ACCOUNTS',          #[5]
                  'features.CONSM_PRD_TREATMNT_CD',             #[6]
                  'features.PIT_STATUS_CROSS_DEFAULT_ORIG',     #[7]
                  'features.SML_BUS_F',                         #[8]
                  'features.TRNST_EXCLSN_F',                    #[9]
                  'features.HELOC_F',                           #[10]
                  'features.BASEL_PRD_CD',                      #[11]
                  'features.REVISED_EXPSR_OV_125K_F',           #[12]
                  'reference.BASEL_RPTG_PRD_LKP',               #[13]
                  'features.AF_ADJ_OS_BAL_AMT',                 #[14]
                  'features.TREATMENT_F',                       #[15]
                  'features.PRD_ID',                            #[16]
                  'features.OS_BAL_AMT_V2']                     #[17]

DOWNSTREAM_ASSET = "features.BASEL_ACCT_ID_CCAR_MATCHED"

DEPENDENCIES = {
    "export_auto": ["duckdb_delete_basel_acct_id_ccar_matched"],
    "export_cc": ["duckdb_delete_basel_acct_id_ccar_matched"],
    "export_cl": ["duckdb_delete_basel_acct_id_ccar_matched"],
    "duckdb_delete_basel_acct_id_ccar_matched": ["duckdb_load_basel_acct_id_ccar_matched"],
}

def export_auto(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    WITH SPL_ACCOUNTS_FROM_INST_FACT AS (
        SELECT
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            MTH_TM_ID, 
            PRD_CD, 
            OS_BAL_AMT, 
            BASEL_ACCT_ID, 
            ADJUSTED_OS_BAL_AMT,
            CAST(TRIM(CRNT_BR_LOCTN_TRNST::VARCHAR) || TRIM(LOAN_NUM::VARCHAR) AS BIGINT) AS UNIQUE_ACCOUNTS
        FROM (
            SELECT
                MTH_TM_ID,
                prd.PRD_ID AS PRD_CD,
                spl.TOT_CRNT_BAL_AMT AS OS_BAL_AMT,
                spl.BASEL_ACCT_ID,
                spl.CRNT_BR_LOCTN_TRNST, 
                spl.LOAN_NUM,
                CASE 
                    WHEN pit.PIT_STATUS_CROSS_DEFAULT_ORIG = 'CUR' 
                    AND treat.TREATMENT_F = 'A' 
                    AND trnst.TRNST_EXCLSN_F = 'N' 
                    AND prd.PRD_ID IN ('S09', 'S10') THEN af.AF_ADJ_OS_BAL_AMT 
                    ELSE os_bal.OS_BAL_AMT_V2 
                END AS ADJUSTED_OS_BAL_AMT, 
            FROM ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT spl
            LEFT JOIN {UPSTREAM_ASSET[14]} af ON
                spl.BASEL_ACCT_ID = af.BASEL_ACCT_ID
                AND af.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
            LEFT JOIN {UPSTREAM_ASSET[17]} os_bal ON
                spl.BASEL_ACCT_ID = os_bal.BASEL_ACCT_ID
                AND os_bal.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
            LEFT JOIN {UPSTREAM_ASSET[16]} prd ON
                spl.BASEL_ACCT_ID = prd.BASEL_ACCT_ID
                AND prd.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
            LEFT JOIN {UPSTREAM_ASSET[7]} pit ON
                spl.BASEL_ACCT_ID = pit.BASEL_ACCT_ID
                AND pit.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
            LEFT JOIN {UPSTREAM_ASSET[15]} treat ON
                spl.BASEL_ACCT_ID = treat.BASEL_ACCT_ID
                AND treat.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
            LEFT JOIN {UPSTREAM_ASSET[9]} trnst ON
                spl.BASEL_ACCT_ID = trnst.BASEL_ACCT_ID
                AND trnst.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
            WHERE
                spl.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
                AND spl.TOT_CRNT_BAL_AMT > 0
                AND treat.TREATMENT_F = 'A'
                AND pit.PIT_STATUS_CROSS_DEFAULT_ORIG IN ('CUR', 'DEF')
                AND trnst.TRNST_EXCLSN_F = 'N'
                AND prd.PRD_ID IN ('S09', 'S10')
                AND pit.SRC_SYS_CD = 'SPL'
            )
        )

        SELECT
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            'AUTO' AS SECRTZTN_TP_CD,
            spl.BASEL_ACCT_ID,
        FROM SPL_ACCOUNTS_FROM_INST_FACT spl
        INNER JOIN {UPSTREAM_ASSET[3]} auto ON
            auto.ACCOUNT_NUMBER = UNIQUE_ACCOUNTS
            AND auto.EFFECTIVE_DATE = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
        WHERE spl.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
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
            cc_if.BASEL_ACCT_ID
        FROM {UPSTREAM_ASSET[4]} cc 
        LEFT JOIN CC_ACCOUNTS_FROM_INSTR_FACT cc_if ON
            cc.VISA_ACCT_NUM = UNIQUE_ACCOUNTS
        WHERE cc.EFFECTIVE_DATE = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
        GROUP BY cc_if.BASEL_ACCT_ID
        
    """
):
    pass

def export_cl(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
        SELECT
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            'CL' as SECRTZTN_TP_CD,
            ks.BASEL_ACCT_ID
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

def duckdb_delete_basel_acct_id_ccar_matched(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        DELETE FROM {DOWNSTREAM_ASSET}
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """,
):
    pass

def duckdb_load_basel_acct_id_ccar_matched(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        INSERT INTO {DOWNSTREAM_ASSET} BY NAME
        FROM (
            SELECT
                '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
                SECRTZTN_TP_CD,
                BASEL_ACCT_ID AS BASEL_ACCT_ID_CCAR_MATCHED
            FROM read_parquet([
            '{{{{ task_instance.xcom_pull(task_ids="derived__basel_acct_id_ccar_matched.export_auto", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__basel_acct_id_ccar_matched.export_cc", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__basel_acct_id_ccar_matched.export_cl", key="parquet") }}}}'
        ], union_by_name=true)
    )
    """,
):
    pass