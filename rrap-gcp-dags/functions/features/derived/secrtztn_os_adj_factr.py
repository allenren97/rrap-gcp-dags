import os
from datetime import timedelta
import pendulum

from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import _pull_asset_event_extras, _push_asset_event_extras


UPSTREAM_ASSET = ['features.ADJUSTED_OS_BAL_AMT_SECURITIZATION',
                  'ingestion.SPL_SOURCE_FILE_AMOUNT',
                  'features.AF_ADJ_OS_BAL_AMT',
                  'ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT',
                  'features.TOT_SECRTZTN_AMT_TO_ADJUST',
                  'ingestion.CC_SOURCE_FILE_ACCOUNTS',
                  'features.CONSM_PRD_TREATMNT_CD',
                  'features.PIT_STATUS_CROSS_DEFAULT_ORIG',
                  'features.SML_BUS_F',
                  'features.TRNST_EXCLSN_F',
                  'features.HELOC_F',
                  'features.BASEL_PRD_CD',
                  'features.REVISED_EXPSR_OV_125K_F',
                  'reference.BASEL_RPTG_PRD_LKP']

DOWNSTREAM_ASSET = "features.SECRTZTN_OS_ADJ_FACTR"

DEPENDENCIES = {
    'export_auto': ['duckdb_clear_derive_secrtztn_os_adj_factr'],
    'export_cc': ['duckdb_clear_derive_secrtztn_os_adj_factr'],
    'export_cl': ['duckdb_clear_derive_secrtztn_os_adj_factr'],
    'duckdb_clear_derive_secrtztn_os_adj_factr': ['duckdb_derive_secrtztn_os_adj_factr'],
}

def export_auto(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
        SELECT
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate")  }}}}' AS OBSN_DT,
            'AUTO' AS SECRTZTN_TP_CD,
            reduction_rate AS SECRTZTN_OS_ADJ_FACTR
        FROM {UPSTREAM_ASSET[1]}
        WHERE EFFECTIVE_DATE = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate")  }}}}'
    """
):
    pass

def export_cc(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
        SELECT
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate")  }}}}' AS OBSN_DT,
            'CC' AS SECRTZTN_TP_CD,
            adj.TOT_SECRTZTN_AMT_TO_ADJUST / SUM(af.AF_ADJ_OS_BAL_AMT) AS SECRTZTN_OS_ADJ_FACTR
        FROM {UPSTREAM_ASSET[4]} adj
        LEFT JOIN {UPSTREAM_ASSET[2]} af ON
            adj.OBSN_DT = af.OBSN_DT
        LEFT JOIN {UPSTREAM_ASSET[3]} ks ON 
            ks.BASEL_ACCT_ID = af.BASEL_ACCT_ID
        INNER JOIN {UPSTREAM_ASSET[5]} cc ON
            CAST(SUBSTR(CONCAT('CA', '0201', ks.ACCT_NUM),17,13) AS BIGINT) = cc.VISA_ACCT_NUM
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
            cc.EFFECTIVE_DATE = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' 
            AND adj.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
            AND ks.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
            AND adj.SECRTZTN_TP_CD = 'CC'
            AND ks.TOT_NEW_BAL_AMT >= 0
            AND consm.CONSM_PRD_TREATMNT_CD = 'A'
            AND sml.SML_BUS_F = 'N'
            AND pit.PIT_STATUS_CROSS_DEFAULT_ORIG IN ('CUR','DEF')
            AND trnst.TRNST_EXCLSN_F = 'N'
            AND rptg.BASEL_PRD_TP_CD = 'CARD'
        GROUP BY adj.TOT_SECRTZTN_AMT_TO_ADJUST
    """
):
    pass
 
def export_cl(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
        SELECT
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate")  }}}}' AS OBSN_DT,
            'CL' AS SECRTZTN_TP_CD,
            COALESCE((adj.TOT_SECRTZTN_AMT_TO_ADJUST / os_bal.SUM_ADJ_OS_BAL), 0) AS SECRTZTN_OS_ADJ_FACTR
        FROM {UPSTREAM_ASSET[4]} adj
        LEFT JOIN (SELECT OBSN_DT, SUM(ADJUSTED_OS_BAL_AMT_SECURITIZATION) AS SUM_ADJ_OS_BAL FROM {UPSTREAM_ASSET[0]} WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate")  }}}}' GROUP BY OBSN_DT) os_bal ON
            adj.OBSN_DT = os_bal.OBSN_DT
        WHERE 
            adj.SECRTZTN_TP_CD = 'CL'
            AND adj.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate")  }}}}'

    """
):
    pass

def duckdb_clear_derive_secrtztn_os_adj_factr(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    DELETE FROM { DOWNSTREAM_ASSET } 
    WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass

def duckdb_derive_secrtztn_os_adj_factr(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET}
        SELECT 
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate")  }}}}' AS OBSN_DT,
            SECRTZTN_TP_CD,
            SECRTZTN_OS_ADJ_FACTR
        FROM read_parquet([
            '{{{{ task_instance.xcom_pull(task_ids="derived__secrtztn_os_adj_factr.export_auto", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__secrtztn_os_adj_factr.export_cc", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__secrtztn_os_adj_factr.export_cl", key="parquet") }}}}'
        ], union_by_name=true)
    """
):
    pass