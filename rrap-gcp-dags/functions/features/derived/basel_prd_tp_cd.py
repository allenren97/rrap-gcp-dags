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
                  'reference.BASEL_RPTG_PRD_LKP',
                  'reference.BASEL_EGL_LKP_NZ',
                  'features.PRD_ID',
                  'reference.PSNL_LOAN_RPTG_PRD_LKP',
                  'reference.MORT_RPTG_PRD_LKP',
                  'features.BULK_IND']

DOWNSTREAM_ASSET = 'features.BASEL_PRD_TP_CD'
DEPENDENCIES = {
    'export_ks': ['duckdb_clear_basel_prd_tp_cd'],
    'export_spl': ['duckdb_clear_basel_prd_tp_cd'],
    'export_mor': ['duckdb_clear_basel_prd_tp_cd'],
    'export_tng': ['duckdb_clear_basel_prd_tp_cd'],
    'duckdb_clear_basel_prd_tp_cd': ['duckdb_derive_basel_prd_tp_cd']
}

def export_ks(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
    WITH RPTG_PRD_LKP1 AS(
    SELECT
        TRIM(rptg.BASEL_PRD_TP_CD) AS BASEL_PRD_TP_CD,
        TRIM(rptg.PRD_ID) AS PRD_ID,
        TRIM(rptg.BASEL_SUB_PRD_NM) AS BASEL_SUB_PRD_NM,
        TRIM(rptg.BCAR_SCHED_NUM) AS BCAR_SCHED_NUM,
        TRIM(rptg.CCAR_BASEL_PRD_TP_NM) AS CCAR_BASEL_PRD_TP_NM0,
        TRIM(rptg.SCRTY_TP_CD) AS LKP_SCRTY_TP_CD,
        TRIM(rptg.PRD_CD) AS PRD_CD,
        TRIM(rptg.SUB_PRD_CD) AS SUB_PRD_CD0,
        TRIM(rptg.REVISED_EXPSR_OV_125K_F) AS REVISED_EXPSR_OV_125K_F,
        TRIM(rptg.HELOC_F) AS HELOC_F,
        TRIM(rptg.BASEL_PRD_CD) AS BASEL_PRD_CD,
        TRIM(rptg.SRC_SYS_CD) AS SRC_SYS_CD,
        LTRIM(RTRIM(rptg.BASEL_PRD_TP_CD)) AS basel_prd_tp_cd
    FROM {UPSTREAM_ASSET[10]} rptg
    LEFT JOIN {UPSTREAM_ASSET[11]} egl ON
        LTRIM(RTRIM(rptg.PRD_ID)) = LTRIM(RTRIM(egl.PRD_CD))
    WHERE 
        TRIM(rptg.SRC_SYS_CD) = 'KS'
        AND '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="yyyymm") }}}}' BETWEEN egl.EFF_FROM_YR_MTH AND egl.EFF_TO_YR_MTH
    )

    SELECT
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        ks.BASEL_ACCT_ID,
        'KS' AS SRC_SYS_CD,
        CASE 
            WHEN TRIM(pl1.BASEL_PRD_TP_CD) IS NOT NULL
            THEN TRIM(pl1.BASEL_PRD_TP_CD) 
            ELSE NULL
        END AS BASEL_PRD_TP_CD
    FROM {UPSTREAM_ASSET[1]} ks
    LEFT JOIN (SELECT * FROM {UPSTREAM_ASSET[5]} WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}') heloc ON
        ks.BASEL_ACCT_ID = heloc.BASEL_ACCT_ID
    LEFT JOIN (SELECT * FROM {UPSTREAM_ASSET[6]} WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}') prd_cd ON
        ks.BASEL_ACCT_ID = prd_cd.BASEL_ACCT_ID
    LEFT JOIN (SELECT * FROM {UPSTREAM_ASSET[7]} WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}') expsr ON
        ks.BASEL_ACCT_ID = expsr.BASEL_ACCT_ID
    LEFT JOIN RPTG_PRD_LKP1 pl1 ON
        ks.PRD_CD = pl1.PRD_CD
        AND ks.SUB_PRD_CD = pl1.SUB_PRD_CD0
        AND expsr.TOTAL_EXPSR_ABOVE_LMT_F = pl1.REVISED_EXPSR_OV_125K_F
        AND heloc.HELOC_F = pl1.HELOC_F
        AND prd_cd.BASEL_PRD_CD = pl1.BASEL_PRD_CD
    LEFT JOIN {UPSTREAM_ASSET[0]} fact ON
        ks.BASEL_ACCT_ID = fact.BASEL_ACCT_ID
        AND ks.MTH_TM_ID = fact.MTH_TM_ID
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
        CASE 
            WHEN TRIM(rptg.BASEL_PRD_TP_CD) IS NOT NULL
            THEN TRIM(rptg.BASEL_PRD_TP_CD) 
            ELSE NULL
        END AS BASEL_PRD_TP_CD
    FROM {UPSTREAM_ASSET[2]} spl
    LEFT JOIN {UPSTREAM_ASSET[12]} prd ON
        spl.BASEL_ACCT_ID = prd.BASEL_ACCT_ID
        AND prd.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    LEFT JOIN {UPSTREAM_ASSET[13]} rptg ON
        prd.PRD_ID = rptg.PRD_ID
        AND TRIM(rptg.SRC_SYS_CD) = 'SPL'
    WHERE 
        spl.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    GROUP BY
        spl.BASEL_ACCT_ID,
        rptg.BASEL_PRD_TP_CD
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
        CASE 
            WHEN TRIM(rptg.BASEL_PRD_TP_CD) IS NOT NULL
            THEN TRIM(rptg.BASEL_PRD_TP_CD) 
            ELSE NULL
        END AS BASEL_PRD_TP_CD
    FROM {UPSTREAM_ASSET[3]} mor
    LEFT JOIN {UPSTREAM_ASSET[15]} bulk ON
        mor.MORT_NUM = bulk.MORT_NUM
        AND bulk.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    LEFT JOIN {UPSTREAM_ASSET[14]} rptg ON
        rptg.SRC_SYS_CD = 'MOR'
        AND UPPER(mor.INSUR_GRP) = TRIM(UPPER(rptg.BASEL_MORT_INSURER_GRP_DESC))
        AND TRIM(bulk.BULK_IND) = TRIM(rptg.BULK_F)
    LEFT JOIN {UPSTREAM_ASSET[11]} egl ON
        UPPER(rptg.PRD_ID) = UPPER(egl.PRD_CD)
    WHERE 
        mor.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    GROUP BY
        mor.BASEL_ACCT_ID,
        rptg.BASEL_PRD_TP_CD
    """
):
    pass

def export_tng(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
    WITH bulk_ind AS(
        SELECT
            'TNG-MOR' AS SRC_SYS_CD,
            INSURER_DESC AS INSUR_GROUP,
            case 
                when UPPER(bulk_nsurer_desc)='BULKINSURED' then 'Y'
                else 'N'
            end as bulk_ind
            FROM {UPSTREAM_ASSET[8]}
            WHERE MONTH_END_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
        )

        SELECT
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        dim.BASEL_ACCT_ID,
        'TNG-MOR' AS SRC_SYS_CD,
        CASE 
            WHEN TRIM(rptg.BASEL_PRD_TP_CD) IS NOT NULL
            THEN TRIM(rptg.BASEL_PRD_TP_CD) 
            ELSE NULL
        END AS BASEL_PRD_TP_CD
    FROM {UPSTREAM_ASSET[8]} tng
    INNER JOIN {UPSTREAM_ASSET[9]} dim ON
        dim.SRC_APP_CD = 'TNG-MOR'
        AND dim.SRC_SYS_DEL_F != 'Y'
        AND TRIM(tng.ACCOUNT_ID) = TRIM(dim.SRC_APP_ID)
    INNER JOIN {UPSTREAM_ASSET[14]} rptg ON
        UPPER(tng.INSURER_DESC) = UPPER(rptg.BASEL_MORT_INSURER_GRP_DESC)
    INNER JOIN bulk_ind bulk ON
        UPPER(bulk.SRC_SYS_CD) = UPPER(rptg.SRC_SYS_CD)
        AND UPPER(bulk.INSUR_GROUP) = UPPER(rptg.BASEL_MORT_INSURER_GRP_DESC)
        AND UPPER(bulk.BULK_IND) = UPPER(rptg.BULK_F)
    LEFT JOIN {UPSTREAM_ASSET[11]} egl ON
        UPPER(rptg.PRD_ID) = UPPER(egl.PRD_CD)
    WHERE 
        tng.MONTH_END_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
        AND rptg.SRC_SYS_CD = 'TNG-MOR'
    GROUP BY
        dim.BASEL_ACCT_ID,
        TRIM(rptg.BASEL_PRD_TP_CD)
    """
):
    pass

def duckdb_clear_basel_prd_tp_cd(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
        DELETE FROM { DOWNSTREAM_ASSET } 
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass

def duckdb_derive_basel_prd_tp_cd(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    INSERT INTO { DOWNSTREAM_ASSET } BY NAME 
    FROM (
        SELECT
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            BASEL_ACCT_ID,
            SRC_SYS_CD,
            basel_prd_tp_cd
        FROM read_parquet([
            '{{{{ task_instance.xcom_pull(task_ids="derived__basel_prd_tp_cd.export_ks", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__basel_prd_tp_cd.export_spl", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__basel_prd_tp_cd.export_mor", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__basel_prd_tp_cd.export_tng", key="parquet") }}}}'
        ], union_by_name=true)
    )
    """
):
    pass
