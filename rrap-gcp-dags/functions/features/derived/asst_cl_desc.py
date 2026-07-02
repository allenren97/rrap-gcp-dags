import pyarrow as pa
from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

UPSTREAM_ASSET = ['ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT',
                  'ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT',
                  'ingestion.MORT_MTH_SNAPSHOT',
                  'features.SRC_SYS_CD',
                  'features.HELOC_F',
                  'features.BASEL_PRD_CD',
                  'features.TOTAL_EXPSR_ABOVE_LMT_F',
                  'ingestion.TNG_ACCT_MO',
                  'ingestion.BASEL_ACCT_DIM',
                  'reference.BASEL_RPTG_PRD_LKP',
                  'features.PRD_ID',
                  'reference.PSNL_LOAN_RPTG_PRD_LKP',
                  'reference.MORT_RPTG_PRD_LKP']

DOWNSTREAM_ASSET = 'features.ASST_CL_DESC'
DEPENDENCIES = {
    'export_ks': ['duckdb_clear_asst_cl_desc'],
    'export_spl': ['duckdb_clear_asst_cl_desc'],
    'export_mor': ['duckdb_clear_asst_cl_desc'],
    'export_tng': ['duckdb_clear_asst_cl_desc'],
    'duckdb_clear_asst_cl_desc': ['duckdb_derive_asst_cl_desc']
}

def export_ks(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
    SELECT
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        ks.BASEL_ACCT_ID,
        sys_cd.SRC_SYS_CD,
        CASE WHEN TRIM(rptg.ASST_CL_DESC) IS NOT NULL
        THEN TRIM(rptg.ASST_CL_DESC)
        ELSE NULL
        END AS ASST_CL_DESC
    FROM {UPSTREAM_ASSET[0]} ks
    LEFT JOIN (SELECT * FROM {UPSTREAM_ASSET[4]} WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}') heloc ON
        ks.BASEL_ACCT_ID = heloc.BASEL_ACCT_ID
    LEFT JOIN (SELECT * FROM {UPSTREAM_ASSET[5]} WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}') prd_cd ON
        ks.BASEL_ACCT_ID = prd_cd.BASEL_ACCT_ID
    LEFT JOIN (SELECT * FROM {UPSTREAM_ASSET[6]} WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}') expsr ON
        ks.BASEL_ACCT_ID = expsr.BASEL_ACCT_ID
    LEFT JOIN {UPSTREAM_ASSET[9]} rptg ON
        TRIM(ks.PRD_CD) = TRIM(rptg.PRD_CD)
        AND TRIM(ks.SUB_PRD_CD) = TRIM(rptg.SUB_PRD_CD)
        AND TRIM(heloc.HELOC_F) = TRIM(rptg.HELOC_F)
        AND TRIM(prd_cd.BASEL_PRD_CD) = TRIM(rptg.BASEL_PRD_CD)
        AND TRIM(expsr.TOTAL_EXPSR_ABOVE_LMT_F) = TRIM(rptg.REVISED_EXPSR_OV_125K_F)
    LEFT JOIN {UPSTREAM_ASSET[3]} sys_cd ON
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
        CASE WHEN TRIM(rptg.ASST_CL_DESC) IS NOT NULL
        THEN TRIM(rptg.ASST_CL_DESC)
        ELSE NULL
        END AS ASST_CL_DESC    
    FROM {UPSTREAM_ASSET[1]} spl
    LEFT JOIN {UPSTREAM_ASSET[10]} prd ON
        spl.BASEL_ACCT_ID = prd.BASEL_ACCT_ID
        AND prd.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    LEFT JOIN {UPSTREAM_ASSET[11]} rptg ON
        prd.PRD_ID = rptg.PRD_ID
        AND rptg.SRC_SYS_CD = 'SPL'
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
        CASE WHEN TRIM(rptg.ASST_CL_DESC) IS NOT NULL
        THEN TRIM(rptg.ASST_CL_DESC)
        ELSE NULL
        END AS ASST_CL_DESC
    FROM {UPSTREAM_ASSET[2]} mor
    LEFT JOIN {UPSTREAM_ASSET[12]} rptg ON
        rptg.SRC_SYS_CD = 'MOR'
        AND UPPER(mor.INSUR_GRP) = UPPER(rptg.BASEL_MORT_INSURER_GRP_DESC)
    WHERE mor.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    GROUP BY 
        mor.BASEL_ACCT_ID, 
        TRIM(rptg.ASST_CL_DESC)
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
            FROM {UPSTREAM_ASSET[7]}
            WHERE MONTH_END_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
        )

        SELECT
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            dim.BASEL_ACCT_ID,
            'TNG-MOR' AS SRC_SYS_CD,
            CASE WHEN TRIM(rptg.ASST_CL_DESC) IS NOT NULL
            THEN TRIM(rptg.ASST_CL_DESC)
            ELSE NULL
            END AS ASST_CL_DESC
        FROM {UPSTREAM_ASSET[7]} tng
        INNER JOIN {UPSTREAM_ASSET[8]} dim ON
            dim.SRC_APP_CD = 'TNG-MOR'
            AND dim.SRC_SYS_DEL_F != 'Y'
            AND TRIM(tng.ACCOUNT_ID) = TRIM(dim.SRC_APP_ID)
        INNER JOIN {UPSTREAM_ASSET[12]} rptg ON
            UPPER(tng.INSURER_DESC) = UPPER(rptg.BASEL_MORT_INSURER_GRP_DESC)
        INNER JOIN bulk_ind bulk ON
            UPPER(bulk.SRC_SYS_CD) = UPPER(rptg.SRC_SYS_CD)
            AND UPPER(bulk.INSUR_GROUP) = UPPER(rptg.BASEL_MORT_INSURER_GRP_DESC)
            AND UPPER(bulk.BULK_IND) = UPPER(rptg.BULK_F)
        WHERE tng.MONTH_END_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
        AND rptg.SRC_SYS_CD = 'TNG-MOR'
        GROUP BY
            dim.BASEL_ACCT_ID,
            TRIM(rptg.ASST_CL_DESC)
    """
):
    pass

def duckdb_clear_asst_cl_desc(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
        DELETE FROM { DOWNSTREAM_ASSET } 
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass

def duckdb_derive_asst_cl_desc(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    INSERT INTO { DOWNSTREAM_ASSET } BY NAME 
    FROM (
        SELECT
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            BASEL_ACCT_ID,
            SRC_SYS_CD,
            ASST_CL_DESC
        FROM read_parquet([
            '{{{{ task_instance.xcom_pull(task_ids="derived__asst_cl_desc.export_ks", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__asst_cl_desc.export_spl", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__asst_cl_desc.export_mor", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__asst_cl_desc.export_tng", key="parquet") }}}}'
        ], union_by_name=true)
    )
    """
):
    pass
