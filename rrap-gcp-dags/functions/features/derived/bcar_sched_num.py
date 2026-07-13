from airflow.sdk import get_current_context
from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

UPSTREAM_ASSET = ["ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT",        #[0]
                  "ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT",         #[1]
                  "ingestion.MORT_MTH_SNAPSHOT",                    #[2]
                  "ingestion.TNG_ACCT_MO",                          #[3]
                  "ingestion.BASEL_ACCT_DIM",                       #[4]
                  "features.HELOC_F",                               #[5]
                  "features.BASEL_PRD_CD",                          #[6]
                  "features.REVISED_EXPSR_OV_125K_F",               #[7]
                  "features.PRD_ID",                                #[8]
                  "features.BULK_IND",                              #[9]
                  "reference.BASEL_RPTG_PRD_LKP",                   #[10]
                  "reference.PSNL_LOAN_RPTG_PRD_LKP",               #[11]
                  "reference.BASEL_EGL_LKP_NZ",                     #[12]
                  "reference.MORT_RPTG_PRD_LKP"]                    #[13]

DOWNSTREAM_ASSET = "features.BCAR_SCHED_NUM"
DEPENDENCIES = {
    "export_ks": ["duckdb_delete"],
    "export_spl": ["duckdb_delete"],
    "export_mor": ["duckdb_delete"],
    "export_tng": ["duckdb_delete"],
    "duckdb_delete": ["duckdb_load"],
}

def export_ks(
    duckdb_conn_id = 'duckdb-conn',
    sql=f"""
    SELECT
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        'KS' AS SRC_SYS_CD,
        ks.BASEL_ACCT_ID,
        BCAR_SCHED_NUM
    FROM {UPSTREAM_ASSET[0]} ks
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
        AND TRIM(expsr.REVISED_EXPSR_OV_125K_F) = TRIM(rptg.REVISED_EXPSR_OV_125K_F)
    WHERE 
        ks.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    """
):
    pass

def export_spl(
    duckdb_conn_id = 'duckdb-conn',
    sql=f"""
    SELECT
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        'SPL' AS SRC_SYS_CD,
        spl.BASEL_ACCT_ID,
        BCAR_SCHED_NUM
    FROM {UPSTREAM_ASSET[1]} spl
    LEFT JOIN {UPSTREAM_ASSET[8]} prd ON
        spl.BASEL_ACCT_ID = prd.BASEL_ACCT_ID
        AND prd.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    LEFT JOIN {UPSTREAM_ASSET[11]} rptg ON
        prd.PRD_ID = rptg.PRD_ID
        AND TRIM(rptg.SRC_SYS_CD) = 'SPL'
    WHERE 
        spl.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    GROUP BY
        spl.BASEL_ACCT_ID,
        rptg.BCAR_SCHED_NUM
    """
):
    pass

def export_mor(
    duckdb_conn_id = "duckdb-conn",
    sql=f"""
    SELECT
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        'MOR' AS SRC_SYS_CD,
        mor.BASEL_ACCT_ID,
        BCAR_SCHED_NUM
    FROM {UPSTREAM_ASSET[2]} mor
    LEFT JOIN {UPSTREAM_ASSET[9]} bulk ON
        mor.MORT_NUM = bulk.MORT_NUM
        AND bulk.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    LEFT JOIN {UPSTREAM_ASSET[13]} rptg ON
        rptg.SRC_SYS_CD = 'MOR'
        AND UPPER(mor.INSUR_GRP) = TRIM(UPPER(rptg.BASEL_MORT_INSURER_GRP_DESC))
        AND TRIM(bulk.BULK_IND) = TRIM(rptg.BULK_F)
    LEFT JOIN {UPSTREAM_ASSET[12]} egl ON
        LTRIM(RTRIM(rptg.PRD_ID)) = LTRIM(RTRIM(egl.PRD_CD))
    WHERE
        mor.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    GROUP BY
        mor.BASEL_ACCT_ID,
        rptg.BCAR_SCHED_NUM
    """
):
    pass

def export_tng(
    duckdb_conn_id = "duckdb-conn",
    sql=f"""
    WITH bulk_ind AS(
        SELECT
            'TNG-MOR' AS SRC_SYS_CD,
            INSURER_DESC AS INSUR_GROUP,
            case 
                when UPPER(bulk_nsurer_desc)='BULKINSURED' then 'Y'
                else 'N'
            end as bulk_ind
            FROM {UPSTREAM_ASSET[3]}
            WHERE MONTH_END_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
        )

        SELECT
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            'TNG-MOR' AS SRC_SYS_CD,
            dim.BASEL_ACCT_ID,
            BCAR_SCHED_NUM
        FROM {UPSTREAM_ASSET[3]} tng
        INNER JOIN {UPSTREAM_ASSET[4]} dim ON
            dim.SRC_APP_CD = 'TNG-MOR'
            AND dim.SRC_SYS_DEL_F != 'Y'
            AND TRIM(tng.ACCOUNT_ID) = TRIM(dim.SRC_APP_ID)
        INNER JOIN {UPSTREAM_ASSET[13]} rptg ON
            UPPER(tng.INSURER_DESC) = UPPER(rptg.BASEL_MORT_INSURER_GRP_DESC)
        INNER JOIN bulk_ind bulk ON
            UPPER(bulk.SRC_SYS_CD) = UPPER(rptg.SRC_SYS_CD)
            AND UPPER(bulk.INSUR_GROUP) = UPPER(rptg.BASEL_MORT_INSURER_GRP_DESC)
            AND UPPER(bulk.BULK_IND) = UPPER(rptg.BULK_F)
        LEFT JOIN {UPSTREAM_ASSET[12]} egl ON
            UPPER(rptg.PRD_ID) = UPPER(egl.PRD_CD)
        WHERE 
            tng.MONTH_END_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
            AND rptg.SRC_SYS_CD = 'TNG-MOR'
        GROUP BY
            dim.BASEL_ACCT_ID,
            rptg.BCAR_SCHED_NUM
    """
):
    pass

def duckdb_delete(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        DELETE FROM {DOWNSTREAM_ASSET}
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """,
):
    pass

def duckdb_load(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        INSERT INTO {DOWNSTREAM_ASSET} BY NAME
        FROM (
            SELECT
                OBSN_DT,
                BASEL_ACCT_ID,
                SRC_SYS_CD,
                BCAR_SCHED_NUM
            FROM read_parquet([
            '{{{{ task_instance.xcom_pull(task_ids="derived__bcar_sched_num.export_ks", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__bcar_sched_num.export_spl", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__bcar_sched_num.export_mor", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__bcar_sched_num.export_tng", key="parquet") }}}}'
        ], union_by_name=true)
        )
    """,
):
    pass