from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

UPSTREAM_ASSET = [
    "reference.BASEL_RPTG_PRD_LKP",
    "reference.PSNL_LOAN_RPTG_PRD_LKP",
    "reference.MORT_RPTG_PRD_LKP",
    "ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT",
    "ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT",
    "ingestion.MORT_MTH_SNAPSHOT",
    "features.PRD_ID",
    'ingestion.TNG_ACCT_MO',
    'ingestion.BASEL_ACCT_DIM',
    'reference.BASEL_EGL_LKP_NZ',
    'features.HELOC_F',
    'features.BASEL_PRD_CD',
    'features.TOTAL_EXPSR_ABOVE_LMT_F',
    'ingestion.BASEL_ACCT_PRFM_FACT',
    'features.BULK_IND'
]
DOWNSTREAM_ASSET = "features.SCRTY_TP_DESC"
DEPENDENCIES = {
    "export_ks": ["duckdb_delete"],
    "export_spl": ["duckdb_delete"],
    "export_mor": ["duckdb_delete"],
    "export_tng": ["duckdb_delete"],
    "duckdb_delete": ["duckdb_load"],
}


def export_ks(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    WITH rptg_prd_lkp_1 AS (
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
            LTRIM(RTRIM(nz.EGL_DEPRTMNT)) AS EGL_DEPRTMNT
        FROM {UPSTREAM_ASSET[0]} rptg
        LEFT JOIN {UPSTREAM_ASSET[9]} nz ON
            LTRIM(RTRIM(rptg.PRD_ID)) = LTRIM(RTRIM(nz.PRD_CD))
        WHERE TRIM(rptg.SRC_SYS_CD) = 'KS'
    )

    SELECT
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        ks.BASEL_ACCT_ID,
        'KS' AS SRC_SYS_CD,
        NULLIF(TRIM(rptg.LKP_SCRTY_TP_CD), '') AS SCRTY_TP_DESC
    FROM {UPSTREAM_ASSET[3]} ks
    LEFT JOIN (SELECT * FROM {UPSTREAM_ASSET[10]} WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}') heloc ON
        ks.BASEL_ACCT_ID = heloc.BASEL_ACCT_ID
    LEFT JOIN (SELECT * FROM {UPSTREAM_ASSET[11]} WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}') prd_cd ON
        ks.BASEL_ACCT_ID = prd_cd.BASEL_ACCT_ID
    LEFT JOIN (SELECT * FROM {UPSTREAM_ASSET[12]} WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}') expsr ON
        ks.BASEL_ACCT_ID = expsr.BASEL_ACCT_ID
    LEFT JOIN rptg_prd_lkp_1 rptg ON
        TRIM(ks.PRD_CD) = TRIM(rptg.PRD_CD)
        AND TRIM(ks.SUB_PRD_CD) = TRIM(rptg.SUB_PRD_CD0)
        AND TRIM(expsr.TOTAL_EXPSR_ABOVE_LMT_F) = TRIM(rptg.REVISED_EXPSR_OV_125K_F)
        AND TRIM(heloc.HELOC_F) = TRIM(rptg.HELOC_F)
        AND TRIM(prd_cd.BASEL_PRD_CD) = TRIM(rptg.BASEL_PRD_CD)
    LEFT JOIN {UPSTREAM_ASSET[13]} fact ON
        ks.BASEL_ACCT_ID = fact.BASEL_ACCT_ID
        AND ks.MTH_TM_ID = fact.MTH_TM_ID
    WHERE 
        ks.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    GROUP BY
        ks.BASEL_ACCT_ID,
        TRIM(rptg.LKP_SCRTY_TP_CD)
    """,
):
    pass

def export_spl(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    SELECT
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        spl.BASEL_ACCT_ID,
        'SPL' AS SRC_SYS_CD,
        TRIM(rptg.SCRTY_TP) AS SCRTY_TP_DESC
    FROM {UPSTREAM_ASSET[4]} spl
    LEFT JOIN {UPSTREAM_ASSET[6]} prd ON
        spl.BASEL_ACCT_ID = prd.BASEL_ACCT_ID
        AND prd.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    LEFT JOIN {UPSTREAM_ASSET[1]} rptg ON
        rptg.PRD_ID = prd.PRD_ID
        AND rptg.SRC_SYS_CD = 'SPL'
    WHERE 
        spl.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    GROUP BY
        spl.BASEL_ACCT_ID,
        TRIM(rptg.SCRTY_TP)
    
    """,
):
    pass

def export_mor(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    SELECT
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        mor.BASEL_ACCT_ID,
        'MOR' AS SRC_SYS_CD,
        TRIM(UPPER(LEFT(rptg.SCRTY_TP, 1)) || LOWER(SUBSTR(rptg.SCRTY_TP, 2))) AS SCRTY_TP_DESC
    FROM {UPSTREAM_ASSET[5]} mor
    LEFT JOIN {UPSTREAM_ASSET[14]} bulk ON
        mor.MORT_NUM = bulk.MORT_NUM
        AND bulk.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    LEFT JOIN {UPSTREAM_ASSET[2]} rptg ON
        rptg.SRC_SYS_CD = 'MOR'
        AND UPPER(mor.INSUR_GRP) = TRIM(UPPER(rptg.BASEL_MORT_INSURER_GRP_DESC))
        AND TRIM(bulk.BULK_IND) = TRIM(rptg.BULK_F)
    WHERE
        mor.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    GROUP BY
        mor.BASEL_ACCT_ID,
        rptg.SCRTY_TP
    """,
):
    pass

def export_tng(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
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
        TRIM(UPPER(LEFT(rptg.SCRTY_TP, 1)) || LOWER(SUBSTR(rptg.SCRTY_TP, 2))) AS SCRTY_TP_DESC
    FROM {UPSTREAM_ASSET[7]} tng
    INNER JOIN {UPSTREAM_ASSET[8]} dim ON
        dim.SRC_APP_CD = 'TNG-MOR'
        AND dim.SRC_SYS_DEL_F != 'Y'
        AND TRIM(tng.ACCOUNT_ID) = TRIM(dim.SRC_APP_ID)
    INNER JOIN {UPSTREAM_ASSET[2]} rptg ON
        UPPER(tng.INSURER_DESC) = UPPER(rptg.BASEL_MORT_INSURER_GRP_DESC)
    INNER JOIN bulk_ind bulk ON
        UPPER(bulk.SRC_SYS_CD) = UPPER(rptg.SRC_SYS_CD)
        AND UPPER(bulk.INSUR_GROUP) = UPPER(rptg.BASEL_MORT_INSURER_GRP_DESC)
        AND UPPER(bulk.BULK_IND) = UPPER(rptg.BULK_F)
    LEFT JOIN {UPSTREAM_ASSET[9]} nz ON
        UPPER(rptg.PRD_ID) = UPPER(nz.PRD_CD)
    WHERE 
        tng.MONTH_END_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
        AND rptg.SRC_SYS_CD = 'TNG-MOR'
    GROUP BY
        dim.BASEL_ACCT_ID,
        rptg.SCRTY_TP
    """,
):
    pass

def duckdb_delete(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    delete from {DOWNSTREAM_ASSET} where obsn_dt = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """,
):
    pass

def duckdb_load(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} by name
    FROM (
        SELECT
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            BASEL_ACCT_ID,
            SRC_SYS_CD,
            TRIM(SCRTY_TP_DESC) AS SCRTY_TP_DESC
        FROM read_parquet([
            '{{{{ task_instance.xcom_pull(task_ids="derived__scrty_tp_desc.export_ks", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__scrty_tp_desc.export_spl", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__scrty_tp_desc.export_mor", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__scrty_tp_desc.export_tng", key="parquet") }}}}'
        ], union_by_name=true)
    )
    """,
):
    pass