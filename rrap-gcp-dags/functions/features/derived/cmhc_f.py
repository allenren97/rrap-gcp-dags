from airflow.sdk import get_current_context
from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

UPSTREAM_ASSET = ["reference.MORT_RPTG_PRD_LKP",
                  "ingestion.MORT_MTH_SNAPSHOT",
                  "ingestion.BASEL_ACCT_DIM",
                  "reference.BASEL_EGL_LKP_NZ",
                  "ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT",
                  "ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT"]

DOWNSTREAM_ASSET = "features.CMHC_F"
DEPENDENCIES = {
    "export_ks": ["duckdb_delete_cmhc_f"],
    "export_spl": ["duckdb_delete_cmhc_f"],
    "export_mor": ["duckdb_delete_cmhc_f"],
    "export_tng": ["duckdb_delete_cmhc_f"],
    "duckdb_delete_cmhc_f": ["duckdb_load_cmhc_f"],
}

def export_ks(
    duckdb_conn_id = 'duckdb-conn',
    sql=f"""
    SELECT
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        ks.BASEL_ACCT_ID,
        'KS' AS SRC_SYS_CD,
        NULL AS CMHC_F
    FROM {UPSTREAM_ASSET[4]} ks
    WHERE ks.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    """
):
    pass

def export_spl(
    duckdb_conn_id = 'duckdb-conn',
    sql=f"""
    SELECT
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        spl.BASEL_ACCT_ID,
        'SPL' AS SRC_SYS_CD,
        NULL AS CMHC_F
    FROM {UPSTREAM_ASSET[5]} spl
    WHERE spl.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    """
):
    pass

def export_mor(
    duckdb_conn_id = "duckdb-conn",
    sql=f"""
    SELECT DISTINCT
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        mor.BASEL_ACCT_ID,
        'MOR' AS SRC_SYS_CD,
        CASE 
            WHEN TRIM(rptg.SRC_SYS_CD) IN ('TNG-MOR', 'MOR')
            AND TRIM(rptg.BASEL_PRD_TP_CD) LIKE '%CMHC%'
            THEN 'Y' ELSE NULL
        END AS CMHC_F
    FROM {UPSTREAM_ASSET[2]} dim
    LEFT JOIN {UPSTREAM_ASSET[1]} mor ON
        TRIM(dim.SRC_APP_CD) = 'MO'
        AND dim.SRC_SYS_DEL_F != 'Y'
        AND mor.BASEL_ACCT_ID = dim.BASEL_ACCT_ID
    LEFT JOIN {UPSTREAM_ASSET[0]} rptg ON
        TRIM(mor.CRNCY_CD) = TRIM(rptg.CRNCY_OF_ACCT)
        AND rptg.SRC_SYS_CD = 'MOR'
        AND mor.INSUR_GRP = rptg.BASEL_MORT_INSURER_GRP_DESC
    WHERE mor.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    """
):
    pass

def export_tng(
    duckdb_conn_id = "duckdb-conn",
    sql=f"""
    SELECT DISTINCT
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        dim.BASEL_ACCT_ID,
        'TNG' AS SRC_SYS_CD,
        CASE 
            WHEN TRIM(rptg.SRC_SYS_CD) IN ('TNG-MOR', 'MOR')
            AND TRIM(rptg.BASEL_PRD_TP_CD) LIKE '%CMHC%'
            THEN 'Y' ELSE NULL
        END AS CMHC_F
    FROM ingestion.TNG_ACCT_MO tng
    INNER JOIN {UPSTREAM_ASSET[2]} dim ON
        dim.SRC_APP_CD = 'TNG-MOR'
        AND dim.SRC_SYS_DEL_F != 'Y'
        AND trim(tng.ACCOUNT_ID) = TRIM(dim.SRC_APP_ID)
    INNER JOIN {UPSTREAM_ASSET[0]} rptg ON
        UPPER(tng.INSURER_DESC) = UPPER(rptg.BASEL_MORT_INSURER_GRP_DESC)
    LEFT JOIN {UPSTREAM_ASSET[3]} nz ON
        UPPER(rptg.PRD_ID) = UPPER(nz.PRD_CD)
    WHERE tng.MONTH_END_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    AND rptg.SRC_SYS_CD = 'TNG-MOR'
    """
):
    pass

def duckdb_delete_cmhc_f(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        DELETE FROM {DOWNSTREAM_ASSET}
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """,
):
    pass

def duckdb_load_cmhc_f(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        INSERT INTO {DOWNSTREAM_ASSET} BY NAME
        FROM (
            SELECT
                OBSN_DT,
                BASEL_ACCT_ID,
                SRC_SYS_CD,
                CMHC_F
            FROM read_parquet([
            '{{{{ task_instance.xcom_pull(task_ids="derived__cmhc_f.export_ks", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__cmhc_f.export_spl", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__cmhc_f.export_mor", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__cmhc_f.export_tng", key="parquet") }}}}'
        ], union_by_name=true)
        )
    """,
):
    pass

