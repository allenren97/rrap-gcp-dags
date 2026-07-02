from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

UPSTREAM_ASSET = [
    "ingestion.BASEL_ACCT_DIM",
    "ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT",
    "ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT",
    "ingestion.MORT_MTH_SNAPSHOT",
    "ingestion.TNG_ACCT_MO",
    "reference.PSNL_LOAN_RPTG_PRD_LKP",
    "reference.MORT_RPTG_PRD_LKP"
]


DOWNSTREAM_ASSET = "features.SRC_SYS_CD"
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
    SELECT
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        ks.BASEL_ACCT_ID,
        dim.SRC_APP_CD AS SRC_SYS_CD
    FROM {UPSTREAM_ASSET[0]} dim
    LEFT JOIN {UPSTREAM_ASSET[1]} ks ON
        TRIM(dim.SRC_APP_CD) = 'KS'
        AND ks.BASEL_ACCT_ID = dim.BASEL_ACCT_ID
    WHERE 
        ks.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    """,
):
    pass

def export_spl(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    SELECT
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        spl.BASEL_ACCT_ID,
        rptg.SRC_SYS_CD
    FROM {UPSTREAM_ASSET[0]} dim
    LEFT JOIN {UPSTREAM_ASSET[2]} spl ON
        TRIM(dim.SRC_APP_CD) = 'SPL'
        AND spl.BASEL_ACCT_ID = dim.BASEL_ACCT_ID
    INNER JOIN {UPSTREAM_ASSET[5]} rptg ON
        TRIM(spl.CRNCY_CD) = TRIM(rptg.CRNCY_OF_ACCT)
    WHERE 
        spl.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
        AND rptg.SRC_SYS_CD = 'SPL'
    GROUP BY
        spl.BASEL_ACCT_ID,
        rptg.SRC_SYS_CD
    """,
):
    pass

def export_mor(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    SELECT
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        mor.BASEL_ACCT_ID,
        rptg.SRC_SYS_CD
    FROM {UPSTREAM_ASSET[0]} dim
    LEFT JOIN {UPSTREAM_ASSET[3]} mor ON
        TRIM(dim.SRC_APP_CD) = 'MO'
        AND mor.BASEL_ACCT_ID = dim.BASEL_ACCT_ID
    INNER JOIN {UPSTREAM_ASSET[6]} rptg ON
        TRIM(mor.CRNCY_CD) = TRIM(rptg.CRNCY_OF_ACCT)
    WHERE 
        mor.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
        AND rptg.SRC_SYS_CD = 'MOR'
    GROUP BY
        mor.BASEL_ACCT_ID,
        rptg.SRC_SYS_CD
    """,
):
    pass

def export_tng(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    SELECT
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        dim.BASEL_ACCT_ID,
        dim.SRC_APP_CD AS SRC_SYS_CD
    FROM {UPSTREAM_ASSET[0]} dim
    LEFT JOIN {UPSTREAM_ASSET[4]} tng ON
        dim.SRC_APP_CD = 'TNG-MOR'
        AND dim.SRC_SYS_DEL_F != 'Y'
        AND TRIM(tng.ACCOUNT_ID) = TRIM(dim.SRC_APP_ID)
    WHERE 
        tng.MONTH_END_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
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
            TRIM(SRC_SYS_CD) AS SRC_SYS_CD
        FROM read_parquet([
            '{{{{ task_instance.xcom_pull(task_ids="derived__src_sys_cd.export_ks", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__src_sys_cd.export_spl", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__src_sys_cd.export_mor", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__src_sys_cd.export_tng", key="parquet") }}}}'
        ], union_by_name=true)
    )
    """,
):
    pass