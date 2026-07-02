from airflow.sdk import get_current_context
from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

UPSTREAM_ASSET = ["ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT",
                  "ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT",
                  "ingestion.MORT_MTH_SNAPSHOT",
                  "ingestion.TNG_ACCT_MO",
                  "ingestion.BASEL_ACCT_DIM",
                  "ingestion.TNG_CUST_MO"]

DOWNSTREAM_ASSET = "features.PRIM_CUST_CID"
DEPENDENCIES = {
    "export_ks": ["duckdb_delete_prim_cust_cid"],
    "export_spl": ["duckdb_delete_prim_cust_cid"],
    "export_mor": ["duckdb_delete_prim_cust_cid"],
    "export_tng": ["duckdb_delete_prim_cust_cid"],
    "duckdb_delete_prim_cust_cid": ["duckdb_load_prim_cust_cid"],
}

def export_ks(
    duckdb_conn_id = 'duckdb-conn',
    sql=f"""
    SELECT
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        ks.BASEL_ACCT_ID,
        'KS' AS SRC_SYS_CD,
        PRIM_CUST_CID
    FROM {UPSTREAM_ASSET[0]} ks
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
        CUST_CID AS PRIM_CUST_CID
    FROM {UPSTREAM_ASSET[1]} spl
    WHERE spl.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    """
):
    pass

def export_mor(
    duckdb_conn_id = "duckdb-conn",
    sql=f"""
    SELECT
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        mor.BASEL_ACCT_ID,
        'MOR' AS SRC_SYS_CD,
        CASE WHEN LENGTH(PRIM_CUST_CID) < 10 THEN PRIM_CUST_CID
        ELSE SUBSTRING(PRIM_CUST_CID, 1, 10) 
        END AS PRIM_CUST_CID
    FROM {UPSTREAM_ASSET[2]} mor
    WHERE mor.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    """
):
    pass

def export_tng(
    duckdb_conn_id = "duckdb-conn",
    sql=f"""
    SELECT
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        dim.BASEL_ACCT_ID,
        'TNG-MOR' AS SRC_SYS_CD,
        tng_cust.CUSTOMER_ID AS PRIM_CUST_CID
    FROM {UPSTREAM_ASSET[5]} tng_cust
    INNER JOIN {UPSTREAM_ASSET[3]} tng_acct ON
        tng_cust.CUSTOMER_KEY = tng_acct.CUSTOMER_KEY
        AND tng_cust.MONTH_END_DT = tng_acct.MONTH_END_DT
    INNER JOIN {UPSTREAM_ASSET[4]} dim ON
        dim.SRC_APP_CD = 'TNG-MOR'
        AND dim.SRC_SYS_DEL_F != 'Y'
        AND trim(tng_acct.ACCOUNT_ID) = TRIM(dim.SRC_APP_ID)
    WHERE tng_acct.MONTH_END_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass

def duckdb_delete_prim_cust_cid(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        DELETE FROM {DOWNSTREAM_ASSET}
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """,
):
    pass

def duckdb_load_prim_cust_cid(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        INSERT INTO {DOWNSTREAM_ASSET} BY NAME
        FROM (
            SELECT
                OBSN_DT,
                BASEL_ACCT_ID,
                SRC_SYS_CD,
                PRIM_CUST_CID
            FROM read_parquet([
            '{{{{ task_instance.xcom_pull(task_ids="derived__prim_cust_cid.export_ks", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__prim_cust_cid.export_spl", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__prim_cust_cid.export_mor", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__prim_cust_cid.export_tng", key="parquet") }}}}'
        ], union_by_name=true)
        )
    """,
):
    pass
