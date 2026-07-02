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
                  "ingestion.BASELAYER_MOR",
                  "ingestion.BASEL_CUST_DIM"]

DOWNSTREAM_ASSET = "features.BASEL_CIF_KEY"
DEPENDENCIES = {
    "export_ks": ["duckdb_delete_basel_cif_key"],
    "export_spl": ["duckdb_delete_basel_cif_key"],
    "export_mor": ["duckdb_delete_basel_cif_key"],
    "export_tng": ["duckdb_delete_basel_cif_key"],
    "duckdb_delete_basel_cif_key": ["duckdb_load_basel_cif_key"],
}

def export_ks(
    duckdb_conn_id = 'duckdb-conn',
    sql=f"""
    SELECT
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        ks.BASEL_ACCT_ID,
        'KS' AS SRC_SYS_CD,
        CASE WHEN dim.CIF_KEY IS NOT NULL THEN dim.CIF_KEY
        ELSE NULL
        END AS BASEL_CIF_KEY
    FROM {UPSTREAM_ASSET[0]} ks
    LEFT JOIN {UPSTREAM_ASSET[6]} dim ON
        dim.BASEL_CUST_ID = ks.PRIM_BASEL_CUST_ID
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
        CASE WHEN dim.CIF_KEY IS NOT NULL THEN dim.CIF_KEY
        ELSE NULL
        END AS BASEL_CIF_KEY
    FROM {UPSTREAM_ASSET[1]} spl
    LEFT JOIN {UPSTREAM_ASSET[6]} dim ON
        spl.PRIM_BASEL_CUST_ID = dim.BASEL_CUST_ID
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
        CASE WHEN base.BASEL_CIF_KEY IS NOT NULL THEN base.BASEL_CIF_KEY
        ELSE NULL
        END AS BASEL_CIF_KEY
    FROM {UPSTREAM_ASSET[2]} mor
    LEFT JOIN {UPSTREAM_ASSET[5]} base ON
        mor.MORT_NUM = base.MORT_NUM
        AND base.MTH_END_DT = '2025-07-31'
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
        CASE WHEN tng.CUSTOMER_KEY IS NOT NULL THEN tng.CUSTOMER_KEY
        ELSE NULL
        END AS BASEL_CIF_KEY
    FROM {UPSTREAM_ASSET[3]} tng
    INNER JOIN {UPSTREAM_ASSET[4]} dim ON
        dim.SRC_APP_CD = 'TNG-MOR'
        AND dim.SRC_SYS_DEL_F != 'Y'
        AND trim(tng.ACCOUNT_ID) = TRIM(dim.SRC_APP_ID)
    WHERE tng.MONTH_END_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass

def duckdb_delete_basel_cif_key(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        DELETE FROM {DOWNSTREAM_ASSET}
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """,
):
    pass

def duckdb_load_basel_cif_key(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        INSERT INTO {DOWNSTREAM_ASSET} BY NAME
        FROM (
            SELECT
                OBSN_DT,
                BASEL_ACCT_ID,
                SRC_SYS_CD,
                BASEL_CIF_KEY
            FROM read_parquet([
            '{{{{ task_instance.xcom_pull(task_ids="derived__basel_cif_key.export_ks", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__basel_cif_key.export_spl", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__basel_cif_key.export_mor", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__basel_cif_key.export_tng", key="parquet") }}}}'
        ], union_by_name=true)
        )
    """,
):
    pass