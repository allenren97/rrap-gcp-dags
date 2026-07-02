from airflow.sdk import get_current_context
from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

UPSTREAM_ASSET = ["ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT",
                  "ingestion.BASEL_ACCT_PRFM_FACT",
                  "ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT",
                  "ingestion.MORT_MTH_SNAPSHOT",
                  "ingestion.TNG_ACCT_MO",
                  "ingestion.BASEL_ACCT_DIM"]

DOWNSTREAM_ASSET = "features.COLLATERAL_VALUE"
DEPENDENCIES = {
    "export_ks": ["duckdb_delete_collateral_value"],
    "export_spl": ["duckdb_delete_collateral_value"],
    "export_mor": ["duckdb_delete_collateral_value"],
    "export_tng": ["duckdb_delete_collateral_value"],
    "duckdb_delete_collateral_value": ["duckdb_load_collateral_value"],
}

def export_ks(
    duckdb_conn_id = 'duckdb-conn',
    sql=f"""
    SELECT
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        ks.BASEL_ACCT_ID,
        'KS' AS SRC_SYS_CD,
        CASE 
            WHEN ks.SCRTY_VAL_AMT IS NULL THEN NULL
            ELSE COALESCE(ks.SCRTY_VAL_AMT, fact.CRNT_PRPTY_VAL_AMT) 
        END AS COLLATERAL_VALUE
    FROM {UPSTREAM_ASSET[0]} ks
    LEFT JOIN {UPSTREAM_ASSET[1]} fact ON
        ks.BASEL_ACCT_ID = fact.BASEL_ACCT_ID
        AND ks.MTH_TM_ID = fact.MTH_TM_ID
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
        fact.CRNT_PRPTY_VAL_AMT AS COLLATERAL_VALUE
    FROM {UPSTREAM_ASSET[2]} spl
    LEFT JOIN {UPSTREAM_ASSET[1]} fact ON
        spl.BASEL_ACCT_ID = fact.BASEL_ACCT_ID
        AND spl.MTH_TM_ID = fact.MTH_TM_ID
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
        NULL AS COLLATERAL_VALUE
    FROM {UPSTREAM_ASSET[3]} mor
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
        NULL AS COLLATERAL_VALUE
    FROM {UPSTREAM_ASSET[4]} tng
    INNER JOIN {UPSTREAM_ASSET[5]} dim ON
        dim.SRC_APP_CD = 'TNG-MOR'
        AND dim.SRC_SYS_DEL_F != 'Y'
        AND trim(tng.ACCOUNT_ID) = TRIM(dim.SRC_APP_ID)
    WHERE tng.MONTH_END_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass

def duckdb_delete_collateral_value(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        DELETE FROM {DOWNSTREAM_ASSET}
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """,
):
    pass

def duckdb_load_collateral_value(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        INSERT INTO {DOWNSTREAM_ASSET} BY NAME
        FROM (
            SELECT
                OBSN_DT,
                BASEL_ACCT_ID,
                SRC_SYS_CD,
                COLLATERAL_VALUE
            FROM read_parquet([
            '{{{{ task_instance.xcom_pull(task_ids="derived__collateral_value.export_ks", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__collateral_value.export_spl", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__collateral_value.export_mor", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__collateral_value.export_tng", key="parquet") }}}}'
        ], union_by_name=true)
        )
    """,
):
    pass

