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
                  "features.OS_BAL_AMT_IF",
                  "features.ORIG_PRPTY_VAL_AMT"]

DOWNSTREAM_ASSET = "features.LTV_PERCENTAGE"
DEPENDENCIES = {
    "export_ks": ["duckdb_delete_ltv_percentage"],
    "export_spl": ["duckdb_delete_ltv_percentage"],
    "export_mor": ["duckdb_delete_ltv_percentage"],
    "export_tng": ["duckdb_delete_ltv_percentage"],
    "duckdb_delete_ltv_percentage": ["duckdb_load_ltv_percentage"],
}

def export_ks(
    duckdb_conn_id = 'duckdb-conn',
    sql=f"""
    SELECT
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        ks.BASEL_ACCT_ID,
        'KS' AS SRC_SYS_CD,
        NULL AS LTV_PERCENTAGE
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
        NULL AS LTV_PERCENTAGE
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
        case	
            WHEN (os_bal.OS_BAL_AMT_IF/prpty.ORIG_PRPTY_VAL_AMT) <= 0.8 OR os_bal.OS_BAL_AMT_IF = 0.000 THEN '0.80'
            WHEN prpty.ORIG_PRPTY_VAL_AMT IS NULL THEN '0.80'
            WHEN (os_bal.OS_BAL_AMT_IF/prpty.ORIG_PRPTY_VAL_AMT) > 0.8 then '0.81'
            ELSE NULL
        end as LTV_PERCENTAGE
    FROM {UPSTREAM_ASSET[2]} mor
    LEFT JOIN {UPSTREAM_ASSET[5]} os_bal ON
        mor.BASEL_ACCT_ID = os_bal.BASEL_ACCT_ID
        AND os_bal.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    LEFT JOIN {UPSTREAM_ASSET[6]} prpty ON
        mor.BASEL_ACCT_ID = prpty.BASEL_ACCT_ID
        AND prpty.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
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
        case	
            WHEN (os_bal.OS_BAL_AMT_IF/prpty.ORIG_PRPTY_VAL_AMT) <= 0.8 OR os_bal.OS_BAL_AMT_IF = 0.000 THEN '0.80'
            WHEN prpty.ORIG_PRPTY_VAL_AMT IS NULL THEN '0.80'
            WHEN (os_bal.OS_BAL_AMT_IF/prpty.ORIG_PRPTY_VAL_AMT) > 0.8 then '0.81'
            ELSE NULL
        end as LTV_PERCENTAGE
    FROM {UPSTREAM_ASSET[3]} tng
    INNER JOIN {UPSTREAM_ASSET[4]} dim ON
        dim.SRC_APP_CD = 'TNG-MOR'
        AND dim.SRC_SYS_DEL_F != 'Y'
        AND trim(tng.ACCOUNT_ID) = TRIM(dim.SRC_APP_ID)
    LEFT JOIN {UPSTREAM_ASSET[5]} os_bal ON
        dim.BASEL_ACCT_ID = os_bal.BASEL_ACCT_ID
        AND os_bal.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    LEFT JOIN {UPSTREAM_ASSET[6]} prpty ON
        dim.BASEL_ACCT_ID = prpty.BASEL_ACCT_ID
        AND prpty.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    WHERE tng.MONTH_END_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass

def duckdb_delete_ltv_percentage(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        DELETE FROM {DOWNSTREAM_ASSET}
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """,
):
    pass

def duckdb_load_ltv_percentage(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        INSERT INTO {DOWNSTREAM_ASSET} BY NAME
        FROM (
            SELECT
                OBSN_DT,
                BASEL_ACCT_ID,
                SRC_SYS_CD,
                LTV_PERCENTAGE
            FROM read_parquet([
            '{{{{ task_instance.xcom_pull(task_ids="derived__ltv_percentage.export_ks", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__ltv_percentage.export_spl", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__ltv_percentage.export_mor", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__ltv_percentage.export_tng", key="parquet") }}}}'
        ], union_by_name=true)
        )
    """,
):
    pass