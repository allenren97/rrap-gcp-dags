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
                  "features.BASEL_PRD_TP_CD",
                  "ingestion.BASEL_STEP_PLN_MTH_SNAPSHOT"]

DOWNSTREAM_ASSET = "features.INSUR_F_IFRS9"

DEPENDENCIES = {
    "export_ks": ["duckdb_delete_insur_f_ifrs9"],
    "export_spl": ["duckdb_delete_insur_f_ifrs9"],
    "export_mor": ["duckdb_delete_insur_f_ifrs9"],
    "export_tng": ["duckdb_delete_insur_f_ifrs9"],
    "duckdb_delete_insur_f_ifrs9": ["duckdb_load_insur_f_ifrs9"],
}

def export_ks(
    duckdb_conn_id = 'duckdb-conn',
    sql=f"""
    SELECT
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        ks.BASEL_ACCT_ID,
        'KS' AS SRC_SYS_CD,
        CASE
            WHEN TRIM(tp_cd.BASEL_PRD_TP_CD) = 'HELOC' AND INSURER_CD IS NOT NULL
            THEN 'YES' ELSE NULL 
        END AS INSUR_F_IFRS9
    FROM {UPSTREAM_ASSET[0]} ks
    LEFT JOIN {UPSTREAM_ASSET[5]} tp_cd ON
        ks.BASEL_ACCT_ID = tp_cd.BASEL_ACCT_ID
        AND tp_cd.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    LEFT JOIN {UPSTREAM_ASSET[6]} step ON
        ks.PRIM_BASEL_CUST_ID = step.PRIM_BASEL_CUST_ID
        AND ks.MTH_TM_ID = step.MTH_TM_ID
        AND ks.STEP_PLN_SNAPSHOT_ID = step.STEP_PLN_SNAPSHOT_ID
        AND ks.PRIM_CUST_CID = step.PRIM_CUST_CID
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
        NULL AS INSUR_F_IFRS9
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
        NULL AS INSUR_F_IFRS9
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
        NULL AS INSUR_F_IFRS9
    FROM {UPSTREAM_ASSET[3]} tng
    INNER JOIN {UPSTREAM_ASSET[4]} dim ON
        dim.SRC_APP_CD = 'TNG-MOR'
        AND dim.SRC_SYS_DEL_F != 'Y'
        AND trim(tng.ACCOUNT_ID) = TRIM(dim.SRC_APP_ID)
    WHERE tng.MONTH_END_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass

  
def duckdb_delete_insur_f_ifrs9(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        DELETE FROM {DOWNSTREAM_ASSET}
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """,
):
    pass

def duckdb_load_insur_f_ifrs9(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        INSERT INTO {DOWNSTREAM_ASSET} BY NAME
        FROM (
            SELECT
                OBSN_DT,
                BASEL_ACCT_ID,
                SRC_SYS_CD,
                INSUR_F_IFRS9
            FROM read_parquet([
            '{{{{ task_instance.xcom_pull(task_ids="derived__insur_f_ifrs9.export_ks", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__insur_f_ifrs9.export_spl", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__insur_f_ifrs9.export_mor", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__insur_f_ifrs9.export_tng", key="parquet") }}}}'
        ], union_by_name=true)
        )
    """,
):
    pass
