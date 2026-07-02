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
                  "features.BASEL_ACCT_ID_CCAR_MATCHED",
                  "features.AF_ADJ_OS_BAL_AMT",
                  "features.SECRTZTN_OS_ADJ_FACTR"]

DOWNSTREAM_ASSET = "features.CR_LMT_AMT_IF"

DEPENDENCIES = {
    "export_ks": ["duckdb_delete_cr_lmt_amt_if"],
    "export_spl": ["duckdb_delete_cr_lmt_amt_if"],
    "export_mor": ["duckdb_delete_cr_lmt_amt_if"],
    "export_tng": ["duckdb_delete_cr_lmt_amt_if"],
    "duckdb_delete_cr_lmt_amt_if": ["duckdb_load_cr_lmt_amt_if"],
}

def export_ks(
    duckdb_conn_id = 'duckdb-conn',
    sql=f"""
    SELECT
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        ks.BASEL_ACCT_ID,
        'KS' AS SRC_SYS_CD,
        CASE 
            WHEN matched.BASEL_ACCT_ID_CCAR_MATCHED IS NOT NULL and ks.MTH_TM_ID>=16516 
            THEN ROUND(ks.CR_LMT_AMT - af.AF_ADJ_OS_BAL_AMT*sec.SECRTZTN_OS_ADJ_FACTR, 8)
            ELSE ROUND(ks.CR_LMT_AMT, 8)
        END AS CR_LMT_AMT_IF
    FROM {UPSTREAM_ASSET[0]} ks
    LEFT JOIN {UPSTREAM_ASSET[5]} matched ON
        ks.BASEL_ACCT_ID = matched.BASEL_ACCT_ID_CCAR_MATCHED
        AND matched.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    LEFT JOIN {UPSTREAM_ASSET[6]} af ON
        ks.BASEL_ACCT_ID = af.BASEL_ACCT_ID
        AND af.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    LEFT JOIN {UPSTREAM_ASSET[7]} sec ON
        matched.SECRTZTN_TP_CD = sec.SECRTZTN_TP_CD
        AND sec.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
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
        0.0 AS CR_LMT_AMT_IF
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
        0.0 AS CR_LMT_AMT_IF
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
        0.0 AS CR_LMT_AMT_IF
    FROM {UPSTREAM_ASSET[3]} tng
    INNER JOIN {UPSTREAM_ASSET[4]} dim ON
        dim.SRC_APP_CD = 'TNG-MOR'
        AND dim.SRC_SYS_DEL_F != 'Y'
        AND trim(tng.ACCOUNT_ID) = TRIM(dim.SRC_APP_ID)
    WHERE tng.MONTH_END_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass

def duckdb_delete_cr_lmt_amt_if(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        DELETE FROM {DOWNSTREAM_ASSET}
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """,
):
    pass

def duckdb_load_cr_lmt_amt_if(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        INSERT INTO {DOWNSTREAM_ASSET} BY NAME
        FROM (
            SELECT
                OBSN_DT,
                BASEL_ACCT_ID,
                SRC_SYS_CD,
                CR_LMT_AMT_IF
            FROM read_parquet([
            '{{{{ task_instance.xcom_pull(task_ids="derived__cr_lmt_amt_if.export_ks", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__cr_lmt_amt_if.export_spl", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__cr_lmt_amt_if.export_mor", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__cr_lmt_amt_if.export_tng", key="parquet") }}}}'
        ], union_by_name=true)
        )
    """,
):
    pass