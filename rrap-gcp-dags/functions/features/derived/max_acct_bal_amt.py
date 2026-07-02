import os
from datetime import timedelta
import pendulum

from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import _pull_asset_event_extras, _push_asset_event_extras


UPSTREAM_ASSET = [
    "ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT",
    "features.LTV_TP_CD",
    "ingestion.MORT_MTH_SNAPSHOT",
    "ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT",
    "features.OS_BAL_AMT_V2",
    "ingestion.BASEL_ACCT_DIM",
    "ingestion.TNG_ACCT_MO"
]
DOWNSTREAM_ASSET = "features.MAX_ACCT_BAL_AMT"
DEPENDENCIES = {
    "export_ks": ["duckdb_clear"],
    "export_mor": ["duckdb_clear"],
    "export_spl": ["duckdb_clear"],
    "export_tng": ["duckdb_clear"],
    "duckdb_clear": ["duckdb_load"],
}

def export_ks(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
        SELECT 
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            ks.BASEL_ACCT_ID,
            CASE WHEN ltv.LTV_TP_CD in ('LOC','VISA') THEN GREATEST(ks.TOT_NEW_BAL_AMT, ks.CR_LMT_AMT)
                ELSE NULL
            END as MAX_ACCT_BAL_AMT
        FROM ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT AS ks
        LEFT JOIN features.LTV_TP_CD AS ltv
            ON ks.BASEL_ACCT_ID = ltv.BASEL_ACCT_ID
            AND ltv.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
        WHERE ks.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}

    """
):
    pass

def export_mor(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
        SELECT
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            mor.BASEL_ACCT_ID,
            os.OS_BAL_AMT AS MAX_ACCT_BAL_AMT
        FROM ingestion.MORT_MTH_SNAPSHOT AS mor
        LEFT JOIN features.OS_BAL_AMT AS os
            ON os.BASEL_ACCT_ID = mor.BASEL_ACCT_ID
            AND os.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
        WHERE MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    """ 
):
    pass

def export_spl(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
        SELECT
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            spl.BASEL_ACCT_ID,
            bal.OS_BAL_AMT_V2 AS MAX_ACCT_BAL_AMT
        FROM ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT AS spl
        LEFT JOIN features.OS_BAL_AMT_V2 AS bal
            ON spl.BASEL_ACCT_ID = bal.BASEL_ACCT_ID
            AND bal.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
        WHERE spl.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    """
):
    pass

def export_tng(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
        SELECT
            tng.MONTH_END_DT AS OBSN_DT,
            dim.BASEL_ACCT_ID,
            tng.END_PRINCIPAL_BALANCE AS MAX_ACCT_BAL_AMT
        FROM ingestion.BASEL_ACCT_DIM dim
        LEFT JOIN ingestion.TNG_ACCT_MO tng
            ON dim.SRC_APP_CD = 'TNG-MOR'
            AND dim.SRC_SYS_DEL_F != 'Y'
            AND TRIM(tng.ACCOUNT_ID) = TRIM(dim.SRC_APP_ID)
        WHERE tng.MONTH_END_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass


def duckdb_clear(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    DELETE FROM { DOWNSTREAM_ASSET } 
    WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass

def duckdb_load(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} BY NAME
    FROM (
        SELECT
            OBSN_DT,
            BASEL_ACCT_ID,
            MAX_ACCT_BAL_AMT
        FROM read_parquet([
            '{{{{ task_instance.xcom_pull(task_ids="derived__max_acct_bal_amt.export_ks", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__max_acct_bal_amt.export_tng", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__max_acct_bal_amt.export_mor", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__max_acct_bal_amt.export_spl", key="parquet") }}}}'
        ], union_by_name=true)
    )
    """
):
    pass