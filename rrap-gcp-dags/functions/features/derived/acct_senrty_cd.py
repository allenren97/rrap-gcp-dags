import pyarrow as pa
from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

UPSTREAM_ASSET = [
    "ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT",
    "features.LTV_TP_CD",
    "ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT",
    "ingestion.MORT_MTH_SNAPSHOT",
    "ingestion.BASEL_ACCT_DIM",
    "ingestion.TNG_ACCT_MO"
]

DOWNSTREAM_ASSET = 'features.ACCT_SENRTY_CD'

DEPENDENCIES = {
    'export_ks': ['duckdb_clear'],
    'export_spl': ['duckdb_clear'],
    'export_mor': ['duckdb_clear'],
    'export_tng': ['duckdb_clear'],
    'duckdb_clear': ['duckdb_load']
}

def export_ks(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
    SELECT 
        ks.BASEL_ACCT_ID,
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        CASE
            WHEN ltv.LTV_TP_CD = 'LOC' THEN 2
            WHEN ltv.LTV_TP_CD = 'VISA' THEN 4
            ELSE NULL
        END AS ACCT_SENRTY_CD
    FROM ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT ks
    LEFT JOIN features.LTV_TP_CD AS ltv
        ON ks.BASEL_ACCT_ID = ltv.BASEL_ACCT_ID
        AND ltv.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    WHERE ks.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    """
):
    pass

def export_spl(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
    SELECT
        BASEL_ACCT_ID,
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        3 AS ACCT_SENRTY_CD
    FROM
        ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT
    WHERE MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    """
):
    pass

def export_mor(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
    SELECT
        BASEL_ACCT_ID,
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        1 AS ACCT_SENRTY_CD
    FROM
        ingestion.MORT_MTH_SNAPSHOT
    WHERE MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    """
):
    pass

def export_tng(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
    SELECT
        dim.BASEL_ACCT_ID,
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        1 AS ACCT_SENRTY_CD
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
    sql=rf"""
        DELETE FROM { DOWNSTREAM_ASSET } 
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass

def duckdb_load(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} by name
    FROM (
        SELECT
            OBSN_DT,
            BASEL_ACCT_ID,
            ACCT_SENRTY_CD
        FROM read_parquet([
            '{{{{ task_instance.xcom_pull(task_ids="derived__acct_senrty_cd.export_ks", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__acct_senrty_cd.export_spl", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__acct_senrty_cd.export_mor", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__acct_senrty_cd.export_tng", key="parquet") }}}}'
        ], union_by_name=true)
    )
    """,
):
    pass