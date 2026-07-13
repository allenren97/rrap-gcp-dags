import os
from datetime import timedelta
import pendulum

from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import _pull_asset_event_extras, _push_asset_event_extras


UPSTREAM_ASSET = [
    "ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT",
    "instruments.EAD_BASEL_SEG_NUM",
    "instruments.FINAL_RTO",
    "ingestion.MORT_MTH_SNAPSHOT",
    "ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT",
    "ingestion.BASEL_ACCT_DIM",
    "ingestion.TNG_ACCT_MO"
]
DOWNSTREAM_ASSET = "instruments.EXPSR_AT_DFT_RTO"
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
            rto.FINAL_RTO AS EXPSR_AT_DFT_RTO
        FROM ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT AS ks
        LEFT JOIN instruments.EAD_BASEL_SEG_NUM AS seg
            ON ks.BASEL_ACCT_ID = seg.BASEL_ACCT_ID
            AND seg.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
            AND seg.STREAM = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}'
        LEFT JOIN instruments.FINAL_RTO AS rto
            ON rto.BASEL_MODEL_ID = seg.MODEL
            AND rto.SEG_NUM = seg.EAD_BASEL_SEG_NUM
            AND rto.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
            AND rto.STREAM = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}'
        WHERE ks.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    """
):
    pass

def export_mor(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
        SELECT
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            BASEL_ACCT_ID,
            1 AS EXPSR_AT_DFT_RTO
        FROM
            ingestion.MORT_MTH_SNAPSHOT
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
            1 AS EXPSR_AT_DFT_RTO
        FROM ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT AS spl
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
            1 AS EXPSR_AT_DFT_RTO
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
    AND STREAM = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}'
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
            EXPSR_AT_DFT_RTO,
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}' AS STREAM
        FROM read_parquet([
            '{{{{ task_instance.xcom_pull(task_ids="fact__expsr_at_dft_rto.export_ks", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="fact__expsr_at_dft_rto.export_tng", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="fact__expsr_at_dft_rto.export_mor", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="fact__expsr_at_dft_rto.export_spl", key="parquet") }}}}'
        ], union_by_name=true)
    )
    """
):
    pass