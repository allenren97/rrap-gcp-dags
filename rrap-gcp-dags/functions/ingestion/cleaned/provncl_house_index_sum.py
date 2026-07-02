import os

from datetime import timedelta, datetime
import pendulum
import unicodedata
import re
import logging

import pyarrow as pa
import pyarrow.parquet as pq


from airflow.sdk import get_current_context
from airflow.sdk.bases.sensor import PokeReturnValue

from bns.rrap.helpers.asset_event import _pull_asset_event_extras, _push_asset_event_extras
from bns.rrap.hooks.duckdb import DuckLakeHook


UPSTREAM_ASSET = [ 'ingestion.TERANET_HOUSE_PRC_INDEX']
DOWNSTREAM_ASSET = "ingestion.PROVNCL_HOUSE_INDEX_SUM"
DEPENDENCIES = {
    'duckdb_delete_ducklake':['duckdb_load_into_ducklake'],
    'duckdb_load_into_ducklake':['xcom_push_job_audit'],
}


SESSIONTIME = pendulum.now().format("YYYY-MM-DD HH:mm:ss")


def duckdb_delete_ducklake(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    DELETE FROM {DOWNSTREAM_ASSET} WHERE 
    MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    """
):
    pass


def duckdb_load_into_ducklake(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    WITH LOAD AS (
        SELECT 
            MTH_TM_ID,
            SUBSTR(TRIM(LABEL_1),1,2) AS PROV_CD, 
            SUM(CAST(SLS_PAIR_CNT AS DOUBLE)) AS SLS_PAIR_ANLYS_CNT, 
            CASE 
                WHEN SUM(CAST(SLS_PAIR_CNT AS DOUBLE)) = 0 THEN 0 
                ELSE 
                CAST(ROUND(SUM(CAST(INDEX AS DOUBLE) * CAST(SLS_PAIR_CNT AS DOUBLE))/SUM(CAST(SLS_PAIR_CNT AS DOUBLE)), 4)AS DECIMAL(18,4))
                END
                AS HOUSE_INDEX_RTO,
            '{SESSIONTIME}'	AS INSRT_PROCESS_TMSTMP,
            '{SESSIONTIME}'	AS UPDT_PROCESS_TMSTMP
        FROM 
            {UPSTREAM_ASSET[0]}
        WHERE 
            MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
        GROUP BY 
            MTH_TM_ID, LABEL_1
    ) INSERT INTO {DOWNSTREAM_ASSET} SELECT * FROM LOAD
    """
):
    pass

def xcom_push_job_audit():
    context = get_current_context()
    hook = DuckLakeHook(duckdb_conn_id='duckdb-conn')
    mth_tm_id = context['task_instance'].xcom_pull(task_ids='handle_month_context',key='mth_tm_id')
    scr_count = hook.duckdb.sql(f"""
    SELECT COUNT(*) FROM {UPSTREAM_ASSET[0]} WHERE MTH_TM_ID = {mth_tm_id}
    """).fetchone()[0]
    tgt_count = hook.duckdb.sql(f"""
    SELECT COUNT(*) FROM {DOWNSTREAM_ASSET} WHERE MTH_TM_ID ={mth_tm_id}
    """).fetchone()[0]

    logging.info(f"Source Count: {scr_count}")
    logging.info(f"Target Count: {tgt_count}")

    context['ti'].xcom_push(key='scr_count', value=scr_count)
    context['ti'].xcom_push(key='tgt_count', value=tgt_count)


