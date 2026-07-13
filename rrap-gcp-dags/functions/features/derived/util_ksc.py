import pyarrow as pa
from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

UPSTREAM_ASSET = [
    "ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT",
]
DOWNSTREAM_ASSET = "features.UTIL_KSC"
DEPENDENCIES = {
    "duckdb_clear_util_ksc": ["duckdb_derive_util_ksc"],
}


def duckdb_clear_util_ksc(
    duckdb_conn_id="duckdb-conn",
    sql=rf"""
        DELETE FROM {DOWNSTREAM_ASSET} 
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """,
):
    pass


def duckdb_derive_util_ksc(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET}
    BY NAME FROM (
        SELECT DISTINCT
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            PRIM_BASEL_CUST_ID AS BASEL_CUST_ID, 
            (CASE WHEN (CR_LMT = 0 AND TOT_BAL > 0) THEN 1
                WHEN TOT_BAL < 0 THEN 0
                WHEN CR_LMT <> 0 THEN TOT_BAL/CR_LMT
                ELSE NULL 
            END) AS UTIL_KSC
        FROM 
        (SELECT DISTINCT 
            PRIM_BASEL_CUST_ID, 
            SUM(CR_LMT_AMT) AS CR_LMT, 
            SUM(TOT_NEW_BAL_AMT) AS TOT_BAL
        FROM {UPSTREAM_ASSET[0]}
        WHERE MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
        GROUP BY PRIM_BASEL_CUST_ID) 
    )
    """,
):
    pass
