import os
from datetime import timedelta
import pendulum

from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import _pull_asset_event_extras, _push_asset_event_extras


UPSTREAM_ASSET = ['ingestion.CR_BUREAU_DELI_MTH_SNAPSHOT']

DOWNSTREAM_ASSET = "features.AT21"
DEPENDENCIES = {
    'duckdb_clear_derive_at21': ['duckdb_derive_at21'],
}


def duckdb_clear_derive_at21(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    DELETE FROM { DOWNSTREAM_ASSET } 
    WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass

def duckdb_derive_at21(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} 
        SELECT 
        BASEL_CUST_ID,
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate")  }}}}' AS OBSN_DT,
        MTH_SINCE_MOST_RECNT_TRADE_OPND_CNT AS AT21
        FROM 
            {UPSTREAM_ASSET[0]}
        WHERE 
            MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id")  }}}} 
       
   
    """

):
    pass
