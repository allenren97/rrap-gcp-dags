import os
from datetime import timedelta
import pendulum

from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import _pull_asset_event_extras, _push_asset_event_extras


UPSTREAM_ASSET = [ 'ingestion.AIRB_MORT_MTH_SNAPSHOT']
DOWNSTREAM_ASSET = "features.FORECLOSE_IND"
DEPENDENCIES = {
    'duckdb_clear_derive_foreclose_ind': ['duckdb_derive_foreclose_ind'],
}


def duckdb_clear_derive_foreclose_ind(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    DELETE FROM { DOWNSTREAM_ASSET } 
    WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass

def duckdb_derive_foreclose_ind(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} 
        SELECT
        MORT_NUM,
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' as OBSN_DT,
        CASE 
        WHEN frclsr_f IS NULL 
        THEN '' 
        ELSE FRCLSR_F  
        END AS FORECLOSE_IND
        from {UPSTREAM_ASSET[0]}
        where TM_ID = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}'
    """

):
    pass

