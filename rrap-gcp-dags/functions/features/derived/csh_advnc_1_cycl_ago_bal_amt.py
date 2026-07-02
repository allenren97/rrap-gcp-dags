import os
from datetime import timedelta
import pendulum

from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import _pull_asset_event_extras, _push_asset_event_extras


UPSTREAM_ASSET = ['ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT']

DOWNSTREAM_ASSET = "features.CSH_ADVNC_1_CYCL_AGO_BAL_AMT"
DEPENDENCIES = {
    'duckdb_clear_derive_CSH_ADVNC_1_CYCL_AGO_BAL_AMT': ['duckdb_derive_CSH_ADVNC_1_CYCL_AGO_BAL_AMT'],
}


def duckdb_clear_derive_CSH_ADVNC_1_CYCL_AGO_BAL_AMT(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    DELETE FROM { DOWNSTREAM_ASSET } 
    WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass

def duckdb_derive_CSH_ADVNC_1_CYCL_AGO_BAL_AMT(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} BY NAME FROM
    (
        SELECT 
        BASEL_ACCT_ID,
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate")  }}}}' AS OBSN_DT,
        CSH_ADVNC_1_CYCL_AGO_BAL_AMT
        FROM 
            {UPSTREAM_ASSET[0]}
        WHERE 
            MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id")  }}}}
    ) 
       
   
    """

):
    pass

