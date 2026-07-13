import os
from datetime import timedelta
import pendulum

from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import _pull_asset_event_extras, _push_asset_event_extras


UPSTREAM_ASSET = [ 'features.PRD_ID']
DOWNSTREAM_ASSET = "features.SUBV"
DEPENDENCIES = {
    'duckdb_clear_derive_subv': ['duckdb_derive_subv'],
}


def duckdb_clear_derive_subv(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    DELETE FROM { DOWNSTREAM_ASSET } 
    WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass

def duckdb_derive_subv(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} 
        SELECT         
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate")}}}}' AS OBSN_DT,
        BASEL_ACCT_ID,
        CASE 
            WHEN PRD_ID = 'S10' THEN 1 ELSE 0 
        END AS SUBV
        FROM 
            {UPSTREAM_ASSET[0]}
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate")}}}}'
    """

):
    pass

