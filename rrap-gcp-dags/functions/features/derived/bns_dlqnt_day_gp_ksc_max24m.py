import os
from datetime import timedelta
import pendulum

from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import _pull_asset_event_extras, _push_asset_event_extras


UPSTREAM_ASSET = [ 'features.BNS_DLQNT_DAY_GP_KSC']
DOWNSTREAM_ASSET = "features.BNS_DLQNT_DAY_GP_KSC_MAX24M"
DEPENDENCIES = {
    'duckdb_clear_derive_bns_dlqnt_day_gp_ksc_max24m': ['duckdb_derive_bns_dlqnt_day_gp_ksc_max24m'],
}


def duckdb_clear_derive_bns_dlqnt_day_gp_ksc_max24m(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    DELETE FROM { DOWNSTREAM_ASSET } 
    WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass

def duckdb_derive_bns_dlqnt_day_gp_ksc_max24m(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} BY NAME 
    with base as (
        SELECT 
        BASEL_CUST_ID,
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        MAX(bns_dlqnt_day_gp_ksc) AS BNS_DLQNT_DAY_GP_KSC_MAX24M
        FROM 
            {UPSTREAM_ASSET[0]}
        WHERE 
            OBSN_DT BETWEEN 
                DATE_ADD(DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}', - INTERVAL 23 MONTH)
                AND
                '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
        GROUP BY 
            BASEL_CUST_ID
    )
    SELECT 
    BASEL_CUST_ID,
    '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate")  }}}}' AS OBSN_DT,
    BNS_DLQNT_DAY_GP_KSC_MAX24M
    from base

    """

):
    pass

