import os
from datetime import timedelta
import pendulum

from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import _pull_asset_event_extras, _push_asset_event_extras


UPSTREAM_ASSET = [ 'ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT']
DOWNSTREAM_ASSET = "features.BNS_DLQNT_DAY_GP_KSC_CURR_ACCTS_MAX24M"
DEPENDENCIES = {
    'duckdb_clear': ['duckdb_load'],
}


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
    INSERT INTO {DOWNSTREAM_ASSET} BY NAME (
    with subq as (
        SELECT
        BASEL_ACCT_ID,
        PRIM_BASEL_CUST_ID as BASEL_CUST_ID,
        (case when bns_dlqnt_day < 31 then 0 else bns_dlqnt_day - 30 end) as BNS_DLQNT_DAY_GP,
        MAX(MTH_TM_ID) OVER (PARTITION BY BASEL_ACCT_ID) as MAX_TM_ID
        FROM 
            {UPSTREAM_ASSET[0]}
        WHERE MTH_TM_ID BETWEEN 
            {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}} - 40 * 23
            AND {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
        AND PRIM_BASEL_CUST_ID > 0
        ) SELECT
            BASEL_CUST_ID,
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' as OBSN_DT,
            MAX(BNS_DLQNT_DAY_GP) AS BNS_DLQNT_DAY_GP_KSC_CURR_ACCTS_MAX24M
        FROM subq
        WHERE MAX_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}} 
        -- only grab max of customer's current accounts as of obsn_dt
        GROUP BY BASEL_CUST_ID
    )
    """

):
    pass
