import os
from datetime import timedelta
import pendulum

from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import _pull_asset_event_extras, _push_asset_event_extras


UPSTREAM_ASSET = [ "ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT"]
DOWNSTREAM_ASSET = "features.BNS_DLQNT_DAY_GP_KSC_AVG12M"
DEPENDENCIES = {
    'duckdb_clear_derive_bns_dlqnt_day_gp_ksc_avg12m': ['duckdb_derive_bns_dlqnt_day_gp_ksc_avg12m'],
}


def duckdb_clear_derive_bns_dlqnt_day_gp_ksc_avg12m(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    DELETE FROM { DOWNSTREAM_ASSET } 
    WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass

def duckdb_derive_bns_dlqnt_day_gp_ksc_avg12m(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} BY NAME 
    WITH accounts AS (
        SELECT 
            BASEL_ACCT_ID,
        FROM {UPSTREAM_ASSET[0]}
            WHERE MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}} 
            AND PRIM_BASEL_CUST_ID > 0
        GROUP BY 
            BASEL_ACCT_ID
    ),

    mthsum as(
        SELECT
            PRIM_BASEL_CUST_ID as BASEL_CUST_ID,
            CAST(MAX(CASE
                WHEN BNS_DLQNT_DAY < 31 THEN 0
                ELSE BNS_DLQNT_DAY - 30 
            END) AS DECIMAL(18,3)) AS BNS_DLQNT_DAY_GP_KSC
        FROM {UPSTREAM_ASSET[0]} AS main
        INNER JOIN accounts on accounts.BASEL_ACCT_ID = main.BASEL_ACCT_ID
        WHERE
            MTH_TM_ID BETWEEN {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}} - 40*11 AND 
            {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
            AND PRIM_BASEL_CUST_ID > 0
        GROUP BY PRIM_BASEL_CUST_ID, MTH_TM_ID
    )

    SELECT 
        BASEL_CUST_ID,
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate")  }}}}' AS OBSN_DT,
        AVG(BNS_DLQNT_DAY_GP_KSC) AS BNS_DLQNT_DAY_GP_KSC_AVG12M
    FROM mthsum
    GROUP BY BASEL_CUST_ID

    """

):
    pass

