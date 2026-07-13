import os
from datetime import timedelta
import pendulum

from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import _pull_asset_event_extras, _push_asset_event_extras


UPSTREAM_ASSET = [ 'ingestion.MORT_MTH_SNAPSHOT',]
DOWNSTREAM_ASSET = "features.STEP_HGST_DLQNT_DAY_MTG_MAX12M_ACCT"
DEPENDENCIES = {
    'duckdb_clear_derive_step_hgst_dlqnt_day_mtg_max12m_acct': ['duckdb_derive_step_hgst_dlqnt_day_mtg_max12m_acct'],
}


def duckdb_clear_derive_step_hgst_dlqnt_day_mtg_max12m_acct(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    DELETE FROM { DOWNSTREAM_ASSET } 
    WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass

def duckdb_derive_step_hgst_dlqnt_day_mtg_max12m_acct(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} BY NAME (
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
                STEP_PLN_AGRMNT_NUM,
                DLQNT_DAY AS HGST_DLQNT_DAY
            FROM {UPSTREAM_ASSET[0]} AS main
            INNER JOIN accounts on accounts.BASEL_ACCT_ID = main.BASEL_ACCT_ID
            WHERE
                MTH_TM_ID BETWEEN {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}} - 40*11 AND 
                {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
                AND PRIM_BASEL_CUST_ID > 0
        )

        SELECT 
            STEP_PLN_AGRMNT_NUM,
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate")  }}}}' AS OBSN_DT,
            MAX(HGST_DLQNT_DAY) AS STEP_HGST_DLQNT_DAY_MTG_MAX12M_ACCT
        FROM mthsum
        GROUP BY STEP_PLN_AGRMNT_NUM
    )
    """

):
    pass

