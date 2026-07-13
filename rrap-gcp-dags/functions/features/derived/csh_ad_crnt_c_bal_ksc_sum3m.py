import os
from datetime import timedelta
import pendulum

from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import _pull_asset_event_extras, _push_asset_event_extras


UPSTREAM_ASSET = ['features.CSH_AD_CRNT_C_BAL_KSC',
                  ]
DOWNSTREAM_ASSET = "features.CSH_AD_CRNT_C_BAL_KSC_SUM3M"
DEPENDENCIES = {
    'duckdb_delete': ['duckdb_load'],
}


def duckdb_delete(
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
    INSERT INTO {DOWNSTREAM_ASSET} BY NAME FROM (
    (WITH CurrentMonthAccounts AS (
      SELECT DISTINCT BASEL_CUST_ID, BASEL_ACCT_ID
      FROM {UPSTREAM_ASSET[0]}
      WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
      ),
      Last3MonthsData AS (
      SELECT T.BASEL_CUST_ID,
            T.BASEL_ACCT_ID,
            T.CSH_ADVNC_CRNT_CYCL_BAL_AMT
      FROM {UPSTREAM_ASSET[0]} T
        WHERE OBSN_DT BETWEEN 
            DATE_ADD(DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}', -INTERVAL 2 MONTH) AND 
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
      )
      
      SELECT L.BASEL_CUST_ID,
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            SUM(L.CSH_ADVNC_CRNT_CYCL_BAL_AMT) AS  CSH_AD_CRNT_C_BAL_KSC_SUM3M
      FROM Last3MonthsData L
      JOIN CurrentMonthAccounts C
      ON L.BASEL_CUST_ID = C.BASEL_CUST_ID
      AND L.BASEL_ACCT_ID = C.BASEL_ACCT_ID
      GROUP BY L.BASEL_CUST_ID, OBSN_DT)
    )
    """

):
    pass

