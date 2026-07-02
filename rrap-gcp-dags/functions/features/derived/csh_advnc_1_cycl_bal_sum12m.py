import os
from datetime import timedelta
import pendulum

from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import _pull_asset_event_extras, _push_asset_event_extras


UPSTREAM_ASSET = ['features.CSH_ADVNC_1_CYCL_AGO_BAL_AMT']
DOWNSTREAM_ASSET = "features.CSH_ADVNC_1_CYCL_BAL_SUM12M"
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
        INSERT INTO {DOWNSTREAM_ASSET} BY NAME 
        FROM (
            WITH lst_12m_aggr AS (
                SELECT
                    BASEL_ACCT_ID,
                    SUM(CSH_ADVNC_1_CYCL_AGO_BAL_AMT) AS CSH_ADVNC_1_CYCL_BAL_SUM12M
                FROM features.CSH_ADVNC_1_CYCL_AGO_BAL_AMT f
                WHERE OBSN_DT BETWEEN DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' - INTERVAL 11 MONTH
                    AND '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
                GROUP BY BASEL_ACCT_ID
            )
            SELECT 
                '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
                f.BASEL_ACCT_ID,
                CSH_ADVNC_1_CYCL_BAL_SUM12M
            FROM features.CSH_ADVNC_1_CYCL_AGO_BAL_AMT f
            LEFT JOIN lst_12m_aggr USING (BASEL_ACCT_ID)
            WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
        )
    """

):
    pass

