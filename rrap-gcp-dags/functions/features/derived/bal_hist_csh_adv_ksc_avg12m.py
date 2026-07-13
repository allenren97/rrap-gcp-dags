import os
from datetime import timedelta
import pendulum

from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import _pull_asset_event_extras, _push_asset_event_extras


UPSTREAM_ASSET = [ 'ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT']
DOWNSTREAM_ASSET = "features.BAL_HIST_CSH_ADV_KSC_AVG12M"
DEPENDENCIES = {
    'duckdb_clear_bal_hist_csh_adv_ksc_avg12m': ['duckdb_derive_bal_hist_csh_adv_ksc_avg12m'],
}


def duckdb_clear_bal_hist_csh_adv_ksc_avg12m(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    DELETE FROM { DOWNSTREAM_ASSET } 
    WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass

def duckdb_derive_bal_hist_csh_adv_ksc_avg12m(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} BY NAME
    FROM(
        SELECT 
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            t.PRIM_BASEL_CUST_ID AS BASEL_CUST_ID,
            AVG(t.BAL_HIST_CSH_ADV_SUM) AS BAL_HIST_CSH_ADV_KSC_AVG12M
        FROM (
            SELECT
                revl.PRIM_BASEL_CUST_ID,
                SUM(revl.BAL_HIST_CSH_ADV_AMT) AS BAL_HIST_CSH_ADV_SUM
            FROM {UPSTREAM_ASSET[0]} revl
            WHERE revl.MTH_TM_ID BETWEEN {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}-40*11 AND 
            {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
            GROUP BY revl.PRIM_BASEL_CUST_ID, revl.MTH_TM_ID
        ) t
        GROUP BY
            t.PRIM_BASEL_CUST_ID
    )
    """

):
    pass

