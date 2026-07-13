import os
from datetime import timedelta
import pendulum
from math import pi

from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import _pull_asset_event_extras, _push_asset_event_extras


UPSTREAM_ASSET = [ 'features.TOT_UNP_FNC_CHRG_KSC']
DOWNSTREAM_ASSET = "features.TOT_UNP_FNC_CHRG_KSC_CHG12M"
DEPENDENCIES = {
    'duckdb_clear_derive_tot_unp_fnc_chrg_kscchg12m': ['duckdb_derive_tot_unp_fnc_chrg_kscchg12m'],
}
TOT_UNP_FNC_CHRG=1098.3
PI = pi


def duckdb_clear_derive_tot_unp_fnc_chrg_kscchg12m(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    DELETE FROM { DOWNSTREAM_ASSET } 
    WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass

def duckdb_derive_tot_unp_fnc_chrg_kscchg12m(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} 
    SELECT A.BASEL_CUST_ID,
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate")  }}}}' AS OBSN_DT,
        ((ATAN(A.TOT_UNPAID_FNCL_CHRG_AMTT_OBS/{TOT_UNP_FNC_CHRG}))/{PI}+0.5)/ 
        ((ATAN(B.TOT_UNPAID_FNCL_CHRG_AMT_12MAGO/{TOT_UNP_FNC_CHRG}))/{PI}+0.5) - 1 
		AS TOT_UNP_FNC_CHRG_KSC_CHG12M 
	FROM
		(SELECT BASEL_CUST_ID, SUM(TOT_UNP_FNC_CHRG_KSC) AS TOT_UNPAID_FNCL_CHRG_AMTT_OBS
		FROM {UPSTREAM_ASSET[0]}
		WHERE OBSN_DT='{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
		GROUP BY BASEL_CUST_ID) A
		,
		(SELECT BASEL_CUST_ID, SUM(TOT_UNP_FNC_CHRG_KSC) AS TOT_UNPAID_FNCL_CHRG_AMT_12MAGO
		FROM {UPSTREAM_ASSET[0]}
		WHERE OBSN_DT=DATE_ADD(DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' , - INTERVAL 11 MONTH)
		GROUP BY BASEL_CUST_ID) B
	WHERE A.BASEL_CUST_ID=B.BASEL_CUST_ID
    """

):
    pass

