import pyarrow as pa
from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

UPSTREAM_ASSET = [ 'ingestion.MORT_MTH_SNAPSHOT' ]
DOWNSTREAM_ASSET = 'features.STEP_TOT_PREPAY_YTD_MTG_MAX12M'
DEPENDENCIES = {
    'duckdb_clear_step_tot_prepay_ytd_mtg_max12m': ['duckdb_derive_step_tot_prepay_ytd_mtg_max12m'],
}


def duckdb_clear_step_tot_prepay_ytd_mtg_max12m(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
        DELETE FROM { DOWNSTREAM_ASSET } 
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass


def duckdb_derive_step_tot_prepay_ytd_mtg_max12m(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
        INSERT INTO { DOWNSTREAM_ASSET }
        BY NAME FROM (
WITH CURR_ACCOUNTS AS (
	SELECT
		BASEL_ACCT_ID,
		STEP_PLN_AGRMNT_NUM
	FROM
		{UPSTREAM_ASSET[0]} 
	WHERE
		MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
		AND
		(STEP_PLN_AGRMNT_NUM NOT NULL AND TRIM(STEP_PLN_AGRMNT_NUM) != '')
		AND
		TRIM(PD_OFF_F) = 'N'
		AND 
		TRIM(UPPER(COMM_TP)) = 'RESIDENTIAL'
		AND 
		CRNT_BAL_AMT>0
	),

BASE AS (
	SELECT
		A.MTH_TM_ID,
		A.STEP_PLN_AGRMNT_NUM,
		SUM(COALESCE(A.YTD_PRPY_AMT, 0)) AS YTD_PRPY_AMT
	FROM 
		{UPSTREAM_ASSET[0]}  A
	INNER JOIN
		CURR_ACCOUNTS B
	ON 
		A.BASEL_ACCT_ID = B.BASEL_ACCT_ID
	AND
		A.STEP_PLN_AGRMNT_NUM = B.STEP_PLN_AGRMNT_NUM
	WHERE 
		A.MTH_TM_ID BETWEEN {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}-440 
		AND 
		{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}} 
		AND 
		TRIM(A.PD_OFF_F) ='N'
		AND 
		TRIM(UPPER(A.COMM_TP)) = 'RESIDENTIAL'
		AND 
		A.CRNT_BAL_AMT>0 
		AND 
		A.STEP_PLN_AGRMNT_NUM NOT NULL
	GROUP BY
		A.STEP_PLN_AGRMNT_NUM, A.MTH_TM_ID
		)
		
	SELECT
	'{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
	STEP_PLN_AGRMNT_NUM,
	MAX(YTD_PRPY_AMT) AS STEP_TOT_PREPAY_YTD_MTG_MAX12M
	FROM 
		BASE
	GROUP BY
		STEP_PLN_AGRMNT_NUM    
    )
    """
):
    pass

