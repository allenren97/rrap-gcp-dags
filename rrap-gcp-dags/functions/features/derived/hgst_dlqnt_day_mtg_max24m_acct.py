import pyarrow as pa
from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

UPSTREAM_ASSET = [ 'features.DLQNT_DAY', 'ingestion.MORT_MTH_SNAPSHOT', 'ingestion.TM_DIM' ]
DOWNSTREAM_ASSET = 'features.HGST_DLQNT_DAY_MTG_MAX24M_ACCT'
DEPENDENCIES = {
    'duckdb_clear_hgst_dlqnt_day_mtg_max24m_acct': ['duckdb_derive_hgst_dlqnt_day_mtg_max24m_acct'],
}


def duckdb_clear_hgst_dlqnt_day_mtg_max24m_acct(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
        DELETE FROM { DOWNSTREAM_ASSET } 
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass


def duckdb_derive_hgst_dlqnt_day_mtg_max24m_acct(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    INSERT INTO { DOWNSTREAM_ASSET }
    BY NAME FROM (
    WITH 
    CURR_ACCOUNTS AS (
    SELECT
		A.BASEL_ACCT_ID
    FROM
		{UPSTREAM_ASSET[1]} A
    WHERE
		A.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    AND
    	UPPER(TRIM(A.COMM_TP))='RESIDENTIAL'
	AND
		A.CRNT_BAL_AMT>0
	AND
		TRIM(A.PD_OFF_F)='N'
    ),
    
    BASE AS (
	SELECT
		A.BASEL_ACCT_ID,
		A.DLQNT_DAY,
        A.OBSN_DT,
        C.TM_ID
	FROM 
		{UPSTREAM_ASSET[0]} A
    INNER JOIN
		{UPSTREAM_ASSET[2]} C
    ON
		A.OBSN_DT = C.TM_LVL_END_DT
	INNER JOIN 
		{UPSTREAM_ASSET[1]} B
	ON
	A.BASEL_ACCT_ID = B.BASEL_ACCT_ID
	AND
	C.TM_ID = B.MTH_TM_ID
    INNER JOIN
		CURR_ACCOUNTS D
    ON
		A.BASEL_ACCT_ID = D.BASEL_ACCT_ID
	WHERE
		UPPER(TRIM(B.COMM_TP))='RESIDENTIAL'
		AND
		B.CRNT_BAL_AMT>0
		AND
		TRIM(B.PD_OFF_F)='N'
		AND
		A.OBSN_DT BETWEEN
			DATE_ADD(DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}', -INTERVAL 23 MONTH) 
            AND
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
	)
	
	SELECT
		'{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
		BASEL_ACCT_ID,
		MAX(DLQNT_DAY) AS HGST_DLQNT_DAY_MTG_MAX24M_ACCT
	FROM BASE
	GROUP BY BASEL_ACCT_ID
    )
    """
):
    pass

