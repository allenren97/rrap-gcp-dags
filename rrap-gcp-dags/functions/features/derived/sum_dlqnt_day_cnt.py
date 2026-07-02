import pyarrow as pa
from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

UPSTREAM_ASSET = [ 
    'ingestion.MORT_MTH_SNAPSHOT', 
    'ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT',
    'ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT',
    'features.MODEL_EXCL_F',
    'features.WRITTEN_OUT_F',
    'features.TREATMENT_F',
    'features.DLQNT_DAY_CNT']

DOWNSTREAM_ASSET = 'features.SUM_DLQNT_DAY_CNT'
DEPENDENCIES = {
    'duckdb_clear_sum_dlqnt_day_cnt': ['duckdb_derive_sum_dlqnt_day_cnt'],
}


def duckdb_clear_sum_dlqnt_day_cnt(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
        DELETE FROM { DOWNSTREAM_ASSET } 
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass


def duckdb_derive_sum_dlqnt_day_cnt(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    INSERT INTO { DOWNSTREAM_ASSET }
    BY NAME FROM (
		WITH non_excl AS (
						SELECT BASEL_ACCT_ID FROM {UPSTREAM_ASSET[3]}  
						WHERE MODEL_EXCL_F = 'N' AND OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
						INTERSECT
						SELECT BASEL_ACCT_ID FROM {UPSTREAM_ASSET[4]} 
						WHERE WRITTEN_OUT_F = 'N' AND OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
						INTERSECT
						SELECT BASEL_ACCT_ID FROM {UPSTREAM_ASSET[5]} 
						WHERE TREATMENT_F = 'A' AND OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
						),


		MOR AS (
					SELECT
						A.MTH_TM_ID,
						A.PRIM_BASEL_CUST_ID AS BASEL_CUST_ID, 
						B.DLQNT_DAY_CNT
					FROM 
						{UPSTREAM_ASSET[0]}  A
					INNER JOIN 
						{UPSTREAM_ASSET[6]}  B 
					ON 
						A.BASEL_ACCT_ID = B.BASEL_ACCT_ID
					RIGHT JOIN 
						non_excl C 
					ON
						A. BASEL_ACCT_ID = C.BASEL_ACCT_ID
					WHERE 
						A.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
					AND
						UPPER(TRIM(B.SRC_SYS_CD)) = 'MO'
					AND
						B.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
				),
		KS AS 	(	
					SELECT
						A.MTH_TM_ID,
						A.PRIM_BASEL_CUST_ID AS BASEL_CUST_ID, 
						B.DLQNT_DAY_CNT
					FROM 
						{UPSTREAM_ASSET[1]}  A
					INNER JOIN 
						{UPSTREAM_ASSET[6]}  B 
					ON 
						A.BASEL_ACCT_ID = B.BASEL_ACCT_ID
					RIGHT JOIN 
						non_excl C 
					ON
						A. BASEL_ACCT_ID = C.BASEL_ACCT_ID
					WHERE 
						A.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
					AND 
						A.PRIM_BASEL_CUST_ID > 0
					AND 
						UPPER(TRIM(B.SRC_SYS_CD)) = 'KS'
					AND
						B.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
				),
				
		PSNL AS (
					SELECT
						A.MTH_TM_ID,
						A.PRIM_BASEL_CUST_ID AS BASEL_CUST_ID, 
						B.DLQNT_DAY_CNT
					FROM 
						{UPSTREAM_ASSET[2]}  A
					INNER JOIN 
						{UPSTREAM_ASSET[6]}  B 
					ON 
						A.BASEL_ACCT_ID = B.BASEL_ACCT_ID
					RIGHT JOIN 
						non_excl C 
					ON
						A. BASEL_ACCT_ID = C.BASEL_ACCT_ID
					WHERE 
						A.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
					AND 
						A.PRIM_BASEL_CUST_ID > 0
					AND 
						UPPER(TRIM(B.SRC_SYS_CD)) = 'SPL'
					AND
						B.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
				)				
		SELECT
			'{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
			B.MTH_TM_ID,
			B.BASEL_CUST_ID,
			SUM(B.DLQNT_DAY_CNT) AS SUM_DLQNT_DAY_CNT
		FROM
			(SELECT * FROM MOR
			UNION ALL
			SELECT * FROM KS
			UNION ALL
			SELECT * FROM PSNL
			) B
		GROUP BY
			B.MTH_TM_ID,
			B.BASEL_CUST_ID
    )
    """
):
    pass

