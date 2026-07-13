import os
from datetime import timedelta
import pendulum

from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import _pull_asset_event_extras, _push_asset_event_extras


UPSTREAM_ASSET = [ 
    'ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT',#0
    'ingestion.BASELAYER_MOR',#1
    'ingestion.MORT_MTH_SNAPSHOT',#2
	'features.REVISED_EXPSR_AMT',#3
	'features.AF_ADJ_OS_BAL_AMT',#4
    'ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT',#5
    'ingestion.TNG_ACCT_MO',#6
    'ingestion.BASEL_ACCT_DIM'#7
    ]
DOWNSTREAM_ASSET = "features.AF_ZERO_NET_UNDRAWN_AMT"
DEPENDENCIES = {
	'export_spl':['duckdb_clear'],
	'export_ks':['duckdb_clear'],
	'export_mor':['duckdb_clear'],
	'export_tng':['duckdb_clear'],
    'duckdb_clear': ['duckdb_load']
}


def duckdb_clear(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    DELETE FROM { DOWNSTREAM_ASSET } 
    WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass

def export_spl(
     duckdb_conn_id='duckdb-conn',
    sql=f"""
	SELECT 
		BASEL_ACCT_ID, 
		0 AS AF_ZERO_NET_UNDRAWN_AMT,
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' as OBSN_DT
	FROM 
		{UPSTREAM_ASSET[0]}
    WHERE
     	 mth_tm_id = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}'
    """   
): pass

def export_mor(
     duckdb_conn_id='duckdb-conn',
    sql=f"""
	select 
		BASEL_ACCT_ID,  
		AF_ZERO_NET_UNDRAWN_AMT,
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' as OBSN_DT
	from 
		{UPSTREAM_ASSET[1]} a left join {UPSTREAM_ASSET[2]} b 
    on 
    	a.mort_num=b.mort_num 
    	and a.mth_end_dt=b.mth_end_dt 
    where 
    	a.mth_end_dt = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """   
): pass

    

def export_ks(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
		WITH 
		REVISED_EXPSR_AMT as (
				SELECT 
				BASEL_ACCT_ID,
				REVISED_EXPSR_AMT
				FROM
				{UPSTREAM_ASSET[3]}
				where OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
			),
		AF_ADJ_OS_BAL_AMT AS (
			SELECT
			BASEL_ACCT_ID,
			AF_ADJ_OS_BAL_AMT,
			ROUND(AF_ADJ_OS_BAL_AMT, 8) AS VADJUSTED_OS_BAL_AMT
			FROM (
					SELECT
					BASEL_ACCT_ID,
					AF_ADJ_OS_BAL_AMT 
					FROM {UPSTREAM_ASSET[4]}
					where 
					SRC_SYS_CD = 'KS' 
					and 
					OBSN_DT='{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
				)
		),
		JOINED as (
			SELECT
			a.BASEL_ACCT_ID,
			AF_ADJ_OS_BAL_AMT,
			VADJUSTED_OS_BAL_AMT
			from
			{UPSTREAM_ASSET[5]} a LEFT JOIN AF_ADJ_OS_BAL_AMT b 
			on a.BASEL_ACCT_ID=b.BASEL_ACCT_ID
			WHERE mth_tm_id={{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
		),
		final as(
			SELECT T1.BASEL_ACCT_ID,
			CASE
			WHEN AF_ADJ_OS_BAL_AMT > 0 THEN
				ROUND(
					COALESCE(REVISED_EXPSR_AMT, 0) - COALESCE(VADJUSTED_OS_BAL_AMT, 0)
				, 3)
			ELSE
				ROUND(REVISED_EXPSR_AMT, 3)
			END AS AF_ZERO_NET_UNDRAWN_AMT
			FROM JOINED T1 JOIN REVISED_EXPSR_AMT T2
			ON T1.BASEL_ACCT_ID=T2.BASEL_ACCT_ID
		)
		select
		BASEL_ACCT_ID, 
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'  AS OBSN_DT,
		CASE 
			when AF_ZERO_NET_UNDRAWN_AMT < 0 THEN 0
			ELSE AF_ZERO_NET_UNDRAWN_AMT
		END as AF_ZERO_NET_UNDRAWN_AMT
		from final
"""
):
    pass



def export_tng(
 	duckdb_conn_id='duckdb-conn',
	sql=f"""
	SELECT
      	BASEL_ACCT_ID,
		0 AS AF_ZERO_NET_UNDRAWN_AMT,
		'{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' as OBSN_DT
  	FROM {UPSTREAM_ASSET[6]} a INNER JOIN {UPSTREAM_ASSET[7]} b 
    ON 
    	a.ACCOUNT_ID = b.SRC_APP_ID
    	AND b.SRC_APP_CD ='TNG-MOR'
    	AND b.SRC_SYS_DEL_F != 'Y' 
      and MONTH_END_DT='{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'

	"""
):pass

def duckdb_load(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} BY NAME
    FROM (
            SELECT OBSN_DT, 
            BASEL_ACCT_ID, 
            AF_ZERO_NET_UNDRAWN_AMT            
            FROM read_parquet('{{{{ task_instance.xcom_pull(task_ids="derived__af_zero_net_undrawn_amt.export_ks", key="parquet") }}}}')
            UNION 
            SELECT OBSN_DT, 
            BASEL_ACCT_ID, 
            AF_ZERO_NET_UNDRAWN_AMT          
            FROM read_parquet('{{{{ task_instance.xcom_pull(task_ids="derived__af_zero_net_undrawn_amt.export_spl", key="parquet") }}}}')
     		UNION 
            SELECT OBSN_DT, 
            BASEL_ACCT_ID, 
            AF_ZERO_NET_UNDRAWN_AMT
            FROM read_parquet('{{{{ task_instance.xcom_pull(task_ids="derived__af_zero_net_undrawn_amt.export_mor", key="parquet") }}}}')
            UNION 
            SELECT OBSN_DT, 
            BASEL_ACCT_ID, 
            AF_ZERO_NET_UNDRAWN_AMT
            FROM read_parquet('{{{{ task_instance.xcom_pull(task_ids="derived__af_zero_net_undrawn_amt.export_tng", key="parquet") }}}}')

    )
    """
):
    pass
