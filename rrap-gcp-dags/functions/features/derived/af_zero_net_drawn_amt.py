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
    'features.ADJUSTED_OS_BAL_AMT_SECURITIZATION'#9
    ]
DOWNSTREAM_ASSET = "features.AF_ZERO_NET_DRAWN_AMT"
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
	with ADJUSTED_OS_BAL_AMT as (
        SELECT
        BASEL_ACCT_ID,
        ADJUSTED_OS_BAL_AMT_SECURITIZATION as ADJUSTED_OS_BAL_AMT 
        FROM features.ADJUSTED_OS_BAL_AMT_SECURITIZATION
        where OBSN_DT='{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' 
    ), final as(
        SELECT BASEL_ACCT_ID,
        CASE 
        WHEN ADJUSTED_OS_BAL_AMT < 0 THEN 0 
        ELSE ADJUSTED_OS_BAL_AMT 
        END
        AS AF_ZERO_NET_DRAWN_AMT,
        FROM ADJUSTED_OS_BAL_AMT
    )
        select
        BASEL_ACCT_ID, 
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'  AS OBSN_DT,
        AF_ZERO_NET_DRAWN_AMT,
        from final
    """   
): pass

def export_mor(
     duckdb_conn_id='duckdb-conn',
    sql=f"""
	 select 
		BASEL_ACCT_ID,  
		AF_ZERO_NET_DRAWN_AMT,
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' as OBSN_DT
	from 
		ingestion.BASELAYER_MOR a left join ingestion.MORT_MTH_SNAPSHOT b 
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
        base as (
        select 
        basel_acct_id
        from ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT
        where mth_tm_id={{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
        ),
        AF_ADJ_OS_BAL_AMT AS (
        SELECT
        BASEL_ACCT_ID,
        AF_ADJ_OS_BAL_AMT 
        FROM features.AF_ADJ_OS_BAL_AMT
        where 
        SRC_SYS_CD = 'KS' 
        and 
        OBSN_DT='{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'

        ),
        final as(
        SELECT BASEL_ACCT_ID,
        (CASE 
        WHEN round(AF_ADJ_OS_BAL_AMT,8) < 0 THEN 0 
        ELSE round(AF_ADJ_OS_BAL_AMT,8) 
        END)
        AS AF_ZERO_NET_DRAWN_AMT,
        FROM AF_ADJ_OS_BAL_AMT
        )
        select
        a.BASEL_ACCT_ID, 
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'  AS OBSN_DT,
        AF_ZERO_NET_DRAWN_AMT,
        from base a left join final b 
        on a.basel_acct_id=b.basel_acct_id
"""
):
    pass

def export_tng(
 	duckdb_conn_id='duckdb-conn',
	sql=f"""
	with tng as (
	SELECT
      	BASEL_ACCT_ID
        FROM ingestion.TNG_ACCT_MO a INNER JOIN ingestion.BASEL_ACCT_DIM b 
        ON 
            a.ACCOUNT_ID = b.SRC_APP_ID
            AND b.SRC_APP_CD ='TNG-MOR'
            AND b.SRC_SYS_DEL_F != 'Y' 
        and MONTH_END_DT='{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'

        ),adjusted_os_bal_amt AS (
        SELECT
        BASEL_ACCT_ID,
        adjusted_os_bal_amt 
        FROM features.adjusted_os_bal_amt
        where 
        SRC_SYS_CD = 'TNG' 
        and 
        OBSN_DT='{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
        )
        select 
        a.basel_acct_id,
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'  AS OBSN_DT,
        case 
            when adjusted_os_bal_amt < 0 then 0
            else 
            adjusted_os_bal_amt
            end as AF_ZERO_NET_DRAWN_AMT
        from tng a left join adjusted_os_bal_amt b 
        on a.basel_acct_id=b.basel_acct_id
	"""
):pass

def duckdb_load(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} BY NAME
    FROM (
            SELECT OBSN_DT, 
            BASEL_ACCT_ID, 
            AF_ZERO_NET_DRAWN_AMT            
            FROM read_parquet('{{{{ task_instance.xcom_pull(task_ids="derived__af_zero_net_drawn_amt.export_ks", key="parquet") }}}}')
            UNION 
            SELECT OBSN_DT, 
            BASEL_ACCT_ID, 
            AF_ZERO_NET_DRAWN_AMT          
            FROM read_parquet('{{{{ task_instance.xcom_pull(task_ids="derived__af_zero_net_drawn_amt.export_spl", key="parquet") }}}}')
     		UNION 
            SELECT OBSN_DT, 
            BASEL_ACCT_ID, 
            AF_ZERO_NET_DRAWN_AMT
            FROM read_parquet('{{{{ task_instance.xcom_pull(task_ids="derived__af_zero_net_drawn_amt.export_mor", key="parquet") }}}}')
            UNION 
            SELECT OBSN_DT, 
            BASEL_ACCT_ID, 
            AF_ZERO_NET_DRAWN_AMT
            FROM read_parquet('{{{{ task_instance.xcom_pull(task_ids="derived__af_zero_net_drawn_amt.export_tng", key="parquet") }}}}')

    )
    """
):
    pass
