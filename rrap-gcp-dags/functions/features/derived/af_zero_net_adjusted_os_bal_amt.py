import os
from datetime import timedelta
import pendulum

from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import _pull_asset_event_extras, _push_asset_event_extras


UPSTREAM_ASSET = [ 
    'features.ADJUSTED_OS_BAL_AMT',#0
    'features.SRC_SYS_CD',#1
    'features.CONSM_PRD_TREATMNT_CD',#2
    'features.SML_BUS_F',#3
    'features.PIT_STATUS_CROSS_DEFAULT_ORIG',#4
    'features.TRNST_EXCLSN_F',#5
    'features.DLGD_F',#6
    
    ]
DOWNSTREAM_ASSET = "features.AF_ZERO_NET_ADJUSTED_OS_BAL_AMT"
DEPENDENCIES = {
	
	
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


def duckdb_load(
     duckdb_conn_id='duckdb-conn',
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} BY NAME
    FROM (
        SELECT 
        a.BASEL_ACCT_ID,
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' as OBSN_DT,
        sum
            (case 
                when (ADJUSTED_OS_BAL_AMT) <=0 then 0 
                else (ADJUSTED_OS_BAL_AMT) 
            end)
            as AF_ZERO_NET_ADJUSTED_OS_BAL_AMT
        FROM 
                    (select basel_acct_id, adjusted_os_bal_amt from {UPSTREAM_ASSET[0]} where obsn_dt='{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}') a 
        left join   (select basel_acct_id, src_sys_cd from {UPSTREAM_ASSET[1]} where obsn_dt='{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}') b
            on a.basel_acct_id = b.basel_acct_id
        left join   (select basel_acct_id, consm_prd_treatmnt_cd from {UPSTREAM_ASSET[2]}  where obsn_dt='{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}') c
            on a.basel_acct_id=c.basel_acct_id
        left join   (select basel_acct_id, sml_bus_f from {UPSTREAM_ASSET[3]} where obsn_dt='{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}') d 
            on a.basel_acct_id=d.basel_acct_id
        left join   (select basel_acct_id, PIT_STATUS_CROSS_DEFAULT_ORIG as PIT_STAT_CD from {UPSTREAM_ASSET[4]}  where obsn_dt='{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}') e
            on a.basel_acct_id=e.basel_acct_id
        left join (select basel_acct_id, trnst_exclsn_f from {UPSTREAM_ASSET[5]}  where obsn_dt='{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}') f
            on a.basel_acct_id=f.basel_acct_id
        left join (select basel_acct_id, dlgd_f from {UPSTREAM_ASSET[6]}  where obsn_dt='{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}') g
            on a.basel_acct_id=g.basel_acct_id
        
        
        WHERE 
            b.SRC_SYS_CD in ('TNG-MOR','KS','MOR','SPL') 
        and c.CONSM_PRD_TREATMNT_CD='A' 
        and d.SML_BUS_F='N'
        and e.PIT_STAT_CD in ('CUR','DEF')
        and f.TRNST_EXCLSN_F='N'
        and g.DLGD_F='N'
        
        
        GROUP BY a.basel_acct_id
    )
    """   
): pass
