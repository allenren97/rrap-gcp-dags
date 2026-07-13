import os
from datetime import timedelta
import pendulum

from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import _pull_asset_event_extras, _push_asset_event_extras


UPSTREAM_ASSET = [ 
'ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT',#0
'features.PRD_ID',#1
'reference.PSNL_LOAN_RPTG_PRD_LKP',#2
'ingestion.MORT_MTH_SNAPSHOT',#3
'features.BULK_IND',#4
'reference.MORT_RPTG_PRD_LKP',#5
'ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT',#6
'features.PRD_CD',#7
'features.TOTAL_EXPSR_ABOVE_LMT_F',#8
'features.HELOC_F',#9
'features.BASEL_PRD_CD',#10
'reference.BASEL_RPTG_PRD_LKP ',#11
'ingestion.TNG_ACCT_MO',#12
'ingestion.BASEL_ACCT_DIM',#13
'features.SRC_SYS_CD',#14

]
DOWNSTREAM_ASSET = "features.ASST_CL_NUM"
DEPENDENCIES = {
    'duckdb_clear':['export_spl','export_ks','export_mor','export_tng'],
	'export_spl':['duckdb_load'],
	'export_ks':['duckdb_load'],
	'export_mor':['duckdb_load'],
	'export_tng':['duckdb_load']
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
    with base as (                        
        select 
        a.basel_acct_id, 
        b.PRD_ID,
        FROM {UPSTREAM_ASSET[0]} a 
        left join {UPSTREAM_ASSET[1]} b 
        on a.basel_acct_id=b.basel_acct_id
        and obsn_dt='{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
        where MTH_TM_ID={{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}} 
        )
        select 
        a.BASEL_ACCT_ID, 
        c.ASST_CL_NUM,
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' as OBSN_DT
        from base a
        left join {UPSTREAM_ASSET[2]} c
        ON a.PRD_ID = c.PRD_ID AND c.SRC_SYS_CD = 'SPL'
    """   
): pass

def export_mor(
     duckdb_conn_id='duckdb-conn',
    sql=f"""
    with base as (                        
    select 
    a.basel_acct_id, 
    a.INSUR_GRP,
    b.BULK_IND
    FROM {UPSTREAM_ASSET[3]} a 
    left join {UPSTREAM_ASSET[4]} b 
    on a.mort_num=b.mort_num
    where MTH_TM_ID={{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}  
    and obsn_dt='{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    )
    select 
    a.BASEL_ACCT_ID, 
    c.ASST_CL_NUM,
    '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' as OBSN_DT 
    from base a
    left join {UPSTREAM_ASSET[5]} c
    ON a.INSUR_GRP = c.basel_mort_insurer_grp_desc 
    AND a.BULK_IND = c.BULK_F
    """   
): pass

def export_ks(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    with base as (
        select 
        a.BASEL_ACCT_ID, 
        b.PRD_CD,
        a.SUB_PRD_CD,
        d.TOTAL_EXPSR_ABOVE_LMT_F,
        e.HELOC_F,
        f.BASEL_PRD_CD
        
        from 
        {UPSTREAM_ASSET[6]} a 
        left join 
        (select BASEL_ACCT_ID,PRD_CD from {UPSTREAM_ASSET[7]} where obsn_dt='{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}')b 
        on a.basel_acct_id=b.basel_acct_id
        left join 
        (select BASEL_ACCT_ID,TOTAL_EXPSR_ABOVE_LMT_F from {UPSTREAM_ASSET[8]} where obsn_dt='{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}')d 
        on a.basel_acct_id=d.basel_acct_id
        left join 
        (select BASEL_ACCT_ID,HELOC_F from {UPSTREAM_ASSET[9]} where obsn_dt='{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}')e 
        on a.basel_acct_id=e.basel_acct_id
        left join 
        (select BASEL_ACCT_ID,BASEL_PRD_CD from {UPSTREAM_ASSET[10]} where obsn_dt='{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}')f 
        on a.basel_acct_id=f.basel_acct_id
        
        where mth_tm_id={{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    )
        select
        a.BASEL_ACCT_ID,
        b.ASST_CL_NUM,
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' as OBSN_DT
        FROM base a
            LEFT JOIN
            {UPSTREAM_ASSET[11]} b
            ON a.PRD_CD = TRIM(b.PRD_CD)
            AND a.SUB_PRD_CD = TRIM(b.SUB_PRD_CD)
            AND a.TOTAL_EXPSR_ABOVE_LMT_F = TRIM(b.REVISED_EXPSR_OV_125K_F)
            AND a.HELOC_F = TRIM(b.HELOC_F)
            AND a.BASEL_PRD_CD = TRIM(b.BASEL_PRD_CD)
"""
):
    pass

def export_tng(
 	duckdb_conn_id='duckdb-conn',
	sql=f"""
	WITH accts as (SELECT
    BASEL_ACCT_ID,
    MONTH_END_DT, 
    account_id,
    case 
    when UPPER(bulk_nsurer_desc)='BULKINSURED' then 'Y'
    else 'N'
    end as bulk_ind,
    INSURER_DESC
    FROM {UPSTREAM_ASSET[12]} a 
    INNER JOIN {UPSTREAM_ASSET[13]} b 
    ON 
    a.ACCOUNT_ID = b.SRC_APP_ID
    AND b.SRC_APP_CD ='TNG-MOR'
    AND b.SRC_SYS_DEL_F != 'Y' 
    and MONTH_END_DT='{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'),
    base as (
    select a.*, 
    SRC_SYS_CD 
    from accts a 
    left join {UPSTREAM_ASSET[14]} b 
    on a.basel_acct_id=b.basel_acct_id
    where obsn_dt='{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    )
    select 
    BASEL_ACCT_ID, 
    ASST_CL_NUM,
    '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' as OBSN_DT
    from base a 
    left join 
    {UPSTREAM_ASSET[5]} b
    on (UPPER(a.src_sys_cd) = UPPER(b.SRC_SYS_CD) and
    UPPER(a.INSURER_DESC) = UPPER(b.BASEL_MORT_INSURER_GRP_DESC) and
    UPPER(a.bulk_ind) = UPPER(b.BULK_F))
	"""
):pass

def duckdb_load(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} BY NAME
    FROM (
            SELECT OBSN_DT, 
            BASEL_ACCT_ID, 
            ASST_CL_NUM          
            FROM read_parquet('{{{{ task_instance.xcom_pull(task_ids="derived__asst_cl_num.export_ks", key="parquet") }}}}')
            UNION 
            SELECT OBSN_DT, 
            BASEL_ACCT_ID, 
            ASST_CL_NUM    
            FROM read_parquet('{{{{ task_instance.xcom_pull(task_ids="derived__asst_cl_num.export_spl", key="parquet") }}}}')
     		UNION 
            SELECT OBSN_DT, 
            BASEL_ACCT_ID, 
            ASST_CL_NUM
            FROM read_parquet('{{{{ task_instance.xcom_pull(task_ids="derived__asst_cl_num.export_mor", key="parquet") }}}}')
            UNION 
            SELECT OBSN_DT, 
            BASEL_ACCT_ID, 
            ASST_CL_NUM
            FROM read_parquet('{{{{ task_instance.xcom_pull(task_ids="derived__asst_cl_num.export_tng", key="parquet") }}}}')

    )
    """
):
    pass
