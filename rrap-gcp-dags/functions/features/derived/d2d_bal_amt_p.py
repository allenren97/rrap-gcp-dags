import os
from datetime import timedelta
import pendulum

from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import _pull_asset_event_extras, _push_asset_event_extras


UPSTREAM_ASSET = [ 
    'ingestion.MORT_MTH_SNAPSHOT', 
    'ingestion.CUST_XREF',
    'ingestion.IWF_CUST_ACCT',
    'ingestion.IWD_PD_PLN'
]
DOWNSTREAM_ASSET = "features.D2D_BAL_AMT_P"
DEPENDENCIES = {
    'duckdb_clear_derive_d2dbalp': ['duckdb_derive_d2dbalp'],
}


def duckdb_clear_derive_d2dbalp(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    DELETE FROM { DOWNSTREAM_ASSET } 
    WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass

def duckdb_derive_d2dbalp(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET}
    SELECT 
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' as OBSN_DT, 
        D2D_BAL_AMT_P,    
        aa.basel_acct_id          
    FROM {UPSTREAM_ASSET[0]} aa INNER JOIN {UPSTREAM_ASSET[1]} bb 
    ON trim(bb.cust_ID) = trim(aa.PRIM_CUST_CID)
    LEFT JOIN (
    SELECT 
        mth_tm_id,
        cust_base_key, 
        sum(
            CASE WHEN 
                sum_srvc_code in ('CHQ','SAV') and trim(prim_cust_f)='P'
            THEN acct_bal 
            ELSE 0 
            END) 
        as D2D_BAL_AMT_P
    FROM (
    SELECT 
        a.mth_tm_id, 
        b.cust_base_key, 
        e.sum_srvc_code, 
        acct_bal, 
        acct_lcst,
        prim_cust_f
    FROM (
    SELECT 
        DISTINCT mth_tm_id, 
        PRIM_CUST_CID 
        from {UPSTREAM_ASSET[0]}) a inner JOIN {UPSTREAM_ASSET[1]} b 
        ON trim(b.cust_ID) = trim(a.PRIM_CUST_CID)
        left JOIN {UPSTREAM_ASSET[2]} acct 
        ON b.cust_base_key = acct.cust_base_key 
        INNER JOIN {UPSTREAM_ASSET[3]} e 
        ON acct.pd_pln_key = e.pd_pln_key
        WHERE a.mth_tm_id = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}} 
        AND acct.time_key = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}} 
        and acct_lcst in ('A','I','D')
    ) group by cust_base_key, mth_tm_id 
    ) cc 
        ON bb.cust_base_key = cc.cust_base_key 
        AND aa.mth_tm_id =  cc.mth_tm_id
        WHERE aa.mth_tm_id = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    """

):
    pass

