import os
from datetime import timedelta
import pendulum

from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import _pull_asset_event_extras, _push_asset_event_extras


UPSTREAM_ASSET = [ 'ingestion.AIRB_MORT_MTH_SNAPSHOT']
DOWNSTREAM_ASSET = "features.BULK_IND"
DEPENDENCIES = {
    'duckdb_clear_derive_bulk_ind': ['duckdb_derive_bulk_ind'], 
}
REFERENCES = ['reference.GEN_LKP','reference.GENWORTH_BULKINS']


def duckdb_clear_derive_bulk_ind(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    DELETE FROM { DOWNSTREAM_ASSET } 
    WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass

def duckdb_derive_bulk_ind(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} 
    with BASE as(
        SELECT 
        MORT_NUM, 
        tm_id, 
        cast(cl.genl_lkp_cd as int) as CLASS,
        case
        when Class in (54,74) OR (Class in (71,72) and gb.lender_loan is not null) then 'Y'
        else 'N'
        end AS Bulk_ind_calc
        FROM {UPSTREAM_ASSET[0]} bm
        LEFT join {REFERENCES[0]} cl
        ON bm.class = cl.GENL_LKP_CD 
        AND cl.genl_lkp_tp_nm='CL_CD_LKP_ID' 
        and cl.TBL_NM='BASEL_MORT'
        LEFT JOIN {REFERENCES[1]} gb 
        on gb.lender_loan = bm.mort_num
        WHERE tm_id = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}') 
        SELECT 
        MORT_NUM, 
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        Bulk_ind_calc as BULK_IND 
        from BASE

    """

):
    pass

