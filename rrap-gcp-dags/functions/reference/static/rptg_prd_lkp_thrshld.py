import os
from datetime import timedelta
import pendulum
from airflow.sdk import get_current_context
from airflow.sdk.bases.sensor import PokeReturnValue
from bns.rrap.helpers.asset_event import _pull_asset_event_extras, _push_asset_event_extras

UPSTREAM_ASSET = [ f"ingestion.TM_DIM" ]
DOWNSTREAM_ASSET = f"reference.RPTG_PRD_LKP_THRSHLD"
DEPENDENCIES = {
    'branch_decide': ['duckdb_update'],
    'duckdb_update' : ['duckdb_load'],
}
INPUT_PATH = 'RPTG_PRD_LKP_THRSHLD.csv'


def branch_decide():
    context = get_current_context()
    rundate, _ = _pull_asset_event_extras(context, UPSTREAM_ASSET[0])
    path = os.path.join("/bns/rrap/data", f"{ rundate }", INPUT_PATH)

    if os.path.exists(path):
        context['ti'].xcom_push(key="csv", value=path)
        return ["static__rptg_prd_lkp_thrshld.duckdb_update"]
    
    return None

 
def duckdb_update(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    UPDATE {DOWNSTREAM_ASSET}
    SET 
        CRNT_F = 'N',
        EFF_TO_YR_MTH = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    WHERE CRNT_F = 'Y'
    """
):
    pass


def duckdb_load(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} BY NAME
    FROM (
        SELECT * FROM read_csv('{{{{ task_instance.xcom_pull(task_ids="static__rptg_prd_lkp_thrshld.branch_decide", key="csv") }}}}')
    )
    """
):
    pass
