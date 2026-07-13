import os
from datetime import timedelta
import pendulum
from airflow.sdk import get_current_context
from airflow.sdk.bases.sensor import PokeReturnValue
from bns.rrap.helpers.asset_event import _pull_asset_event_extras, _push_asset_event_extras

UPSTREAM_ASSET = [ f"ingestion.TM_DIM" ]
DOWNSTREAM_ASSET = f"reference.IND_COST_DFT_ALLOC_LKP"
DEPENDENCIES = {
    'branch_decide': ['duckdb_delete'],
    'duckdb_delete' : ['duckdb_load'],
    'duckdb_load': ['duckdb_postprocess_trim'],
}
INPUT_PATH = 'IND_COST_DFT_ALLOC_LKP.csv'


def branch_decide():
    context = get_current_context()
    rundate, _ = _pull_asset_event_extras(context, UPSTREAM_ASSET[0])
    path = os.path.join("/bns/rrap/data", f"{ rundate }", INPUT_PATH)

    if os.path.exists(path):
        context['ti'].xcom_push(key="csv", value=path)
        return ["static__ind_cost_dft_alloc_lkp.duckdb_delete"]
    
    return None


def duckdb_delete(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    DELETE FROM {DOWNSTREAM_ASSET} current
    WHERE EXISTS (
        SELECT * FROM read_csv('{{{{ task_instance.xcom_pull(task_ids="static__ind_cost_dft_alloc_lkp.branch_decide", key="csv") }}}}') incoming
        WHERE trim(incoming.TRNST_NUM) = trim(current.TRNST_NUM)
        AND upper(trim(incoming.ACCOUNT_STATUS)) = upper(trim(current.ACCOUNT_STATUS))
        AND incoming.EFF_FROM_YR_MTH = current.EFF_FROM_YR_MTH
        AND incoming.EFF_TO_YR_MTH = current.EFF_TO_YR_MTH
    )
    """
):
    pass


def duckdb_load(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} BY NAME
    FROM (
        SELECT * FROM read_csv('{{{{ task_instance.xcom_pull(task_ids="static__ind_cost_dft_alloc_lkp.branch_decide", key="csv") }}}}')
    )
    """
):
    pass


def duckdb_postprocess_trim(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    UPDATE {DOWNSTREAM_ASSET}
    SET 
        TRNST_NUM = trim(TRNST_NUM),
        ACCOUNT_STATUS = trim(ACCOUNT_STATUS)
    """
):
    pass
