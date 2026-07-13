import os
from datetime import timedelta
import pendulum
from airflow.sdk import get_current_context
from airflow.sdk.bases.sensor import PokeReturnValue
from bns.rrap.helpers.asset_event import _pull_asset_event_extras, _push_asset_event_extras

UPSTREAM_ASSET = [ f"ingestion.TM_DIM" ]
DOWNSTREAM_ASSET = f"reference.BASEL_EGL_LKP_NZ"
DEPENDENCIES = {
    'branch_decide': ['duckdb_update'],
    'duckdb_update' : ['duckdb_load'],
    'duckdb_load': ['duckdb_postprocess_trim'],
}
INPUT_PATH = 'BASEL_EGL_LKP_NZ.csv'


def branch_decide():
    context = get_current_context()
    rundate, _ = _pull_asset_event_extras(context, UPSTREAM_ASSET[0])
    path = os.path.join("/bns/rrap/data", f"{ rundate }", INPUT_PATH)

    if os.path.exists(path):
        context['ti'].xcom_push(key="csv", value=path)
        return ["static__basel_egl_lkp_nz.duckdb_update"]
    
    return None


def duckdb_update(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    UPDATE {DOWNSTREAM_ASSET}
    SET 
        CRNT_F = 'N',
        EFF_TO_YR_MTH = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="yyyymm") }}}}',
        UPDT_PROCESS_TMSTMP = current_timestamp
    WHERE TRIM(PRD_CD) IN (
        SELECT TRIM(PRD_CD) FROM read_csv('{{{{ task_instance.xcom_pull(task_ids="static__basel_egl_lkp_nz.branch_decide", key="csv") }}}}')
    ) AND CRNT_F = 'Y'
    """
):
    pass


def duckdb_load(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} BY NAME
    FROM (
        SELECT * EXCLUDE(INSRT_PROCESS_TMSTMP, UPDT_PROCESS_TMSTMP),
        current_timestamp AS INSRT_PROCESS_TMSTMP,
        current_timestamp AS UPDT_PROCESS_TMSTMP
        FROM read_csv('{{{{ task_instance.xcom_pull(task_ids="static__basel_egl_lkp_nz.branch_decide", key="csv") }}}}')
    )
    """
):
    pass


def duckdb_postprocess_trim(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    UPDATE {DOWNSTREAM_ASSET}
    SET PRD_CD = TRIM(PRD_CD)
    """
):
    pass
