import os
from datetime import timedelta
import pendulum
from airflow.sdk import get_current_context
from airflow.sdk.bases.sensor import PokeReturnValue
from bns.rrap.helpers.asset_event import _pull_asset_event_extras, _push_asset_event_extras

UPSTREAM_ASSET = [ f"ingestion.TM_DIM" ]
DOWNSTREAM_ASSET = f"reference.PSNL_LOAN_SCRTY_CD_PRPS_CD_LKP"
DEPENDENCIES = {
    'branch_decide': ['duckdb_delete'],
    'duckdb_delete' : ['duckdb_load'],
}
INPUT_PATH = 'PSNL_LOAN_SCRTY_CD_PRPS_CD_LKP.csv'


def branch_decide():
    context = get_current_context()
    rundate, _ = _pull_asset_event_extras(context, UPSTREAM_ASSET[0])
    path = os.path.join("/bns/rrap/data", f"{ rundate }", INPUT_PATH)

    if os.path.exists(path):
        context['ti'].xcom_push(key="csv", value=path)
        return ["static__psnl_loan_scrty_cd_prps_cd_lkp.duckdb_delete"]
    
    return None


def duckdb_delete(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    UPDATE {DOWNSTREAM_ASSET} current
    SET
        CRNT_F = 'N',
        EFF_TO_YR_MTH = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="yyyymm") }}}}',
        UPDT_PROCESS_TMSTMP = current_timestamp       
    WHERE EXISTS (
        SELECT * FROM read_csv('{{{{ task_instance.xcom_pull(task_ids="static__psnl_loan_scrty_cd_prps_cd_lkp.branch_decide", key="csv") }}}}') incoming
        WHERE incoming.SCRTY_CD = current.SCRTY_CD
        AND incoming.PRPS_CD = current.PRPS_CD
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
            current_timestamp AS UPDT_PROCESS_TMSTMP,
        FROM '{{{{ task_instance.xcom_pull(task_ids="static__psnl_loan_scrty_cd_prps_cd_lkp.branch_decide", key="csv") }}}}'
    )
    """
):
    pass
