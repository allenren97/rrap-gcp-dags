import os
from datetime import timedelta
import pendulum
from airflow.sdk import get_current_context
from airflow.sdk.bases.sensor import PokeReturnValue
from bns.rrap.helpers.asset_event import _pull_asset_event_extras, _push_asset_event_extras

UPSTREAM_ASSET = [ f"ingestion.TM_DIM" ]
DOWNSTREAM_ASSET = f"reference.DLVY_KEY_TO_TXN_GRP_MAPPNG_LKP"
DEPENDENCIES = {
    'branch_decide': ['duckdb_delete'],
    'duckdb_delete' : ['duckdb_load'],
}
INPUT_PATH = 'DLVY_KEY_TO_TXN_GRP_MAPPNG_LKP.csv'


def branch_decide():
    context = get_current_context()
    rundate, _ = _pull_asset_event_extras(context, UPSTREAM_ASSET[0])
    path = os.path.join("/bns/rrap/data", f"{ rundate }", INPUT_PATH)

    if os.path.exists(path):
        context['ti'].xcom_push(key="csv", value=path)
        return ["static__dlvy_key_to_txn_grp_mappng_lkp.duckdb_delete"]
    
    return None


def duckdb_delete(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    DELETE FROM {DOWNSTREAM_ASSET}
    WHERE DLVY_KEY IN (
        SELECT DLVY_KEY FROM '{{{{ task_instance.xcom_pull(task_ids="static__dlvy_key_to_txn_grp_mappng_lkp.branch_decide", key="csv") }}}}'
    )
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
        FROM '{{{{ task_instance.xcom_pull(task_ids="static__dlvy_key_to_txn_grp_mappng_lkp.branch_decide", key="csv") }}}}'
    )
    """
):
    pass
