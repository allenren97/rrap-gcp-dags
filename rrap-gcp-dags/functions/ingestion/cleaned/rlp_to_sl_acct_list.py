import os
from datetime import timedelta
import pendulum
import duckdb
import hashlib

from airflow.sdk import get_current_context
from airflow.sdk.bases.sensor import PokeReturnValue

from bns.rrap.helpers.asset_event import _pull_asset_event_extras, _push_asset_event_extras
from bns.rrap.hooks.duckdb import DuckLakeHook


UPSTREAM_ASSET = [ "ingestion.TM_DIM" ]
DOWNSTREAM_ASSET = "ingestion.RLP_TO_SL_ACCT_LIST"
DEPENDENCIES = {
    'sensor_wait_for_table': ['branch_check_for_rlp_to_sl_updates'],
    'branch_check_for_rlp_to_sl_updates': ['duckdb_clear_rlp_to_sl_acct_list'],
    'duckdb_clear_rlp_to_sl_acct_list': ['duckdb_load_into_ducklake'],
    'duckdb_load_into_ducklake': ['empty_task'],
}


def sensor_wait_for_table(poke_interval=300, timeout=(60 * 60 * 24 * 8), mode='reschedule'):
    context = get_current_context()
    rundate, _ = _pull_asset_event_extras(context, UPSTREAM_ASSET[0])  # don't need MTH_TM_ID, this column not in RLP_TO_SL_ACCT_LIST data
    path = os.path.join("/bns/rrap/data/sas_inputs", 'RLP_TO_SL_ACCT_LIST.parquet')

    if os.path.exists(path):
        context['ti'].xcom_push(key="parquet", value=path)
        return PokeReturnValue(is_done=True)
    else:
        return PokeReturnValue(is_done=False)


# Python operator to check if RLP_TO_SL account list static file has been updated
def branch_check_for_rlp_to_sl_updates():
    # See if the static file md5sum output matches the md5sum
    # If not, update the md5sum file and proceed to next steps
    context = get_current_context()
    parquet_filename = context['ti'].xcom_pull(task_ids=f"cleaned__rlp_to_sl_acct_list.sensor_wait_for_table", key="parquet")
    md5sum_filename = os.path.join("/bns/rrap/data/sas_inputs", "RLP_TO_SL_ACCT_LIST.parquet.md5sum")

    md5sum_hash = hashlib.md5(open(parquet_filename, 'rb').read()).hexdigest()

    # If there does not yet exist an .md5sum for the file, create it
    if not os.path.exists(md5sum_filename):
        with open(md5sum_filename, 'w') as f:
            f.write(md5sum_hash)
        return 'duckdb_load_into_ducklake'
    else:
        existing_hash = open(md5sum_filename).read().split(' ')[0]
        if md5sum_hash == existing_hash:
            return 'empty_task'
        else:
            return 'duckdb_clear_rlp_to_sl_acct_list'


def duckdb_clear_rlp_to_sl_acct_list(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    TRUNCATE ingestion.RLP_TO_SL_ACCT_LIST
    """
):
    pass


def duckdb_load_into_ducklake(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    INSERT INTO { DOWNSTREAM_ASSET }
    FROM (
    SELECT * FROM '{{{{ task_instance.xcom_pull(task_ids="cleaned__rlp_to_sl_acct_list.sensor_wait_for_table", key="parquet") }}}}'
    )
    """
):
    pass


def empty_task():
    print("Do nothing.")
