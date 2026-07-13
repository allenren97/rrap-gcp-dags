import os
from datetime import timedelta
import pendulum
import csv
import hashlib

from airflow.sdk import get_current_context
from airflow.sdk.bases.sensor import PokeReturnValue
from airflow.exceptions import AirflowSkipException

from bns.rrap.helpers.asset_event import _pull_asset_event_extras, _push_asset_event_extras


UPSTREAM_ASSET = [ "ingestion.TM_DIM" ]
DOWNSTREAM_ASSET = "ingestion.EDW_CMA_CITY_LKP"
DEPENDENCIES = {
    'sensor_wait_for_csv': ['reformat_file'],
    'reformat_file': ['check_for_edw_cma_updates'],
    'check_for_edw_cma_updates': ['duckdb_load_into_ducklake'],
}

RAW_CSV_PATH = os.path.join("/bns/rrap/data/sas_inputs", 'rrm_edwext_CMA_City_lookup_f_adhoc.csv')
REFORMAT_CSV_PATH = os.path.join("/bns/rrap/data/sas_inputs", 'EDW_CMA_CITY_LKP.csv')


def sensor_wait_for_csv(poke_interval=300, timeout=(60 * 60 * 24 * 8), mode='reschedule'):
    context = get_current_context()
    rundate, mth_tm_id = _pull_asset_event_extras(context, UPSTREAM_ASSET[0])  # don't need MTH_TM_ID, this column not in RLP_TO_SL_ACCT_LIST data
    path = os.path.join("/bns/rrap/data/reference", 'rrm_edwext_CMA_City_lookup_f_adhoc.csv')

    if os.path.exists(RAW_CSV_PATH):
        context['ti'].xcom_push(key="csv", value=REFORMAT_CSV_PATH)
        return PokeReturnValue(is_done=True)
    else:
        return PokeReturnValue(is_done=False)    


def reformat_file():
    current_metrpl_nm = ""
    with open(REFORMAT_CSV_PATH, 'w') as fout:
        writer = csv.writer(fout)
        with open(RAW_CSV_PATH, 'r', encoding='latin-1') as fin:
            reader = csv.reader(fin)
            for row in reader:
                if row[0] != "" and row[0] != current_metrpl_nm:
                    current_metrpl_nm = row[0]
                writer.writerow([current_metrpl_nm, row[1], row[2]])


def check_for_edw_cma_updates():
    # See if the static file md5sum output matches the md5sum
    # If not, update the md5sum file and proceed to next steps
    context = get_current_context()
    csv_filename = context['ti'].xcom_pull(task_ids=f"cleaned__edw_cma_city_lkp.sensor_wait_for_csv", key="csv")
    md5sum_filename = os.path.join("/bns/rrap/data/sas_inputs", "EDW_CMA_CITY_LKP.csv.md5sum")

    md5sum_hash = hashlib.md5(open(csv_filename, 'rb').read()).hexdigest()

    # If there does not yet exist an .md5sum for the file, create it
    if not os.path.exists(md5sum_filename):
        with open(md5sum_filename, 'w') as f:
            f.write(md5sum_hash)
        return True
    else:
        existing_hash = open(md5sum_filename).read().split(' ')[0]
        if md5sum_hash == existing_hash:
            raise AirflowSkipException(f"File {csv_filename} has not been updated. Skipping...")


def duckdb_load_into_ducklake(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
    INSERT INTO  { DOWNSTREAM_ASSET }
    FROM (
    SELECT * FROM '{{{{ task_instance.xcom_pull(task_ids="cleaned__edw_cma_city_lkp.sensor_wait_for_csv", key="csv") }}}}'
    )
    """
):
    pass


