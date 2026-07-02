import os
from datetime import timedelta
import pendulum
from airflow.sdk import get_current_context
from airflow.sdk.bases.sensor import PokeReturnValue
from bns.rrap.helpers.asset_event import _pull_asset_event_extras, _push_asset_event_extras

DUCKLAKE_SCHEMA = "ingestion"
UPSTREAM_ASSET = [ f"{DUCKLAKE_SCHEMA}.TM_DIM" ]
DOWNSTREAM_ASSET = f"{DUCKLAKE_SCHEMA}.SPL_SOURCE_FILE_ACCOUNTS"
DEPENDENCIES = {
    'sensor_wait_for_table': ['duckdb_delete'],
    'duckdb_delete' : ['duckdb_load'],
}


def sensor_wait_for_table(poke_interval=300, timeout=(60 * 60 * 24 * 8), mode='reschedule'):
    context = get_current_context()
    rundate, mth_tm_id = _pull_asset_event_extras(context, UPSTREAM_ASSET[0])
    year_month = pendulum.from_format(rundate, 'YYYY-MM-DD').strftime('%Y%m')
    path = os.path.join("/bns/rrap/data/securitization/cc/incoming", f"autoloan_securitization_acct_mthly_{year_month}.csv")

    if os.path.exists(path):
        context['ti'].xcom_push(key="parquet", value=path)
        context['ti'].xcom_push(key="rundate", value=rundate)
        context['ti'].xcom_push(key='mth_tm_id', value=mth_tm_id)
        return PokeReturnValue(is_done=True)
    else:
        return PokeReturnValue(is_done=False)


def duckdb_delete(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    DELETE FROM {DOWNSTREAM_ASSET}
    WHERE EFFECTIVE_DATE = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass


def duckdb_load(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} BY NAME
    FROM (
        SELECT
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' EFFECTIVE_DATE,
            account_number,
            outstanding_balance,
            accured_interest ACCRUED_INTEREST,
            Securitization_Date
        FROM read_csv(
            '{{{{ task_instance.xcom_pull(task_ids="raw__spl_source_file_accounts.sensor_wait_for_table", key="parquet") }}}}',
            delim = ',',
            header = true,
            columns = {{
                'account_number': 'VARCHAR',
                'outstanding_balance': 'DECIMAL(17,2)',
                'accured_interest': 'DECIMAL(17,10)',
                'Securitization_Date': 'VARCHAR'
            }}
        )
    )
    """
):
    pass
