import os
from datetime import timedelta
import pendulum
import duckdb

from airflow.sdk import get_current_context
from airflow.sdk.bases.sensor import PokeReturnValue

from bns.rrap.helpers.asset_event import _push_asset_event_extras
from bns.rrap.hooks.duckdb import DuckLakeHook


UPSTREAM_ASSET = None
DOWNSTREAM_ASSET = "ingestion.TM_DIM"
DEPENDENCIES = {
}


def sensor_wait_for_table(poke_interval=300, timeout=(60 * 60 * 24 * 8), mode='reschedule'):
    context = get_current_context()
    rundate = context['logical_date'].subtract(months=1).end_of('month').strftime('%Y-%m-%d')
    path = os.path.join("/bns/rrap/data", f"{ rundate }", 'TM_DIM.parquet')

    if os.path.exists(path):
        context['ti'].xcom_push(key='parquet', value=path)
        return PokeReturnValue(is_done=True)
    else:
        return PokeReturnValue(is_done=False)

