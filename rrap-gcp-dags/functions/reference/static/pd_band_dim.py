import os
from datetime import timedelta
import pendulum
from airflow.sdk import get_current_context
from airflow.sdk.bases.sensor import PokeReturnValue
from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

UPSTREAM_ASSET = ["ingestion.TM_DIM"]
DOWNSTREAM_ASSET = "reference.PD_BAND_DIM"
DEPENDENCIES = {
    "sensor_wait_for_table": ["duckdb_delete"],
    "duckdb_delete": ["duckdb_load"],
    "duckdb_load": ["outlet_pd_band_dim"],
}
INPUT_PATH = "PD_BAND_DIM.parquet"


def sensor_wait_for_table(
    poke_interval=300, timeout=(60 * 60 * 24 * 8), mode="reschedule"
):
    context = get_current_context()
    rundate, mth_tm_id = _pull_asset_event_extras(context, UPSTREAM_ASSET[0])
    path = os.path.join("/bns/rrap/data", f"{rundate}", INPUT_PATH)

    if os.path.exists(path):
        context["ti"].xcom_push(key="parquet", value=path)
        context["ti"].xcom_push(key="rundate", value=rundate)
        context["ti"].xcom_push(key="mth_tm_id", value=mth_tm_id)
        return PokeReturnValue(is_done=True)
    else:
        return PokeReturnValue(is_done=False)


def duckdb_delete(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    TRUNCATE TABLE {DOWNSTREAM_ASSET}
    """,
):
    pass


def duckdb_load(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} BY NAME
    FROM (
        SELECT * FROM read_parquet('{{{{ task_instance.xcom_pull(task_ids="sensor_wait_for_table", key="parquet") }}}}')
    )
    """,
):
    pass


def outlet_pd_band_dim():
    context = get_current_context()
    _push_asset_event_extras(context, "sensor_wait_for_table", "sensor_wait_for_table")
