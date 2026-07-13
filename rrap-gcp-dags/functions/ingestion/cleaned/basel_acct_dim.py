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
DOWNSTREAM_ASSET = "ingestion.BASEL_ACCT_DIM"
DEPENDENCIES = {
    "sensor_wait_for_table": ["duckdb_delete"],
    "duckdb_delete": ["duckdb_load"],
}


def sensor_wait_for_table(
    poke_interval=1000, timeout=(60 * 60 * 24 * 8), mode="reschedule"
):
    context = get_current_context()
    rundate, mth_tm_id = _pull_asset_event_extras(context, UPSTREAM_ASSET[0])
    path = os.path.join(
        "/bns/rrap/data",
        f"{rundate}",
        "jb0032_XFM_01.parquet",
    )

    if os.path.exists(path):
        context["ti"].xcom_push(key="parquet", value=path)
        return PokeReturnValue(is_done=True)
    else:
        return PokeReturnValue(is_done=False)


def duckdb_delete(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    delete from {DOWNSTREAM_ASSET} where BASEL_ACCT_ID in (select BASEL_ACCT_ID from '{{{{ task_instance.xcom_pull(task_ids="cleaned__basel_acct_dim.sensor_wait_for_table", key="parquet") }}}}')
    """,
):
    pass


def duckdb_load(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} by name
    FROM (
        SELECT * FROM read_parquet('{{{{ task_instance.xcom_pull(task_ids="cleaned__basel_acct_dim.sensor_wait_for_table", key="parquet") }}}}')
    )
    """,
):
    pass


