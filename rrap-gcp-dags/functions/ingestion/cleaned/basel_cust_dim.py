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
DOWNSTREAM_ASSET = "ingestion.BASEL_CUST_DIM"
DEPENDENCIES = {
    "sensor_wait_for_table": ["duckdb_delete"],
    "duckdb_delete": ["duckdb_update"],
    "duckdb_update": ["duckdb_load"],
}


def sensor_wait_for_table(
    poke_interval=1000, timeout=(60 * 60 * 24 * 8), mode="reschedule"
):
    context = get_current_context()
    rundate, mth_tm_id = _pull_asset_event_extras(context, UPSTREAM_ASSET[0])
    new = os.path.join(
        "/bns/rrap/data",
        f"{rundate}",
        "jb0022_XFM_01_new.parquet",
    )

    update = os.path.join(
        "/bns/rrap/data",
        f"{rundate}",
        "jb0022_XFM_01_update.parquet",
    )

    if os.path.exists(new) and os.path.exists(update):
        context["ti"].xcom_push(key="new", value=new)
        context["ti"].xcom_push(key="update", value=update)
        return PokeReturnValue(is_done=True)
    else:
        return PokeReturnValue(is_done=False)


def duckdb_delete(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    delete from {DOWNSTREAM_ASSET} where basel_cust_id in (select basel_cust_id from '{{{{ task_instance.xcom_pull(task_ids="cleaned__basel_cust_dim.sensor_wait_for_table", key="update") }}}}' union select basel_cust_id from '{{{{ task_instance.xcom_pull(task_ids="cleaned__basel_cust_dim.sensor_wait_for_table", key="new") }}}}')
    """,
):
    pass


def duckdb_update(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} by name
    FROM (
        SELECT * exclude (UPDT_PROCESS_TMSTMP), CURRENT_TIMESTAMP as UPDT_PROCESS_TMSTMP FROM read_parquet('{{{{ task_instance.xcom_pull(task_ids="cleaned__basel_cust_dim.sensor_wait_for_table", key="update") }}}}')
    )
    """,
):
    pass


def duckdb_load(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} by name
    FROM (
        SELECT * FROM read_parquet('{{{{ task_instance.xcom_pull(task_ids="cleaned__basel_cust_dim.sensor_wait_for_table", key="new") }}}}')
    )
    """,
):
    pass


