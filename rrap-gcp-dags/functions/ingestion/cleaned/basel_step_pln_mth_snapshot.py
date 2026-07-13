import os

from airflow.sdk import get_current_context, Param
from airflow.sdk.bases.sensor import PokeReturnValue

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

DAG_PARAMS = {
    "mth_tm_id": Param("", type=["null", "string"], description="MTH_TM_ID"),
    "rundate": Param("", type=["null", "string"], description="RUNDATE"),
}

UPSTREAM_ASSET = ["ingestion.TM_DIM"]
DOWNSTREAM_ASSET = "ingestion.BASEL_STEP_PLN_MTH_SNAPSHOT"
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
        "jb0052_join_3_BR_LOCTN_OU_ID.parquet",
    )

    if os.path.exists(path):
        context["ti"].xcom_push(key="parquet", value=path)
        return PokeReturnValue(is_done=True)
    else:
        return PokeReturnValue(is_done=False)


def duckdb_delete(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    delete from {DOWNSTREAM_ASSET} where MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    """,
):
    pass


def duckdb_load(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} by name
    FROM (
        SELECT
            * exclude (MTH_END_DT, ALI_ACCT_NUM),
            CASE
                WHEN TRIM(ALI_ACCT_NUM) = '' THEN NULL
                ELSE TRIM(ALI_ACCT_NUM)::DECIMAL(18, 0)
            END AS ALI_ACCT_NUM,
            CURRENT_TIMESTAMP AS INSRT_PROCESS_TMSTMP,
            CURRENT_TIMESTAMP AS UPDT_PROCESS_TMSTMP
        FROM read_parquet('{{{{ task_instance.xcom_pull(task_ids="cleaned__basel_step_pln_mth_snapshot.sensor_wait_for_table", key="parquet") }}}}')
    )
    """,
):
    pass


