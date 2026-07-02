# Refactored Jaesungs code into template
import os
from datetime import timedelta
import pendulum

from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)


UPSTREAM_ASSET = ["ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT"]
DOWNSTREAM_ASSET = "features.HELOC_F"
DEPENDENCIES = {
    "export_ks": ["duckdb_delete"],
    "duckdb_delete": ["duckdb_load"],
}


def export_ks(
    duckdb_conn_id="duckdb-conn",
    sql=r"""
        SELECT
            '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' AS OBSN_DT,
            BASEL_ACCT_ID,
            CASE
                WHEN TRIM(SUB_PRD_CD) = 'RS'
                OR COALESCE(TRIM(STEP_PLN_AGRMNT_NUM), '') != '' THEN 'Y'
                ELSE 'N'
            END AS HELOC_F
        FROM
            ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT
        WHERE
            MTH_TM_ID = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}
    """,
):
    pass


def duckdb_delete(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    delete from {DOWNSTREAM_ASSET} where obsn_dt = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """,
):
    pass


def duckdb_load(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} by name from
    (
        select * from read_parquet(['{{{{ task_instance.xcom_pull(task_ids="derived__heloc_f.export_ks", key="parquet") }}}}'])
    )
    """,
):
    pass


