from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)


UPSTREAM_ASSET = [
    "ingestion.CR_BUREAU_DELI_MTH_SNAPSHOT",
]
DOWNSTREAM_ASSET = "features.TOT_BAL_HCCL"
DEPENDENCIES = {
    "duckdb_delete": ["duckdb_load"],
}


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
    INSERT INTO {DOWNSTREAM_ASSET} by name
    FROM (
            SELECT
                basel_cust_id,
                '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS obsn_dt,
                CASE
                    WHEN (
                        COALESCE(TOT_HCCL_REVLVNG_AMT, 0) = 0
                        OR COALESCE(TOT_BAL_REVLVNG_AMT, 0) = 0
                    ) THEN 0
                    ELSE (
                        COALESCE(TOT_BAL_REVLVNG_AMT, 0) / TOT_HCCL_REVLVNG_AMT
                    )::decimal(20,12)
                END AS TOT_BAL_HCCL
            FROM
                ingestion.CR_BUREAU_DELI_MTH_SNAPSHOT
            WHERE
                mth_tm_id = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
                AND COALESCE(basel_cust_id, 0) > 0
    )
    """,
):
    pass


