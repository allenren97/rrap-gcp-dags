from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)


UPSTREAM_ASSET = [
    "features.BNS_DLQNT_DAY_GP_KSA",
    "ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT"
]
DOWNSTREAM_ASSET = "features.STEP_BNS_DLQNT_DAY_GP_KSA"
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
            TRIM(STEP_PLN_AGRMNT_NUM) AS STEP_PLN_AGRMNT_NUM,
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS obsn_dt,
            MAX(BNS_DLQNT_DAY_GP_KSA) AS STEP_BNS_DLQNT_DAY_GP_KSA
        FROM (
            SELECT * FROM { UPSTREAM_ASSET[0] } WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' 
        ) a
        INNER JOIN (
            SELECT * FROM { UPSTREAM_ASSET[1] } WHERE MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
        ) b
        ON a.BASEL_ACCT_ID = b.BASEL_ACCT_ID
        WHERE STEP_PLN_AGRMNT_NUM IS NOT NULL AND TRIM(STEP_PLN_AGRMNT_NUM) <> ''
        GROUP BY TRIM(STEP_PLN_AGRMNT_NUM)
    )
    """,
):
    pass


