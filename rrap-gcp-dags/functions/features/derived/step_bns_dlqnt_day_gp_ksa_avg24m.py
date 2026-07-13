from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)


UPSTREAM_ASSET = [
    "features.STEP_BNS_DLQNT_DAY_GP_KSA",
]
DOWNSTREAM_ASSET = "features.STEP_BNS_DLQNT_DAY_GP_KSA_AVG24M"
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
    INSERT INTO {DOWNSTREAM_ASSET} by name (
        SELECT
            STEP_PLN_AGRMNT_NUM,
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            avg(STEP_BNS_DLQNT_DAY_GP_KSA) AS STEP_BNS_DLQNT_DAY_GP_KSA_AVG24M
        FROM
            {UPSTREAM_ASSET[0]}
        WHERE
            OBSN_DT BETWEEN
                DATE_ADD(DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}', - INTERVAL 23 MONTH)
                AND '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
        GROUP BY
            STEP_PLN_AGRMNT_NUM
    )
    """,
):
    pass


