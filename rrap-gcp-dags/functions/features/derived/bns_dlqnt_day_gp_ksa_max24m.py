from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)


UPSTREAM_ASSET = [
    "features.BNS_DAY_DLQNT",
]
DOWNSTREAM_ASSET = "features.BNS_DLQNT_DAY_GP_KSA_MAX24M"
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
    WITH base AS (
        SELECT
            BASEL_ACCT_ID,
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            MAX(
                CASE WHEN BNS_DAY_DLQNT -30 < 0 THEN 0
                ELSE BNS_DAY_DLQNT -30 END
            ) AS BNS_DLQNT_DAY_GP_KSA_MAX24M
        FROM
            {UPSTREAM_ASSET[0]}
        WHERE
            OBSN_DT BETWEEN
                DATE_ADD(DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}', - INTERVAL 23 MONTH)
                AND
                '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
        GROUP BY
            BASEL_ACCT_ID
    )
    SELECT
    BASEL_ACCT_ID,
    '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
    BNS_DLQNT_DAY_GP_KSA_MAX24M
    from base
    """,
):
    pass


