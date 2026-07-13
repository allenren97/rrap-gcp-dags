from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)


UPSTREAM_ASSET = ["features.BNS_DLQNT_DAY_GP_KSA" ]
DOWNSTREAM_ASSET = "features.BNS_DLQNT_DAY_GP_KSA_MAX3M"
DEPENDENCIES = {
    "duckdb_delete": ["duckdb_load"],
}


def duckdb_delete(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        DELETE FROM {DOWNSTREAM_ASSET}
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """,
):
    pass


def duckdb_load(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        INSERT INTO {DOWNSTREAM_ASSET} BY NAME
        FROM (
            SELECT
                '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
                BASEL_ACCT_ID,
                MAX(BNS_DLQNT_DAY_GP_KSA) AS BNS_DLQNT_DAY_GP_KSA_MAX3M
            FROM
                features.BNS_DLQNT_DAY_GP_KSA
            WHERE
                OBSN_DT BETWEEN DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' - INTERVAL 2 MONTH
                AND DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
            GROUP BY BASEL_ACCT_ID
        )
    """,
):
    pass


