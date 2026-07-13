from airflow.sdk import get_current_context, Param

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

DAG_PARAMS = {
    "mth_tm_id": Param("", type=["null", "string"], description="MTH_TM_ID"),
    "rundate": Param("", type=["null", "string"], description="RUNDATE"),
}

UPSTREAM_ASSET = [
    "ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT",
    "features.PIT_STATUS_CROSS_DEFAULT_ORIG",
]
DOWNSTREAM_ASSET = "features.INACT_12M"
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
         WITH
            rvl AS (
                SELECT
                    MAX(TOT_NEW_BAL_AMT) AS MAX_TOT_NEW_BAL_AMT,
                    MIN(TOT_NEW_BAL_AMT) AS MIN_TOT_NEW_BAL_AMT,
                    COUNT(TOT_NEW_BAL_AMT) AS CNT_TOT_NEW_BAL_AMT,
                    BASEL_ACCT_ID AS BASEL_ACCT_ID
                FROM
                    ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT
                WHERE
                    mth_tm_id > {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}} - 480
                    AND mth_tm_id <= {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
                GROUP BY
                    BASEL_ACCT_ID
                ORDER BY
                    BASEL_ACCT_ID
            )
        SELECT
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS obsn_dt,
            rvl.BASEL_ACCT_ID,
            CASE
                WHEN TRIM(pit.PIT_STATUS_CROSS_DEFAULT_ORIG) = 'CUR'
                AND (
                    rvl.MAX_TOT_NEW_BAL_AMT - rvl.MIN_TOT_NEW_BAL_AMT = 0
                    AND rvl.CNT_TOT_NEW_BAL_AMT = 12
                ) THEN 'Y'
                ELSE 'N'
            END AS INACT_12M
        FROM
            rvl
            LEFT JOIN features.PIT_STATUS_CROSS_DEFAULT_ORIG AS pit ON pit.BASEL_ACCT_ID = rvl.BASEL_ACCT_ID and pit.obsn_dt = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    )
    """,
):
    pass


