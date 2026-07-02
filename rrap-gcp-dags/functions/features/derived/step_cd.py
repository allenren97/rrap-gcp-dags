from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)


UPSTREAM_ASSET = [
    "ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT",
]
DOWNSTREAM_ASSET = "features.STEP_CD"
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
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            BASEL_ACCT_ID,
            CASE
                WHEN COALESCE(STEP_PLN_AGRMNT_NUM, '') != '' THEN 'Y'
                WHEN COALESCE(STEP_PLN_AGRMNT_NUM, '') = '' THEN CASE
                    WHEN TRIM(PRD_CD) IN ('SCL', 'VIC') THEN CASE
                        WHEN TRIM(SCRD_TP_CD) = 'U' THEN 'U'
                        WHEN SUBSTR (TRIM(CRNT_BILL_CD), 1, 1) = 'U' THEN 'U'
                        WHEN SUBSTR (TRIM(CRNT_BILL_CD), 1, 2) IN ('11', 'SB', 'SN', 'SP', 'SR', 'ST') THEN 'R'
                        WHEN TRIM(SCRD_TP_CD) = 'S' THEN 'O'
                        ELSE 'O'
                    END
                    ELSE 'N'
                END
            END AS STEP_CD
        FROM
            (
                SELECT
                    MTH_TM_ID,
                    BASEL_ACCT_ID,
                    PRIM_BASEL_CUST_ID AS BASEL_CUST_ID,
                    TRIM(STEP_PLN_AGRMNT_NUM) AS STEP_PLN_AGRMNT_NUM,
                    PRD_CD,
                    CRNT_BILL_CD,
                    SCRD_TP_CD
                FROM
                    ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT
                WHERE
                    MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
            )
    )
    """,
):
    pass


