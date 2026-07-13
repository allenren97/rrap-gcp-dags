from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)


UPSTREAM_ASSET = [
    "ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT",
    "reference.SRC_PRD_LKP",
]
DOWNSTREAM_ASSET = "features.LTV_TP_CD"
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
            TRIM(LTV_TP_CD) AS LTV_TP_CD
        FROM
            ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT A
            LEFT JOIN (
                SELECT
                    *
                FROM
                    reference.SRC_PRD_LKP
                WHERE
                    EFF_FROM_YR_MTH <= strftime ('%Y%m', DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}')
                    AND strftime ('%Y%m', DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}') <= EFF_TO_YR_MTH
            ) SP ON TRIM(A.PRD_CD) = TRIM(SP.SRC_PRD_CD)
            AND TRIM(A.SUB_PRD_CD) = TRIM(SP.SRC_SUB_PRD_CD)
        WHERE
            A.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    )
    """,
):
    pass


