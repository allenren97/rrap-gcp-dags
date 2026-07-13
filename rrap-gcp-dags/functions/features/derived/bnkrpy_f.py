from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)


UPSTREAM_ASSET = [
    "ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT",
    "reference.BLOCK_RECL_LKP",
]
DOWNSTREAM_ASSET = "features.BNKRPY_F"
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
            trim(BNKRPY_F) as BNKRPY_F
        FROM
            ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT a
            LEFT JOIN (
                SELECT
                    BNKRPY_F,
                    BLOCK_RECL_CD,
                    CONSM_SCORECRD_EXCLSN_F
                FROM
                    reference.BLOCK_RECL_LKP
                WHERE
                    strftime ('%Y%m', DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}') BETWEEN EFF_FROM_YR_MTH AND EFF_TO_YR_MTH
            ) bf ON a.BLOCK_RECL_CD = bf.BLOCK_RECL_CD
        WHERE
            MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    )
    """,
):
    pass


