from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

UPSTREAM_ASSET = ["ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT", ]
DOWNSTREAM_ASSET = "features.UTIL_KSA"
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
                CASE 
                    WHEN CR_LMT_AMT = 0 AND TOT_NEW_BAL_AMT > 0 THEN 1
                    WHEN TOT_NEW_BAL_AMT < 0 THEN 0
                    WHEN CR_LMT_AMT <> 0 THEN CAST(TOT_NEW_BAL_AMT * 1.0 / CR_LMT_AMT AS DECIMAL(16, 9))
                    ELSE NULL
                END AS UTIL_KSA
            FROM
                {UPSTREAM_ASSET[0]} RSN
            WHERE
                RSN.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
        )
    """,
):
    pass


