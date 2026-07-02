from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)


UPSTREAM_ASSET = [
    "ingestion.MORT_MTH_SNAPSHOT ",
    "ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT",
    "ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT"
]
DOWNSTREAM_ASSET = "features.WRITTEN_OUT_F"
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
        'N' AS WRITTEN_OUT_F,
        BASEL_ACCT_ID
    FROM
        {UPSTREAM_ASSET[0]}
    WHERE
        MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    UNION ALL
    SELECT
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        CASE WHEN TRIM(RECD_STAT_CD) = '8' THEN 'Y' ELSE 'N' END AS WRITTEN_OUT_F,
        BASEL_ACCT_ID
    FROM
        {UPSTREAM_ASSET[2]}
    WHERE
        MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    UNION ALL
    SELECT
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        CASE WHEN TRIM(CHRG_OFF_CD) = '1' AND TRIM(BLOCK_RECL_CD) LIKE 'D%'
	    THEN 'Y' ELSE 'N' END AS WRITTEN_OUT_F,
        BASEL_ACCT_ID
    FROM
        {UPSTREAM_ASSET[1]}
    WHERE
        MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    )
    """,
):
    pass


