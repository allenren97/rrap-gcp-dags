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
    "features.PRD_ID",
]
DOWNSTREAM_ASSET = "features.SUB_PORT_F"
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
            BASEL_ACCT_ID,
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            CASE
                WHEN TRIM(prd_id) = 'S01'
                OR TRIM(prd_id) = 'S02'
                OR TRIM(prd_id) = 'S03'
                OR TRIM(prd_id) = 'S04'
                OR TRIM(prd_id) = 'S05'
                OR TRIM(prd_id) = 'S06'
                OR TRIM(prd_id) = 'S07'
                OR TRIM(prd_id) = 'S08' THEN 'DIRECT'
                WHEN TRIM(prd_id) = 'S09'
                OR TRIM(prd_id) = 'S10'
                OR TRIM(prd_id) = 'S11'
                OR TRIM(prd_id) = 'S12'
                OR TRIM(prd_id) = 'S13'
                OR TRIM(prd_id) = 'S14'
                OR TRIM(prd_id) = 'S15' THEN 'INDIRECT'
            END AS SUB_PORT_F,
            'SPL' as SRC_SYS_CD
        FROM
            features.PRD_ID
        WHERE
            obsn_dt = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    )
    """,
):
    pass


