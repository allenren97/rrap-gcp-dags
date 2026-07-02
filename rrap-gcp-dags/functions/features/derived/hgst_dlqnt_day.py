from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)


UPSTREAM_ASSET = ["ingestion.MORT_MTH_SNAPSHOT",]
DOWNSTREAM_ASSET = "features.HGST_DLQNT_DAY"
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
            DLQNT_DAY AS HGST_DLQNT_DAY,
            MORT_NUM,
            CAST((NULLIF(TRIM(STEP_PLN_AGRMNT_NUM), '')) AS BIGINT) AS STEP_PLN_AGRMNT_NUM
        FROM
            {UPSTREAM_ASSET[0]}
        WHERE UPPER(TRIM(COMM_TP)) = 'RESIDENTIAL'
        AND CRNT_BAL_AMT > 0
        AND TRIM(PD_OFF_F) = 'N'
        AND MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    )
    """,
):
    pass


