import pyarrow as pa
from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

UPSTREAM_ASSET = ['ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT', 'features.BASEL_PRD_CD', 'features.SML_BUS_F', ]
DOWNSTREAM_ASSET = 'features.PRD_TYPE'
DEPENDENCIES = {
    'duckdb_clear_prd_type': ['duckdb_load_prd_type'],
}


def duckdb_clear_prd_type(
        duckdb_conn_id = "duckdb-conn",
        sql=rf"""
        DELETE FROM {DOWNSTREAM_ASSET} WHERE 
            OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
        """
):
    pass

def duckdb_load_prd_type(
    duckdb_conn_id = "duckdb-conn",
    sql=rf"""
    INSERT INTO {DOWNSTREAM_ASSET} BY NAME 
    FROM (
        SELECT
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
                a.BASEL_ACCT_ID,
                CASE
                WHEN TRIM(a.PRD_CD) = 'VIC' THEN 'CC'
                ELSE b.BASEL_PRD_CD
            END AS PRD_TYPE
        FROM {UPSTREAM_ASSET[0]} a
        LEFT JOIN {UPSTREAM_ASSET[1]} b ON
            a.BASEL_ACCT_ID = b.BASEL_ACCT_ID
        LEFT JOIN {UPSTREAM_ASSET[2]} c ON
            b.BASEL_ACCT_ID = c.BASEL_ACCT_ID
            AND b.OBSN_DT = c.OBSN_DT
        WHERE 
            MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
            AND b.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
            AND c.SML_BUS_F = 'N'
    )
    """
):
    pass

