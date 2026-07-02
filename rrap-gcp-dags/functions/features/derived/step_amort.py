import pyarrow as pa
from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

UPSTREAM_ASSET = [ 'ingestion.MORT_MTH_SNAPSHOT', 'features.COMM_TP_CD' ]
DOWNSTREAM_ASSET = 'features.STEP_AMORT'
DEPENDENCIES = {
    'duckdb_clear_step_amort': ['duckdb_derive_step_amort'],
}


def duckdb_clear_step_amort(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
        DELETE FROM { DOWNSTREAM_ASSET } 
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass


def duckdb_derive_step_amort(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    INSERT INTO { DOWNSTREAM_ASSET }
    BY NAME FROM (
        SELECT
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' as OBSN_DT,
            a.STEP_PLN_AGRMNT_NUM,
            MAX(a.AMORT_MTH) AS STEP_AMORT
        FROM { UPSTREAM_ASSET[0] } a
        INNER JOIN { UPSTREAM_ASSET[1] } b
        ON a.BASEL_ACCT_ID = b.BASEL_ACCT_ID
        WHERE a.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
        AND b.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
        AND a.STEP_PLN_AGRMNT_NUM IS NOT NULL
        AND UPPER(TRIM(b.COMM_TP_CD)) = 'RESIDENTIAL'
        AND a.CRNT_BAL_AMT > 0
        AND TRIM(a.PD_OFF_F) = 'N'
        GROUP BY a.STEP_PLN_AGRMNT_NUM
    )
    """
):
    pass

