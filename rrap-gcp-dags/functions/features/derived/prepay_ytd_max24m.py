import pyarrow as pa
from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

UPSTREAM_ASSET = [ 'features.PREPAY_YTD', 'ingestion.MORT_MTH_SNAPSHOT' ]
DOWNSTREAM_ASSET = 'features.PREPAY_YTD_MAX24M'
DEPENDENCIES = {
    'duckdb_clear_prepay_ytd_max24m': ['duckdb_derive_prepay_ytd_max24m'],
}


def duckdb_clear_prepay_ytd_max24m(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
        DELETE FROM { DOWNSTREAM_ASSET } 
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass


def duckdb_derive_prepay_ytd_max24m(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    INSERT INTO { DOWNSTREAM_ASSET }
    BY NAME FROM (
        SELECT
            BASEL_ACCT_ID,
            MAX(PREPAY_YTD) AS PREPAY_YTD_MAX24M,
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT 
            from { UPSTREAM_ASSET[0] }
            WHERE OBSN_DT BETWEEN
                DATE_ADD(DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}', -INTERVAL 23 MONTH) AND 
                '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
                AND BASEL_ACCT_ID IN (
                    SELECT DISTINCT BASEL_ACCT_ID
                    from { UPSTREAM_ASSET[1] } 
                    WHERE MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
                )
                GROUP BY BASEL_ACCT_ID
    )
    """
):
    pass

