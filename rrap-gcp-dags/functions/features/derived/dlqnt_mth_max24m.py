import pyarrow as pa
from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

UPSTREAM_ASSET = [ 'features.DLQNT_MTH', 'ingestion.MORT_MTH_SNAPSHOT' ]
DOWNSTREAM_ASSET = 'features.DLQNT_MTH_MAX24M'
DEPENDENCIES = {
    'duckdb_clear_dlqnt_mth_max24m': ['duckdb_derive_dlqnt_mth_max24m'],
}


def duckdb_clear_dlqnt_mth_max24m(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
        DELETE FROM { DOWNSTREAM_ASSET } 
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass


def duckdb_derive_dlqnt_mth_max24m(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    INSERT INTO { DOWNSTREAM_ASSET }
    BY NAME FROM (
        SELECT 
            basel_acct_id,
            max(dlqnt_mth) as dlqnt_mth_max24m, 
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' as obsn_dt 
            from { UPSTREAM_ASSET[0] }
            WHERE OBSN_DT BETWEEN
                date_add(date '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}', -INTERVAL 23 MONTH) AND 
                '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
            GROUP BY basel_acct_id
    )
    """
):
    pass

