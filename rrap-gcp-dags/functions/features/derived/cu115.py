import pyarrow as pa
from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

UPSTREAM_ASSET = [ 'ingestion.CR_BUREAU_DELI_MTH_SNAPSHOT' ]
DOWNSTREAM_ASSET = 'features.CU115'
DEPENDENCIES = {
    'duckdb_clear_cu115': ['duckdb_derive_cu115'],
}


def duckdb_clear_cu115(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
        DELETE FROM { DOWNSTREAM_ASSET } 
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass


def duckdb_derive_cu115(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
                INSERT INTO { DOWNSTREAM_ASSET }
                BY NAME FROM (
                    SELECT
                        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT
                        , A.BASEL_CUST_ID
                        , A.TOT_COMPTV_RTO AS CU115
                    FROM {UPSTREAM_ASSET[0]} A
                    WHERE
                        A.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
                        AND
                        A.BASEL_CUST_ID > 0
    )
    """
):
    pass

