
from airflow.sdk import get_current_context
from bns.rrap.helpers.asset_event import _pull_asset_event_extras, _push_asset_event_extras

UPSTREAM_ASSET = [
    'ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT'
]
DOWNSTREAM_ASSET = 'features.WORST_DLQDAYS_KS_CUST'

DEPENDENCIES = {
    'duckdb_clear_worst_dlqdays_ks_cust': ['duckdb_load_worst_dlqdays_ks_cust'],
}


def duckdb_clear_worst_dlqdays_ks_cust(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
        DELETE FROM {DOWNSTREAM_ASSET}
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass

def duckdb_load_worst_dlqdays_ks_cust(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} BY NAME
    FROM (
        SELECT
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            PRIM_BASEL_CUST_ID AS BASEL_CUST_ID,
            CASE WHEN (BNS_DLQNT_DAY - 30) < 0 THEN 0 ELSE (BNS_DLQNT_DAY - 30) END AS WORST_DLQDAYS_KS_CUST
        FROM ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT
        WHERE MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    )
    """
):
    pass

