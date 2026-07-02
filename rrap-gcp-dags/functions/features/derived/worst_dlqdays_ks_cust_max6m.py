from airflow.sdk import get_current_context
from bns.rrap.helpers.asset_event import _pull_asset_event_extras, _push_asset_event_extras

UPSTREAM_ASSET = [ 'features.WORST_DLQDAYS_KS_CUST' ]
DOWNSTREAM_ASSET = 'features.WORST_DLQDAYS_KS_CUST_MAX6M'

DEPENDENCIES = {
    'duckdb_clear_worst_dlqdays_ks_cust_max6m': ['duckdb_derive_worst_dlqdays_ks_cust_max6m'],
}


def duckdb_clear_worst_dlqdays_ks_cust_max6m(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
        DELETE FROM { DOWNSTREAM_ASSET }
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass

def duckdb_derive_worst_dlqdays_ks_cust_max6m(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} BY NAME
    WITH base AS (
        SELECT
            BASEL_CUST_ID,
            MAX(WORST_DLQDAYS_KS_CUST) AS WORST_DLQDAYS_KS_CUST_MAX6M
        FROM
            {UPSTREAM_ASSET[0]}
        WHERE
            OBSN_DT BETWEEN
                DATE_ADD(DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}', - INTERVAL 5 MONTH)
                AND
                '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
        GROUP BY
            BASEL_CUST_ID
    )
    SELECT
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        BASEL_CUST_ID,
        WORST_DLQDAYS_KS_CUST_MAX6M
    FROM base
    """
):
    pass

