import pyarrow as pa
from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

UPSTREAM_ASSET = ['ingestion.BASEL_ACCT_PRFM_FACT',
                  'features.BASEL_ACCT_ID']

DOWNSTREAM_ASSET = 'features.CLP_F_B20'
DEPENDENCIES = {
    'duckdb_clear_clp_f_b20': ['duckdb_derive_clp_f_b20']
}

def duckdb_clear_clp_f_b20(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
        DELETE FROM { DOWNSTREAM_ASSET } 
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass

def duckdb_derive_clp_f_b20(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    INSERT INTO { DOWNSTREAM_ASSET } BY NAME 
    FROM (
        SELECT
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            acct.BASEL_ACCT_ID,
            acct.SRC_SYS_CD,
            fact.CLP_FLAG AS CLP_F_B20
        FROM {UPSTREAM_ASSET[1]} acct
        LEFT JOIN {UPSTREAM_ASSET[0]} fact ON
            acct.BASEL_ACCT_ID = fact.BASEL_ACCT_ID
            AND fact.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
        WHERE 
            acct.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    )
    """
):
    pass