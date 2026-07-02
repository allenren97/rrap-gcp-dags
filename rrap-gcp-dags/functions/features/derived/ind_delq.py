import pyarrow as pa
from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras, 
    _push_asset_event_extras)


UPSTREAM_ASSET = ['ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT']

DOWNSTREAM_ASSET = "features.IND_DELQ"
DEPENDENCIES = {
    'duckdb_clear_ind_delq': ['duckdb_derive_ind_delq'],
}


def duckdb_clear_ind_delq(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
        DELETE FROM { DOWNSTREAM_ASSET } 
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass

def duckdb_derive_ind_delq(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} BY NAME
    FROM(
        SELECT
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            BASEL_ACCT_ID,
            CASE
                WHEN DAY_ODUE > 0
                THEN 1
                ELSE 0
            END AS IND_DELQ
            FROM {UPSTREAM_ASSET[0]}
            WHERE 
                RECD_STAT_CD IN (4,5,6,7,8) AND
                MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id")  }}}} 
    )
    """
):
    pass

