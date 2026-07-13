import pyarrow as pa
from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

UPSTREAM_ASSET = [
    'ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT',
    'ingestion.BASEL_ACCT_PRFM_FACT',
]

DOWNSTREAM_ASSET = 'features.CC_TRANSACTOR_F'
DEPENDENCIES = {
    'duckdb_clear_cc_transactor_f': ['duckdb_derive_cc_transactor_f']
}

def duckdb_clear_cc_transactor_f(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
        DELETE FROM { DOWNSTREAM_ASSET } 
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass

def duckdb_derive_cc_transactor_f(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} by name
    FROM (
        WITH fact AS (
            SELECT TRIM(TRNSCTR_IND) AS TRNSCTR_IND, 
                LPAD(TRIM(ACCT_NUM), 23, '0') AS ACCT_NUM,
                BASEL_ACCT_ID
            FROM ingestion.BASEL_ACCT_PRFM_FACT
            WHERE MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}} 
                AND TRIM(SRC_SYS_CD) = 'KQ'
        )
        SELECT 
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            snap.BASEL_ACCT_ID,
            CASE
                WHEN GREATEST(0, snap.BNS_DLQNT_DAY - 30) > 0 THEN 'D'
                WHEN TRIM(fact.TRNSCTR_IND) = 'T' THEN 'T'
                WHEN TRIM(fact.TRNSCTR_IND) = 'N' THEN 'R'
                ELSE ''
            END AS CC_TRANSACTOR_F
        FROM ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT snap
        LEFT JOIN fact
        ON TRIM(snap.ACCT_NUM) = fact.ACCT_NUM
        WHERE snap.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    )
    """
):
    pass
