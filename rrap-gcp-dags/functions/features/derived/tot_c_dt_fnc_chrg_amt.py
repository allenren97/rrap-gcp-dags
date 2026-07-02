import pyarrow as pa
from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

UPSTREAM_ASSET = ['ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT',]
DOWNSTREAM_ASSET = 'features.TOT_C_DT_FNC_CHRG_AMT'
DEPENDENCIES = {
    'duckdb_clear_tot_c_dt_fnc_chrg_amt': ['duckdb_derive_tot_c_dt_fnc_chrg_amt'],
}


def duckdb_clear_tot_c_dt_fnc_chrg_amt(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
        DELETE FROM { DOWNSTREAM_ASSET } 
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass

def duckdb_derive_tot_c_dt_fnc_chrg_amt(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    INSERT INTO { DOWNSTREAM_ASSET }
    BY NAME FROM (
        SELECT
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            BASEL_ACCT_ID,
            SUM(TOT_CYCL_TO_DT_FNCL_CHRG_AMT)::DECIMAL(20,4) AS TOT_C_DT_FNC_CHRG_AMT
        FROM 
            {UPSTREAM_ASSET[0]}
        WHERE
            MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id")  }}}}
        GROUP BY
            BASEL_ACCT_ID,
            OBSN_DT
    )
    """
):
    pass

