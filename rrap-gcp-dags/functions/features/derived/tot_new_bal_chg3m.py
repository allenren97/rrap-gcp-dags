import pyarrow as pa
from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

UPSTREAM_ASSET = [ 'ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT' ]
DOWNSTREAM_ASSET = 'features.TOT_NEW_BAL_CHG3M'
DEPENDENCIES = {
    'duckdb_clear_tot_new_bal_chg3m': ['duckdb_derive_tot_new_bal_chg3m'],
}


def duckdb_clear_tot_new_bal_chg3m(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
        DELETE FROM { DOWNSTREAM_ASSET } 
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass


def duckdb_derive_tot_new_bal_chg3m(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    INSERT INTO { DOWNSTREAM_ASSET }
    BY NAME FROM (
                WITH S4 AS (
                    SELECT
                        SS.BASEL_ACCT_ID
                        , SS.TOT_NEW_BAL_AMT as TOT_NEW_BAL_AMT
                    FROM { UPSTREAM_ASSET[0] } SS
                    WHERE
                        SS.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}-80
                )
                SELECT
                    '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT
                    , SS.BASEL_ACCT_ID
                    , CAST(ROUND(
                        CASE
                            WHEN COALESCE(SS.TOT_NEW_BAL_AMT-S4.TOT_NEW_BAL_AMT,0) = 0 OR COALESCE(S4.TOT_NEW_BAL_AMT,0) = 0 THEN 0
                            ELSE (SS.TOT_NEW_BAL_AMT - S4.TOT_NEW_BAL_AMT) / S4.TOT_NEW_BAL_AMT
                        END, 3) AS DECIMAL(17,3)) AS TOT_NEW_BAL_CHG3M
                FROM { UPSTREAM_ASSET[0] } SS
                LEFT OUTER JOIN S4 ON
                    SS.BASEL_ACCT_ID = S4.BASEL_ACCT_ID
                WHERE
                    SS.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    )
    """
):
    pass

