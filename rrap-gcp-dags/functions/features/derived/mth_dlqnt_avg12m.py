import pyarrow as pa
from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

UPSTREAM_ASSET = [ 'features.MTH_DLQNT_CNT', 'ingestion.BASEL_CUST_ACCT_RLTNP_SNAPSHOT', 
                   'ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT' ]
DOWNSTREAM_ASSET = 'features.MTH_DLQNT_AVG12M'
DEPENDENCIES = {
    'duckdb_clear_mth_dlqnt_avg12m': ['duckdb_derive_mth_dlqnt_avg12m'],
}


def duckdb_clear_mth_dlqnt_avg12m(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
        DELETE FROM { DOWNSTREAM_ASSET } 
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass


def duckdb_derive_mth_dlqnt_avg12m(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    INSERT INTO { DOWNSTREAM_ASSET }
    BY NAME FROM (
            WITH
				S2 AS (
                    SELECT
                        SS.BASEL_ACCT_ID,
                        SUM(SS.MTH_DLQNT_CNT) AS LAST12MDlqAvMtCnt
                    FROM {UPSTREAM_ASSET[0]} SS
                    WHERE OBSN_DT BETWEEN
						DATE_ADD(DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}', -INTERVAL 11 MONTH) 
						AND
						'{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
                    GROUP BY SS.BASEL_ACCT_ID
                )
                , S12M AS (
                    SELECT
                        BASEL_CUST_ID as BASEL_CUST_ID
                        , COUNT(1) AS BASEL_CUSTACCT_Age_12mths
                    FROM (
                        SELECT DISTINCT
                            A.MTH_TM_ID
                            , A.BASEL_CUST_ID
                        FROM {UPSTREAM_ASSET[1]} A
                        WHERE A.MTH_TM_ID >= {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}-440
                        AND A.MTH_TM_ID <= {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
                    ) A
                    GROUP BY BASEL_CUST_ID
                )
                SELECT
                    '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
					SS.BASEL_ACCT_ID,
                    ROUND(
                        CASE
                            WHEN COALESCE(S12M.BASEL_CUSTACCT_Age_12mths, 0) <> 0 THEN (S2.LAST12MDlqAvMtCnt / S12M.BASEL_CUSTACCT_Age_12mths)
                            ELSE null
                        END, 4) AS MTH_DLQNT_AVG12M
                FROM {UPSTREAM_ASSET[2]} SS
                LEFT OUTER JOIN S2 ON
                    SS.BASEL_ACCT_ID = S2.BASEL_ACCT_ID
                LEFT OUTER JOIN S12M ON
                    SS.PRIM_BASEL_CUST_ID = S12M.BASEL_CUST_ID
				WHERE SS.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    )
    """
):
    pass

