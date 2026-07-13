import pyarrow as pa
from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

UPSTREAM_ASSET = [ 'features.REGINVACCTCNT', 'ingestion.BASEL_CUST_ACCT_RLTNP_SNAPSHOT' ]

DOWNSTREAM_ASSET = 'features.REG_INV_ACCT_CNT_AVG3M'

DEPENDENCIES = {
    'duckdb_clear_reg_inv_acct_cnt_avg3m': ['duckdb_derive_reg_inv_acct_cnt_avg3m'],
}


def duckdb_clear_reg_inv_acct_cnt_avg3m(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
        DELETE FROM { DOWNSTREAM_ASSET } 
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass


def duckdb_derive_reg_inv_acct_cnt_avg3m(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    INSERT INTO { DOWNSTREAM_ASSET }
        BY NAME FROM (
        WITH L25 AS (
            SELECT
                A.BASEL_CUST_ID,
                COUNT(1) AS CNT_BASEL_CUST_ID_25
            FROM (
                SELECT DISTINCT
                    A.MTH_TM_ID,
                    A.BASEL_CUST_ID
                FROM {UPSTREAM_ASSET[1]} A
                WHERE
                    A.MTH_TM_ID >= {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}-80 
                    AND 
                    A.MTH_TM_ID <= {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
            ) A
            GROUP BY
                A.BASEL_CUST_ID
        )
        , L14 AS (
            SELECT
				  WSS.BASEL_CUST_ID AS BASEL_CUST_ID
                , WSS.SUM_RGSTRD_INVSTMNT_BAL_ACCT_CNT AS SUM_RGSTRD_INVSTMNT_BAL_ACCT_CNT
            FROM (
                SELECT
                    A.BASEL_CUST_ID AS BASEL_CUST_ID
                    , SUM(A.REGINVACCTCNT) AS SUM_RGSTRD_INVSTMNT_BAL_ACCT_CNT
                FROM {UPSTREAM_ASSET[0]} A
                INNER JOIN (
                    SELECT DISTINCT
                        A.BASEL_CUST_ID,
                        A.MTH_TM_ID
                    FROM {UPSTREAM_ASSET[1]} A
                    WHERE
                        (A.MTH_TM_ID >= {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}-80 
                        AND 
                        A.MTH_TM_ID <= {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}})
                ) B ON
                    A.BASEL_CUST_ID = B.BASEL_CUST_ID
                    AND 
                    (A.MTH_TM_ID >= {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}-80 
                    AND 
                    A.MTH_TM_ID <= {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}})
                    AND 
                    A.MTH_TM_ID = B.MTH_TM_ID
                GROUP BY A.BASEL_CUST_ID
            ) WSS  
        )
           		SELECT
                    '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
					L14.BASEL_CUST_ID AS BASEL_CUST_ID,
                    CAST(CASE
                        WHEN L14.SUM_RGSTRD_INVSTMNT_BAL_ACCT_CNT IS NULL OR L25.CNT_BASEL_CUST_ID_25 IS NULL OR L25.CNT_BASEL_CUST_ID_25 = 0 THEN NULL
                        ELSE ROUND(L14.SUM_RGSTRD_INVSTMNT_BAL_ACCT_CNT / L25.CNT_BASEL_CUST_ID_25, 4)
                    END AS DECIMAL(17,3)) AS REG_INV_ACCT_CNT_AVG3M
					FROM L14
					INNER JOIN L25 ON
                    L14.BASEL_CUST_ID = L25.BASEL_CUST_ID
        )
    """
): 
    pass

