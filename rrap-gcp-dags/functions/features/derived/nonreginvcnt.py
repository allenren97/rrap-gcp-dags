import pyarrow as pa
from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

UPSTREAM_ASSET = [ 'ingestion.IWF_CUST_ACCT', 'ingestion.IWD_PD_PLN', 
                  'ingestion.CUST_XREF', 'ingestion.BASEL_CUST_DIM' ]

DOWNSTREAM_ASSET = 'features.NONREGINVCNT'

DEPENDENCIES = {
    'duckdb_clear_nonreginvcnt': ['duckdb_derive_nonreginvcnt'],
}


def duckdb_clear_nonreginvcnt(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
        DELETE FROM { DOWNSTREAM_ASSET } 
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass


def duckdb_derive_nonreginvcnt(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    INSERT INTO { DOWNSTREAM_ASSET }
        BY NAME FROM (
           WITH cust_acct_trxn1 AS (
            SELECT
				c.time_key,
                c.CUST_BASE_KEY,
                e.pd_pln_key,
                e.sum_srvc_code
            FROM 
				{UPSTREAM_ASSET[0]} c
            INNER JOIN 
				{UPSTREAM_ASSET[1]} e on c.pd_pln_key = e.pd_pln_key
            WHERE 
				c.acct_lcst in ('A','I','D')
				AND 
				c.TIME_KEY >= {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}-80 AND c.TIME_KEY <= {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
        )
        , cust_products_agg AS (
            SELECT
                t1.time_key,
				t1.cust_base_key,
                SUM(CASE WHEN t1.sum_srvc_code in ('CSH','GIC','MUT') THEN 1 ELSE 0 END) AS nonreginv
            FROM cust_acct_trxn1 t1
            GROUP BY
                t1.time_key,
                t1.cust_base_key
        )
        , CUST_ID_BASE_KEY AS (
            SELECT
                cust_base_key,
                TRIM(CUST_ID) AS CUST_ID
            FROM {UPSTREAM_ASSET[2]}
            GROUP BY
                cust_base_key,
                CUST_ID
        )
        , basel_cust_id_cid AS (
            SELECT
                basel_cust_id
                , TRIM(cust_cid) AS cust_cid
            FROM {UPSTREAM_ASSET[3]}
            GROUP BY
                basel_cust_id,
                cust_cid
        )
        , LD_BASEL_CUST_MTH_POSTN_SUM_FACT AS (
            SELECT
                a.TIME_KEY as MTH_TM_ID,
                c.BASEL_CUST_ID as BASEL_CUST_ID,
                a.nonreginv as NON_REGISTERED_INVSTMNT_ACCT_CNT
            FROM cust_products_agg a
            INNER JOIN cust_id_base_key b ON
                a.CUST_BASE_KEY = b.CUST_BASE_KEY
            INNER JOIN basel_cust_id_cid c ON
                b.CUST_ID = c.CUST_CID
        )
		
	SELECT
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            A.MTH_TM_ID AS MTH_TM_ID,
            A.BASEL_CUST_ID AS BASEL_CUST_ID,
            CAST(A.NON_REGISTERED_INVSTMNT_ACCT_CNT AS INTEGER)  AS NONREGINVCNT
        FROM 
            LD_BASEL_CUST_MTH_POSTN_SUM_FACT A
        WHERE
            A.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
        )
    """
):
    pass

