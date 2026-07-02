import pyarrow as pa
from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

UPSTREAM_ASSET = ['ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT', 'ingestion.CUST_XREF', 'ingestion.BASEL_CUST_DIM', 'ingestion.IWF_CUST_ACCT', 'ingestion.IWD_PD_PLN']
DOWNSTREAM_ASSET = 'features.NONREGINVBAL'
DEPENDENCIES = {
    'duckdb_clear_nonreginvbal': ['export_get_cust_products_agg'],
    'export_get_cust_products_agg': ['export_get_basel_cust_mth_postn_sum_fact'],
    'export_get_basel_cust_mth_postn_sum_fact': ['duckdb_load_nonreginvbal'],
}


def duckdb_clear_nonreginvbal(
        duckdb_conn_id = "duckdb-conn",
        sql=rf"""
        DELETE FROM {DOWNSTREAM_ASSET} WHERE 
            OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
        """
):
    pass

def export_get_cust_products_agg(
    duckdb_conn_id = "duckdb-conn",
    sql=rf"""
    SELECT
        CUST_BASE_KEY,
        TIME_KEY,
        SUM(CASE WHEN e.sum_srvc_code IN ('CHQ', 'SAV') THEN 1 ELSE 0 END) AS D2D_ACCT_CNT,
        SUM(CASE WHEN e.sum_srvc_code IN ('CHQ', 'SAV') THEN acct_bal ELSE 0 END) AS D2D_BAL_AMT,
        SUM(CASE WHEN e.sum_srvc_code IN ('RDS','REF','REI','RIF','RSP','TFS') THEN 1 ELSE 0 END) AS RGSTRD_INVSTMNT_BAL_ACCT_CNT,
        SUM(CASE WHEN e.sum_srvc_code IN ('RDS','REF','REI','RIF','RSP','TFS') THEN acct_bal ELSE 0 END) AS RGSTRD_INVSTMNT_BAL_AMT,
        SUM(CASE WHEN e.sum_srvc_code IN ('CSH','GIC','MUT') THEN 1 ELSE 0 END) AS NON_REGISTERED_INVSTMNT_ACCT_CNT,
        SUM(CASE WHEN e.sum_srvc_code IN ('CSH','GIC','MUT') THEN acct_bal ELSE 0 END) AS NON_REGISTERED_INVSTMNT_BAL_AMT
    FROM {UPSTREAM_ASSET[3]} d
    LEFT JOIN {UPSTREAM_ASSET[4]} e
        ON d.PD_PLN_KEY = e.PD_PLN_KEY
    WHERE d.ACCT_LCST IN ('A', 'I', 'D') AND d.TIME_KEY = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}'
    GROUP BY CUST_BASE_KEY, TIME_KEY
    """
):
    pass

def export_get_basel_cust_mth_postn_sum_fact(
    duckdb_conn_id = "duckdb-conn",
    sql=rf"""
    SELECT
        d.TIME_KEY as MTH_TM_ID,
        c.BASEL_CUST_ID,
        d.D2D_ACCT_CNT,
        d.D2D_BAL_AMT,
        d.RGSTRD_INVSTMNT_BAL_ACCT_CNT,
        d.RGSTRD_INVSTMNT_BAL_AMT,
        d.NON_REGISTERED_INVSTMNT_ACCT_CNT,
        d.NON_REGISTERED_INVSTMNT_BAL_AMT,
        CURRENT_TIMESTAMP AS INSRT_PROCESS_TMSTMP
    FROM read_parquet('{{{{ task_instance.xcom_pull(task_ids="derived__nonreginvbal.export_get_cust_products_agg", key="parquet") }}}}') d
    JOIN {UPSTREAM_ASSET[1]} b
        ON d.CUST_BASE_KEY = b.CUST_BASE_KEY
    JOIN {UPSTREAM_ASSET[2]} c
        ON b.CUST_ID = TRIM(c.CUST_CID)
    WHERE d.TIME_KEY = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    """
):
    pass

def duckdb_load_nonreginvbal(
    duckdb_conn_id = "duckdb-conn",
    sql=rf"""
    INSERT INTO {DOWNSTREAM_ASSET} BY NAME 
    FROM (
        SELECT
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            a.BASEL_CUST_ID,
            CAST(a.NON_REGISTERED_INVSTMNT_BAL_AMT AS DECIMAL(17,3)) AS NONREGINVBAL
        FROM read_parquet('{{{{ task_instance.xcom_pull(task_ids="derived__nonreginvbal.export_get_basel_cust_mth_postn_sum_fact", key="parquet") }}}}') a
        )
        """
):
    pass

