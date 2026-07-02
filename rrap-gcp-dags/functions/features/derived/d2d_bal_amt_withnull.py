from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)


UPSTREAM_ASSET = [
    "ingestion.IWF_CUST_ACCT",
    "ingestion.IWD_PD_PLN",
    "ingestion.CUST_XREF",
    "ingestion.BASEL_CUST_DIM",
]
DOWNSTREAM_ASSET = "features.D2D_BAL_AMT_WITHNULL"
DEPENDENCIES = {
    "export_d2d": ["export"],
    "export": ["duckdb_delete"],
    "duckdb_delete": ["duckdb_load"],
}


def export_d2d(
    duckdb_conn_id="duckdb-conn",
    sql=r"""
        WITH
            main AS (
                SELECT
                    cust.CUST_BASE_KEY,
                    pln.pd_pln_key,
                    pln.sum_srvc_code,
                    cust.time_key,
                    cust.ACCT_BAL
                FROM
                    ingestion.IWF_CUST_ACCT AS cust
                    INNER JOIN ingestion.IWD_PD_PLN AS pln ON cust.pd_pln_key = pln.pd_pln_key
                WHERE
                    cust.TIME_KEY = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}
                    AND trim(cust.acct_lcst) IN ('A', 'I', 'D')
            )
        SELECT
            main.TIME_KEY AS TIME_KEY,
            main.CUST_BASE_KEY AS CUST_BASE_KEY,
            SUM(
                CASE
                    WHEN trim(main.sum_srvc_code) IN ('CHQ', 'SAV') THEN COALESCE(main.ACCT_BAL, 0)
                    WHEN trim(main.sum_srvc_code) IN ('CSH','GIC','MUT','RDS','RIF','RSP','TFS') THEN 0
                    ELSE NULL
                END
            ) AS D2D_BAL_AMT_WITHNULL
        FROM
            main
        GROUP BY
            main.TIME_KEY,
            main.CUST_BASE_KEY
    """,
):
    pass


def export(
    duckdb_conn_id="duckdb-conn",
    sql=r"""
        WITH
            xref AS (
                SELECT
                    cust_base_key,
                    CUST_ID
                FROM
                    ingestion.CUST_XREF
                WHERE
                    POPN_DT <= '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' or POPN_DT <= '2024-09-30'
                GROUP BY
                    cust_base_key,
                    CUST_ID
            ),
            dim AS (
                SELECT
                    basel_cust_id,
                    cust_cid
                FROM
                    ingestion.BASEL_CUST_DIM
                WHERE
                    INSRT_PROCESS_TMSTMP < last_day(date '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' + interval 1 month)
            )
        SELECT
            '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' AS OBSN_DT,
            dim.BASEL_CUST_ID AS BASEL_CUST_ID,
            main.D2D_BAL_AMT_WITHNULL::decimal(20,6) AS D2D_BAL_AMT_WITHNULL
        FROM
            '{{ task_instance.xcom_pull(task_ids="derived__d2d_bal_amt_withnull.export_d2d", key="parquet") }}' AS main
            INNER JOIN xref AS xref ON main.CUST_BASE_KEY = xref.CUST_BASE_KEY
            INNER JOIN dim AS dim ON TRIM(xref.CUST_ID) = TRIM(dim.CUST_CID)
    """,
):
    pass


def duckdb_delete(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    delete from {DOWNSTREAM_ASSET} where obsn_dt = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """,
):
    pass


def duckdb_load(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} by name
    FROM (
        select * from read_parquet(['{{{{ task_instance.xcom_pull(task_ids="derived__d2d_bal_amt_withnull.export", key="parquet") }}}}'])
    )
    """,
):
    pass
