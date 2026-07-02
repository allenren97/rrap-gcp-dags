from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)


UPSTREAM_ASSET = ["features.D2D_BAL_AMT"]
DOWNSTREAM_ASSET = "features.D2D_BAL_AMT_AVG3M"
DEPENDENCIES = {
    "export": ["duckdb_delete"],
    "duckdb_delete": ["duckdb_load"],
}


def export(
    duckdb_conn_id="duckdb-conn",
    sql=r"""
        SELECT
            basel_cust_id,
            COUNT(1) AS CUSTACCT_3MTH_AGE,
            SUM(D2D_BAL_AMT) AS D2D_BAL_SUM
        FROM
            (
                SELECT
                    OBSN_DT,
                    basel_cust_id,
                    D2D_BAL_AMT
                FROM
                    features.D2D_BAL_AMT
                WHERE
                    OBSN_DT <= '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
                    AND OBSN_DT > last_day (DATE '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' - INTERVAL 3 MONTH)
                GROUP BY
                    OBSN_DT,
                    basel_cust_id,
                    D2D_BAL_AMT
            )
        GROUP BY
            basel_cust_id
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
        SELECT
            basel_cust_id AS basel_cust_id,
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS obsn_dt,
            CASE
                WHEN CUSTACCT_3MTH_AGE = 0
                OR D2D_BAL_SUM IS NULL THEN NULL
                ELSE (D2D_BAL_SUM / CUSTACCT_3MTH_AGE)::decimal(18,3)
            END AS D2D_BAL_AMT_AVG3M,
        FROM
            '{{{{ task_instance.xcom_pull(task_ids="derived__d2d_bal_amt_avg3m.export", key="parquet") }}}}'
    )
    """,
):
    pass
