from airflow.sdk import get_current_context, Param

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

DAG_PARAMS = {
    "mth_tm_id": Param("", type=["null", "string"], description="MTH_TM_ID"),
    "rundate": Param("", type=["null", "string"], description="RUNDATE"),
}
UPSTREAM_ASSET = [
    "features.PIT_STATUS_ACCOUNT",
    "features.PIT_STATUS_CROSS_DEFAULT",
]
DOWNSTREAM_ASSET = "features.PIT_STATUS_STEP"
DEPENDENCIES = {
    "export": ["duckdb_delete"],
    "duckdb_delete": ["duckdb_load"],
}


def export(
    duckdb_conn_id="duckdb-conn",
    sql=r"""
    SELECT
        OBSN_DT,
        A.BASEL_ACCT_ID,
        STEP_PLN_AGRMNT_NUM,
        SRC_SYS_CD,
        CASE
            WHEN TRIM(PIT_STATUS_CROSS_DEFAULT) IN ('DEF', 'CHG') THEN 'DEF'
            WHEN TRIM(PIT_STATUS_CROSS_DEFAULT) IN ('CUR') THEN 'CUR'
            WHEN TRIM(PIT_STATUS_CROSS_DEFAULT) IN ('CLO') THEN 'CLO'
            WHEN TRIM(PIT_STATUS_CROSS_DEFAULT) IS NULL THEN NULL
        END AS PIT_STATUS_STEP
    FROM
        features.PIT_STATUS_CROSS_DEFAULT as A
        LEFT JOIN (
            SELECT
                BASEL_ACCT_ID,
                STEP_PLN_AGRMNT_NUM
            FROM features.PIT_STATUS_ACCOUNT
            WHERE OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
        ) as B on A.BASEL_ACCT_ID = B.BASEL_ACCT_ID
    WHERE
        OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
    """,
):
    pass


def duckdb_delete(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    delete from {DOWNSTREAM_ASSET} where obsn_dt = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """,
    max_active_tis_per_dag=1,
):
    pass


def duckdb_load(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} by name
    FROM (
        select * from read_parquet(['{{{{ task_instance.xcom_pull(task_ids="derived__pit_status_step.export", key="parquet") }}}}'])
    )
    """,
    max_active_tis_per_dag=1,
):
    pass
