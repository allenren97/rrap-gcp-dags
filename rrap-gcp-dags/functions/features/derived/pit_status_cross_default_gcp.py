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
    "features.PIT_STATUS_ACCOUNT_GCP",
]
DOWNSTREAM_ASSET = "features.PIT_STATUS_CROSS_DEFAULT_GCP"
DEPENDENCIES = {
    "export": ["duckdb_delete"],
    "duckdb_delete": ["duckdb_load"],
}


def export(
    duckdb_conn_id="duckdb-conn",
    sql=r"""
        WITH
            step AS (
                SELECT
                    BASEL_ACCT_ID,
                    CASE
                        WHEN TRIM(PIT_STATUS_ACCOUNT_GCP) = 'CUR' THEN 'DEF'
                        ELSE PIT_STATUS_ACCOUNT_GCP
                    END AS PIT_STATUS_CROSS_DEFAULT_GCP,
                    CASE
                        WHEN TRIM(PIT_STATUS_ACCOUNT_GCP) = 'CUR' THEN 'Y'
                        ELSE 'N'
                    END AS CROSS_DFLT_PIT_OVERRIDE_F
                FROM
                    features.PIT_STATUS_ACCOUNT_GCP
                WHERE
                    STEP_PLN_AGRMNT_NUM IN (
                        SELECT DISTINCT
                            trim(STEP_PLN_AGRMNT_NUM) as STEP_PLN_AGRMNT_NUM
                        FROM
                            features.PIT_STATUS_ACCOUNT_GCP
                        WHERE
                            OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
                            AND TRIM(PIT_STATUS_ACCOUNT_GCP) IN ('CHG', 'DEF')
                            AND STEP_DFLT_F <> 'W'
                            AND coalesce(trim(STEP_PLN_AGRMNT_NUM), '') != ''
                    )
                    AND OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
            )
        SELECT
            OBSN_DT,
            main.BASEL_ACCT_ID,
            main.SRC_SYS_CD,
            CASE
                WHEN step.PIT_STATUS_CROSS_DEFAULT_GCP IS NULL THEN main.PIT_STATUS_ACCOUNT_GCP
                ELSE step.PIT_STATUS_CROSS_DEFAULT_GCP
            END AS PIT_STATUS_CROSS_DEFAULT_GCP,
            CASE
                WHEN step.CROSS_DFLT_PIT_OVERRIDE_F IS NULL THEN 'N'
                ELSE step.CROSS_DFLT_PIT_OVERRIDE_F
            END AS CROSS_DFLT_PIT_OVERRIDE_F
        FROM
            features.PIT_STATUS_ACCOUNT_GCP AS main
            LEFT JOIN step ON step.BASEL_ACCT_ID = main.BASEL_ACCT_ID
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
        select * from read_parquet(['{{{{ task_instance.xcom_pull(task_ids="derived__pit_status_cross_default_gcp.export", key="parquet") }}}}'])
    )
    """,
    max_active_tis_per_dag=1,
):
    pass
