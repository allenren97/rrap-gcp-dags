from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)


UPSTREAM_ASSET = [
    "ingestion.MORT_MTH_SNAPSHOT",
    "ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT",
    "features.CONSM_PRD_TREATMNT_CD",   # included for the KS portion
    "features.COMM_TP_CD"   # included for the mort portion
]
DOWNSTREAM_ASSET = "features.TREATMENT_F"
DEPENDENCIES = {
    "export_ks": ["duckdb_delete"],
    "export_mor": ["duckdb_delete"],
    "export_spl": ["duckdb_delete"],
    "duckdb_delete": ["duckdb_load"],
}


def export_ks(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        SELECT
            OBSN_DT,
            BASEL_ACCT_ID,
            CONSM_PRD_TREATMNT_CD AS TREATMENT_F
        FROM
            features.CONSM_PRD_TREATMNT_CD
        WHERE
            OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
        AND
            SRC_SYS_CD = 'KS'
    """,
):
    pass


def export_mor(
    duckdb_conn_id="duckdb-conn",
    sql=r"""
        WITH comm_tp AS (
            SELECT
                BASEL_ACCT_ID,
                CASE WHEN UPPER(TRIM(COMM_TP_CD)) = 'RESIDENTIAL'
                THEN 'A' ELSE 'Z' END AS TREATMENT_F FROM features.COMM_TP_CD
            WHERE 
                OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
        ),
        mor AS (
            SELECT
                BASEL_ACCT_ID
            FROM
                ingestion.MORT_MTH_SNAPSHOT
            WHERE
                MTH_TM_ID = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}
        )
        SELECT 
            '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' AS OBSN_DT,
            mor.BASEL_ACCT_ID,
            comm_tp.TREATMENT_F
        FROM 
            mor INNER JOIN comm_tp
        ON 
            mor.BASEL_ACCT_ID = comm_tp.BASEL_ACCT_ID
    """,
):
    pass


def export_spl(
    duckdb_conn_id="duckdb-conn",
    sql=r"""
        SELECT
            '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' AS OBSN_DT,
            BASEL_ACCT_ID,
            CASE WHEN UPPER(TRIM(CRNT_BR_LOCTN_TRNST)) IN (18192, 99432)
            OR UPPER(TRIM(COMM_LOAN_CD)) = 1 THEN 'Z' ELSE 'A' END AS TREATMENT_F
        FROM
            ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT
        WHERE
            MTH_TM_ID = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}
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
        select * from read_parquet(['{{{{ task_instance.xcom_pull(task_ids="derived__treatment_f.export_ks", key="parquet") }}}}','{{{{ task_instance.xcom_pull(task_ids="derived__treatment_f.export_mor", key="parquet") }}}}','{{{{ task_instance.xcom_pull(task_ids="derived__treatment_f.export_spl", key="parquet") }}}}'], union_by_name = true)
    )
    """,
):
    pass


