from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)


UPSTREAM_ASSET = [
    "ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT",
    "ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT",
    "features.BASEL_PRD_CD",
    "features.HELOC_F",
    "features.PRD_ID",
]
DOWNSTREAM_ASSET = "features.RRAP_PRD_CD"
DEPENDENCIES = {
    "export_ks": ["duckdb_delete"],
    "export_spl": ["duckdb_delete"],
    "duckdb_delete": ["duckdb_load"],
}


def export_ks(
    duckdb_conn_id="duckdb-conn",
    sql=r"""
        SELECT
            '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' AS OBSN_DT,
            CRM.BASEL_ACCT_ID,
            CASE
                WHEN HELOC.HELOC_F = 'Y' THEN 'HELOC'
                WHEN HELOC.HELOC_F = 'N'
                AND PRD.BASEL_PRD_CD = 'LOC' THEN 'LOC'
                WHEN HELOC.HELOC_F = 'N'
                AND PRD.BASEL_PRD_CD = 'CC' THEN 'CC'
                WHEN HELOC.HELOC_F = 'N'
                AND PRD.BASEL_PRD_CD = 'SL A' THEN 'SL A'
                WHEN HELOC.HELOC_F = 'N'
                AND PRD.BASEL_PRD_CD = 'SL B' THEN 'SL B'
                WHEN HELOC.HELOC_F = 'N'
                AND PRD.BASEL_PRD_CD = 'SL' THEN 'SL'
            END AS RRAP_PRD_CD
        FROM
            ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT CRM
            LEFT JOIN features.BASEL_PRD_CD PRD ON CRM.BASEL_ACCT_ID = PRD.BASEL_ACCT_ID and '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' = PRD.OBSN_DT
            LEFT JOIN features.HELOC_F HELOC ON CRM.BASEL_ACCT_ID = HELOC.BASEL_ACCT_ID and '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' = HELOC.OBSN_DT
        WHERE
            CRM.MTH_TM_ID = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}
    """,
):
    pass


def export_spl(
    duckdb_conn_id="duckdb-conn",
    sql=r"""
        SELECT
            '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' AS OBSN_DT,
            BASEL_ACCT_ID,
            CASE
                WHEN TRIM(PRD_ID) IN (
                    'S01',
                    'S02',
                    'S03',
                    'S04',
                    'S05',
                    'S06',
                    'S07',
                    'S08'
                ) THEN 'DTL'
                WHEN TRIM(PRD_ID) IN ('S09', 'S10', 'S11', 'S12', 'S13', 'S14', 'S15') THEN 'ITL'
            END AS RRAP_PRD_CD
        FROM
            features.PRD_ID
        where OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
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
        select * from read_parquet(['{{{{ task_instance.xcom_pull(task_ids="derived__rrap_prd_cd.export_ks", key="parquet") }}}}','{{{{ task_instance.xcom_pull(task_ids="derived__rrap_prd_cd.export_spl", key="parquet") }}}}'], union_by_name = true)
    )
    """,
):
    pass


