from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)


UPSTREAM_ASSET = [
    "ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT",
    "ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT",
    "ingestion.MORT_MTH_SNAPSHOT",
    "reference.SRC_PRD_LKP",
]
DOWNSTREAM_ASSET = "features.OS_BAL_AMT"
DEPENDENCIES = {
    "export_ks": ["duckdb_delete"],
    "export_mor": ["duckdb_delete"],
    "export_spl": ["duckdb_delete"],
    "duckdb_delete": ["duckdb_load"],
}


def export_ks(
    duckdb_conn_id="duckdb-conn",
    sql=r"""
        SELECT
            rvl.BASEL_ACCT_ID,
            (COALESCE(rvl.tot_new_bal_amt, 0)) AS OS_BAL_AMT,
            'KS' AS SRC_SYS_CD 
        FROM
            ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT AS rvl
            LEFT JOIN (
                SELECT
                    SRC_PRD_CD,
                    SRC_SUB_PRD_CD,
                    SML_BUS_F
                FROM
                    reference.SRC_PRD_LKP
                WHERE
                    '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="yyyymm") }}' BETWEEN EFF_FROM_YR_MTH AND EFF_TO_YR_MTH
            ) AS sp ON TRIM(rvl.PRD_CD) = TRIM(sp.SRC_PRD_CD)
            AND TRIM(rvl.SUB_PRD_CD) = TRIM(sp.SRC_SUB_PRD_CD)
        WHERE
            rvl.mth_tm_id = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}
            AND TRIM(sp.SML_BUS_F) = 'N'
    """,
):
    pass


def export_mor(
    duckdb_conn_id="duckdb-conn",
    sql=r"""
        SELECT
            BASEL_ACCT_ID,
            (
                COALESCE(crnt_bal_amt, 0) + COALESCE(intr_accr_amt, 0)
            ) AS OS_BAL_AMT,
            'MOR' AS SRC_SYS_CD
        FROM
            ingestion.MORT_MTH_SNAPSHOT
        WHERE
            mth_tm_id = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}
    """,
):
    pass


def export_spl(
    duckdb_conn_id="duckdb-conn",
    sql=r"""
        SELECT
            BASEL_ACCT_ID,
            (
                COALESCE(tot_crnt_bal_amt, 0) + COALESCE(add_on_bal_amt, 0) + COALESCE(accr_intr, 0)
            ) AS OS_BAL_AMT,
            'SPL' AS SRC_SYS_CD
        FROM
            ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT AS spl
        WHERE
            spl.mth_tm_id = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}
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
        select *, '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' as OBSN_DT 
        from read_parquet(
        ['{{{{ task_instance.xcom_pull(task_ids="derived__os_bal_amt.export_ks", key="parquet") }}}}',
        '{{{{ task_instance.xcom_pull(task_ids="derived__os_bal_amt.export_spl", key="parquet") }}}}',
        '{{{{ task_instance.xcom_pull(task_ids="derived__os_bal_amt.export_mor", key="parquet") }}}}'], union_by_name = true)
    )
    """,
):
    pass


