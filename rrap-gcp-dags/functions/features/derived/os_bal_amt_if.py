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
    "ingestion.BASEL_ACCT_DIM",
    "ingestion.TNG_ACCT_MO",
    "features.OS_BAL_AMT_V2"
]
DOWNSTREAM_ASSET = "features.OS_BAL_AMT_IF"
DEPENDENCIES = {
    "export_ks": ["duckdb_delete"],
    "export_mor": ["duckdb_delete"],
    "export_spl": ["duckdb_delete"],
    "export_tng": ["duckdb_delete"],
    "duckdb_delete": ["duckdb_load"],
}


def export_ks(
    duckdb_conn_id="duckdb-conn",
    sql=r"""
        SELECT
            rvl.BASEL_ACCT_ID,
            (COALESCE(rvl.tot_new_bal_amt, 0)) AS OS_BAL_AMT_IF,
            'KS' AS SRC_SYS_CD 
        FROM
            ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT AS rvl
        WHERE
            rvl.mth_tm_id = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}
    """,
):
    pass


def export_mor(
    duckdb_conn_id="duckdb-conn",
    sql=r"""
        SELECT
            mor.BASEL_ACCT_ID,
            COALESCE(CRNT_BAL_AMT, 0) AS OS_BAL_AMT_IF,
            'MOR' AS SRC_SYS_CD
        FROM
            ingestion.MORT_MTH_SNAPSHOT mor
        WHERE
            mor.mth_tm_id = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}
    """,
):
    pass


def export_spl(
    duckdb_conn_id="duckdb-conn",
    sql=r"""
        SELECT
            spl.BASEL_ACCT_ID,
            COALESCE(os_bal.OS_BAL_AMT_V2, 0) AS OS_BAL_AMT_IF,
            'SPL' AS SRC_SYS_CD
        FROM
            ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT AS spl
        LEFT JOIN features.OS_BAL_AMT_V2 os_bal ON
            spl.BASEL_ACCT_ID = os_bal.BASEL_ACCT_ID
            AND os_bal.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
        WHERE
            spl.mth_tm_id = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}
    """,
):
    pass


def export_tng(
    duckdb_conn_id = 'duckdb-conn',
    sql=f"""
        SELECT
            dim.BASEL_ACCT_ID,
            'TNG-MOR' AS SRC_SYS_CD,
            END_PRINCIPAL_BALANCE AS OS_BAL_AMT_IF
        FROM {UPSTREAM_ASSET[4]} dim
        LEFT JOIN {UPSTREAM_ASSET[5]} tng ON
            dim.SRC_APP_CD = 'TNG-MOR'
            AND dim.SRC_SYS_DEL_F != 'Y'
            AND TRIM(tng.ACCOUNT_ID) = TRIM(dim.SRC_APP_ID)
        WHERE tng.MONTH_END_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'

    """
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
        ['{{{{ task_instance.xcom_pull(task_ids="derived__os_bal_amt_if.export_ks", key="parquet") }}}}',
        '{{{{ task_instance.xcom_pull(task_ids="derived__os_bal_amt_if.export_spl", key="parquet") }}}}',
        '{{{{ task_instance.xcom_pull(task_ids="derived__os_bal_amt_if.export_mor", key="parquet") }}}}',
        '{{{{ task_instance.xcom_pull(task_ids="derived__os_bal_amt_if.export_tng", key="parquet") }}}}'], union_by_name = true)
    )
    """,
):
    pass