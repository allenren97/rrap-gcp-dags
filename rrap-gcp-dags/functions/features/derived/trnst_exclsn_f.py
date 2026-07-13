from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)


UPSTREAM_ASSET = [
    "ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT",
    "ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT",
    "ingestion.MORT_MTH_SNAPSHOT",
    "reference.TRNST_EXCLSN_LKP",
    "ingestion.BASELAYER_MOR",
    "ingestion.TNG_ACCT_MO",
    "ingestion.BASEL_ACCT_DIM"
]
DOWNSTREAM_ASSET = "features.TRNST_EXCLSN_F"
DEPENDENCIES = {
    "export_tng": ["duckdb_delete"],
    "export_spl": ["duckdb_delete"],
    "export_ks": ["duckdb_delete"],
    "export_mor": ["duckdb_delete"],
    "duckdb_delete": ["duckdb_load"],
}


def export_spl(
    duckdb_conn_id="duckdb-conn",
    sql=rf"""
        SELECT
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            BASEL_ACCT_ID,
            CASE
                WHEN TRIM(EXCLUDED_TRNST_NUM) IS NULL THEN 'N'
                ELSE 'Y'
            END AS TRNST_EXCLSN_F,
            'SPL' as SRC_SYS_CD
        FROM
            (
            SELECT
                MTH_TM_ID,
                BASEL_ACCT_ID,
                PRIM_BASEL_CUST_ID AS BASEL_CUST_ID,
                EXCLUDED_TRNST_NUM
            FROM
                {UPSTREAM_ASSET[1]} a
                LEFT JOIN reference.TRNST_EXCLSN_LKP c ON a.CRNT_BR_LOCTN_TRNST = c.EXCLUDED_TRNST_NUM
            WHERE
                MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
            )
        """
):
    pass

def export_ks(
    duckdb_conn_id="duckdb-conn",
    sql=rf"""
        SELECT
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            BASEL_ACCT_ID,
            CASE
                WHEN TRIM(EXCLUDED_TRNST_NUM) IS NULL THEN 'N'
                ELSE 'Y'
            END AS TRNST_EXCLSN_F,
            'KS' as SRC_SYS_CD
        FROM
            (
            SELECT
                MTH_TM_ID,
                BASEL_ACCT_ID,
                PRIM_BASEL_CUST_ID AS BASEL_CUST_ID,
                EXCLUDED_TRNST_NUM
            FROM
                {UPSTREAM_ASSET[0]} a
                LEFT JOIN reference.TRNST_EXCLSN_LKP c ON a.TRNST_NUM = c.EXCLUDED_TRNST_NUM
            WHERE
                MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
            )
    """,
):
    pass

def export_mor(
    duckdb_conn_id="duckdb-conn",
    sql=rf"""
        SELECT
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            mor.BASEL_ACCT_ID,
            'MOR' AS SRC_SYS_CD,
            base.TRANSIT_EXCLUSION_FLAG as TRNST_EXCLSN_F
        FROM {UPSTREAM_ASSET[2]} mor
        LEFT JOIN {UPSTREAM_ASSET[4]} base ON
            mor.MORT_NUM = base.MORT_NUM
            AND base.MTH_END_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
        WHERE mor.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    """,
):
    pass

def export_tng(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        SELECT
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            dim.BASEL_ACCT_ID,
            'TNG-MOR' AS SRC_SYS_CD,
            'N' as TRNST_EXCLSN_F
        FROM {UPSTREAM_ASSET[5]} tng
        INNER JOIN {UPSTREAM_ASSET[6]} dim ON
            dim.SRC_APP_CD = 'TNG-MOR'
            AND dim.SRC_SYS_DEL_F != 'Y'
            AND trim(tng.ACCOUNT_ID) = TRIM(dim.SRC_APP_ID)
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
        select * from read_parquet([
            '{{{{ task_instance.xcom_pull(task_ids="derived__trnst_exclsn_f.export_spl", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__trnst_exclsn_f.export_ks", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__trnst_exclsn_f.export_mor", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__trnst_exclsn_f.export_tng", key="parquet") }}}}'],
        union_by_name = true)
    )
    """,
):
    pass


