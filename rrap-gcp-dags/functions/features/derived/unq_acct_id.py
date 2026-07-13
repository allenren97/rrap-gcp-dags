from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

UPSTREAM_ASSET = [
    'ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT',
    'ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT',
    'features.SML_BUS_F',
    'ingestion.BASELAYER_MOR',
    'ingestion.MORT_MTH_SNAPSHOT',
    'ingestion.TNG_ACCT_MO',
    'ingestion.BASEL_ACCT_DIM'
]
DOWNSTREAM_ASSET = "features.UNQ_ACCT_ID"
DEPENDENCIES = {
    "export_spl": ["duckdb_delete_unq_acct_id"],
    "export_ks": ["duckdb_delete_unq_acct_id"],
    "export_mor": ["duckdb_delete_unq_acct_id"],
    "export_tng": ["duckdb_delete_unq_acct_id"],
    "duckdb_delete_unq_acct_id": ["duckdb_load_unq_acct_id"],
}

def export_ks(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
        SELECT 
            brcms.BASEL_ACCT_ID,
            sbf.OBSN_DT,
            CASE 
                WHEN TRIM(sbf.SML_BUS_F) = 'N' THEN CONCAT('CA0201', brcms.ACCT_NUM) 
                ELSE NULL
            END AS UNQ_ACCT_ID
        FROM {UPSTREAM_ASSET[1]} brcms --BASEL_RVLVNG_CR_MTH_SNAPSHOT
        LEFT JOIN {UPSTREAM_ASSET[2]} sbf --SML_BUS_F
            ON brcms.BASEL_ACCT_ID = sbf.BASEL_ACCT_ID
        WHERE brcms.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
        AND sbf.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass


def export_spl(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
        SELECT
            b.BASEL_ACCT_ID,
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            CONCAT('CA0201', LPAD(CONCAT(LPAD(CAST(b.CRNT_BR_LOCTN_TRNST AS VARCHAR), 5, '0'), LPAD(CAST(b.LOAN_NUM AS VARCHAR), 7, '0')), 23, '0')
            ) AS UNQ_ACCT_ID
        FROM {UPSTREAM_ASSET[0]} b --BASEL_PSNL_LOAN_MTH_SNAPSHOT
        WHERE b.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    """
):
    pass


def export_mor(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
        SELECT
            mort.BASEL_ACCT_ID,
            base.MTH_END_DT AS OBSN_DT,
            base.UNIQUEACCOUNTIDENTIFIER AS UNQ_ACCT_ID
        FROM {UPSTREAM_ASSET[4]} mort --MORT_MTH_SNAPSHOT 
        LEFT JOIN {UPSTREAM_ASSET[3]} base --BASELAYER_MOR
            ON mort.MORT_NUM = base.MORT_NUM
            AND base.MTH_END_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
        WHERE 
            mort.MTH_TM_ID =  {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    """
):
    pass


def export_tng(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
        SELECT
            dim.BASEL_ACCT_ID,
            tng.MONTH_END_DT AS OBSN_DT,
            CONCAT(
                'CA',
                '0207', 
                LPAD(CAST(tng.account_key AS VARCHAR), 13, '0')
            ) AS UNQ_ACCT_ID
        FROM {UPSTREAM_ASSET[5]} tng
        INNER JOIN {UPSTREAM_ASSET[6]} dim 
            ON dim.SRC_APP_CD = 'TNG-MOR'
            AND dim.SRC_SYS_DEL_F != 'Y'
            AND trim(tng.ACCOUNT_ID) = TRIM(dim.SRC_APP_ID)
        WHERE tng.MONTH_END_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass


def duckdb_delete_unq_acct_id(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
        DELETE FROM { DOWNSTREAM_ASSET } 
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass


def duckdb_load_unq_acct_id(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        INSERT INTO {DOWNSTREAM_ASSET} BY NAME
        FROM (
            SELECT
                BASEL_ACCT_ID,
                OBSN_DT,
                UNQ_ACCT_ID
            FROM read_parquet([
            '{{{{ task_instance.xcom_pull(task_ids="derived__unq_acct_id.export_ks", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__unq_acct_id.export_spl", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__unq_acct_id.export_mor", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__unq_acct_id.export_tng", key="parquet") }}}}'
        ], union_by_name=true)
        )
    """,
):
    pass
