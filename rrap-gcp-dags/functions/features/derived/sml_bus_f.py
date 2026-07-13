from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)


UPSTREAM_ASSET = [
    "ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT",
    "reference.SRC_PRD_LKP",
    "ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT",
    "ingestion.BASELAYER_MOR",
    "ingestion.MORT_MTH_SNAPSHOT",
    "ingestion.TNG_ACCT_MO",
    "ingestion.BASEL_ACCT_DIM"
]
DOWNSTREAM_ASSET = "features.SML_BUS_F"

DEPENDENCIES = {
    "export_ks": ["duckdb_delete"],
    "export_spl": ["duckdb_delete"],
    "export_mor": ["duckdb_delete"],
    "export_tng": ["duckdb_delete"],
    "duckdb_delete": ["duckdb_load"],
}

def export_ks(
    duckdb_conn_id = 'duckdb-conn',
    sql=f"""
    SELECT
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            BASEL_ACCT_ID,
            TRIM(SML_BUS_F) AS SML_BUS_F
        FROM
            {UPSTREAM_ASSET[0]} A
            LEFT JOIN (
                SELECT
                    *
                FROM
                    {UPSTREAM_ASSET[1]}
                WHERE
                    EFF_FROM_YR_MTH <= strftime ('%Y%m', DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}')
                    AND strftime ('%Y%m', DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}') <= EFF_TO_YR_MTH
            ) SP ON TRIM(A.PRD_CD) = TRIM(SP.SRC_PRD_CD)
            AND TRIM(A.SUB_PRD_CD) = TRIM(SP.SRC_SUB_PRD_CD)
        WHERE
            A.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    """
):
    pass

def export_spl(
    duckdb_conn_id = 'duckdb-conn',
    sql=f"""
    SELECT
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        spl.BASEL_ACCT_ID,
        'N' as SML_BUS_F
    FROM {UPSTREAM_ASSET[2]} spl
    WHERE spl.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    """
):
    pass

def export_mor(
    duckdb_conn_id = "duckdb-conn",
    sql=f"""
    SELECT
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        mor.BASEL_ACCT_ID,
        'N' as SML_BUS_F
    FROM {UPSTREAM_ASSET[4]} mor
    LEFT JOIN {UPSTREAM_ASSET[3]} base ON
        mor.MORT_NUM = base.MORT_NUM
        AND base.MTH_END_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    WHERE mor.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    """
):
    pass

def export_tng(
    duckdb_conn_id = "duckdb-conn",
    sql=f"""
    SELECT
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        dim.BASEL_ACCT_ID,
        'N' as SML_BUS_F
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
    INSERT INTO {DOWNSTREAM_ASSET} BY NAME
        FROM (
            SELECT
                OBSN_DT,
                BASEL_ACCT_ID,
                SML_BUS_F
            FROM read_parquet([
            '{{{{ task_instance.xcom_pull(task_ids="derived__sml_bus_f.export_ks", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__sml_bus_f.export_spl", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__sml_bus_f.export_mor", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__sml_bus_f.export_tng", key="parquet") }}}}'
        ], union_by_name=true)
        )
    """,
):
    pass


