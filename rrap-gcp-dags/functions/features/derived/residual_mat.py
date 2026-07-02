from airflow.sdk import get_current_context
from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

UPSTREAM_ASSET = ["ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT",
                  "ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT",
                  "ingestion.MORT_MTH_SNAPSHOT",
                  "ingestion.TNG_ACCT_MO",
                  "ingestion.BASEL_ACCT_DIM",
                  "ingestion.BASELAYER_MOR",
                  "features.E_MAT_DT",
                  "features.LAST_RGL_PAY_DT",
                  "features.AMORT_IF",
                  "features.NOTE_DT",
                  "features.MAT_DT"]

DOWNSTREAM_ASSET = "features.RESIDUAL_MAT"
DEPENDENCIES = {
    "export_ks": ["duckdb_delete_residual_mat"],
    "export_spl": ["duckdb_delete_residual_mat"],
    "export_mor": ["duckdb_delete_residual_mat"],
    "export_tng": ["duckdb_delete_residual_mat"],
    "duckdb_delete_residual_mat": ["duckdb_load_residual_mat"],
}

def export_ks(
    duckdb_conn_id = 'duckdb-conn',
    sql=f"""
    SELECT
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        ks.BASEL_ACCT_ID,
        'KS' AS SRC_SYS_CD,
        NULL AS RESIDUAL_MAT
    FROM {UPSTREAM_ASSET[0]} ks
    WHERE ks.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    """
):
    pass

def export_spl(
    duckdb_conn_id = 'duckdb-conn',
    sql=f"""
    SELECT
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        spl.BASEL_ACCT_ID,
        'SPL' AS SRC_SYS_CD,
        CASE
            WHEN e_mat.E_MAT_DT IS NULL THEN 
                date_diff(
                    'Month',
                    DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}',
                    last_day((note.NOTE_DT + am.AMORT_IF))
                )
            WHEN e_mat.E_MAT_DT < DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' THEN 
                date_diff(
                    'Month',
                    DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}',
                    rgl.LAST_RGL_PAY_DT
                )
            ELSE 
                date_diff(
                    'Month',
                    DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}',
                    e_mat.E_MAT_DT
                )
        END AS RESIDUAL_MAT
    FROM {UPSTREAM_ASSET[1]} spl
    LEFT JOIN {UPSTREAM_ASSET[6]} e_mat ON
        spl.BASEL_ACCT_ID = e_mat.BASEL_ACCT_ID
        AND e_mat.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    LEFT JOIN {UPSTREAM_ASSET[7]} rgl ON
        spl.BASEL_ACCT_ID = rgl.BASEL_ACCT_ID
        AND rgl.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    LEFT JOIN {UPSTREAM_ASSET[8]} am ON
        spl.BASEL_ACCT_ID = am.BASEL_ACCT_ID
        AND am.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    LEFT JOIN {UPSTREAM_ASSET[9]} note ON
        spl.BASEL_ACCT_ID = note.BASEL_ACCT_ID
        AND note.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
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
        'MOR' AS SRC_SYS_CD,
        base.RESIDUAL_MATURITY AS RESIDUAL_MAT
    FROM {UPSTREAM_ASSET[2]} mor
    LEFT JOIN {UPSTREAM_ASSET[5]} base ON
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
        'TNG-MOR' AS SRC_SYS_CD,
        date_diff('Month', MONTH_END_DT, mat.MAT_DT) AS RESIDUAL_MAT
    FROM {UPSTREAM_ASSET[3]} tng
    INNER JOIN {UPSTREAM_ASSET[4]} dim ON
        dim.SRC_APP_CD = 'TNG-MOR'
        AND dim.SRC_SYS_DEL_F != 'Y'
        AND trim(tng.ACCOUNT_ID) = TRIM(dim.SRC_APP_ID)
    LEFT JOIN {UPSTREAM_ASSET[10]} mat ON
        dim.BASEL_ACCT_ID = mat.BASEL_ACCT_ID
        AND mat.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    WHERE tng.MONTH_END_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass

def duckdb_delete_residual_mat(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        DELETE FROM {DOWNSTREAM_ASSET}
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """,
):
    pass

def duckdb_load_residual_mat(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        INSERT INTO {DOWNSTREAM_ASSET} BY NAME
        FROM (
            SELECT
                OBSN_DT,
                BASEL_ACCT_ID,
                SRC_SYS_CD,
                RESIDUAL_MAT
            FROM read_parquet([
            '{{{{ task_instance.xcom_pull(task_ids="derived__residual_mat.export_ks", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__residual_mat.export_spl", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__residual_mat.export_mor", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__residual_mat.export_tng", key="parquet") }}}}'
        ], union_by_name=true)
        )
    """,
):
    pass
