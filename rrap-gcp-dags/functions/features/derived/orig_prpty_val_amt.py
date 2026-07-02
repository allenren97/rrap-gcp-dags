import pyarrow as pa
from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

UPSTREAM_ASSET = [
    'ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT', #[0]
    'ingestion.BASEL_STEP_PLN_MTH_SNAPSHOT', #[1]
    'ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT', #[2]
    'ingestion.MORT_MTH_SNAPSHOT', #[3]
    'ingestion.BASELAYER_MOR', #[4]
    'ingestion.BASEL_ACCT_DIM', #[5]
    'ingestion.TNG_ACCT_MO', #[6]
]

DOWNSTREAM_ASSET = 'features.ORIG_PRPTY_VAL_AMT'
DEPENDENCIES = {
    "export_ks": ["duckdb_clear_orig_prpty_val_amt"],
    "export_spl": ["duckdb_clear_orig_prpty_val_amt"],
    "export_mor": ["duckdb_clear_orig_prpty_val_amt"],
    "export_tng": ["duckdb_clear_orig_prpty_val_amt"],
    'duckdb_clear_orig_prpty_val_amt': ['duckdb_derive_orig_prpty_val_amt']
}

def export_ks(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    SELECT
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        ks.BASEL_ACCT_ID,
        'KS' AS SRC_SYS_CD,
        steps.APRSD_VAL AS ORIG_PRPTY_VAL_AMT
    FROM {UPSTREAM_ASSET[0]} ks
    LEFT JOIN {UPSTREAM_ASSET[1]} steps ON
        ks.STEP_PLN_SNAPSHOT_ID = steps.STEP_PLN_SNAPSHOT_ID
    AND ks.MTH_TM_ID = steps.MTH_TM_ID
    WHERE
        ks.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    GROUP BY
        ks.BASEL_ACCT_ID,
        steps.APRSD_VAL
    """,
):
    pass

def export_spl(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    SELECT
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        spl.BASEL_ACCT_ID,
        'SPL' AS SRC_SYS_CD,
        0 AS ORIG_PRPTY_VAL_AMT
    FROM {UPSTREAM_ASSET[2]} spl
    WHERE
        spl.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    GROUP BY
        spl.BASEL_ACCT_ID
    """,
):
    pass

def export_mor(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    SELECT
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        mor.BASEL_ACCT_ID,
        'MOR' AS SRC_SYS_CD,
        base.ORIGINALPROPERTYVALUE AS ORIG_PRPTY_VAL_AMT
    FROM {UPSTREAM_ASSET[3]} mor
    LEFT JOIN {UPSTREAM_ASSET[4]} base ON
        mor.MORT_NUM = base.MORT_NUM
    AND
        mor.MTH_END_DT = base.MTH_END_DT
    WHERE
        mor.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    AND
        base.MORT_NUM NOT NULL
    GROUP BY
        mor.BASEL_ACCT_ID,
        base.ORIGINALPROPERTYVALUE
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
        tng.ORIG_PROP_APPRAISAL_VAL AS ORIG_PRPTY_VAL_AMT
    FROM 
        {UPSTREAM_ASSET[6]} tng
    LEFT JOIN
        {UPSTREAM_ASSET[5]} dim
    ON
        dim.SRC_APP_ID = tng.ACCOUNT_ID
    AND
        UPPER(TRIM(dim.SRC_APP_CD)) ='TNG-MOR'
    AND 
        UPPER(TRIM(dim.SRC_SYS_DEL_F)) != 'Y'
    WHERE
        tng.month_end_dt = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    GROUP BY
        dim.BASEL_ACCT_ID,
        tng.ORIG_PROP_APPRAISAL_VAL
    """,
):
    pass

def duckdb_clear_orig_prpty_val_amt(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
        DELETE FROM { DOWNSTREAM_ASSET } 
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass

def duckdb_derive_orig_prpty_val_amt(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    INSERT INTO { DOWNSTREAM_ASSET } BY NAME 
    FROM (
        SELECT
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            BASEL_ACCT_ID,
            SRC_SYS_CD,
            ORIG_PRPTY_VAL_AMT
        FROM read_parquet([
            '{{{{ task_instance.xcom_pull(task_ids="derived__orig_prpty_val_amt.export_ks", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__orig_prpty_val_amt.export_spl", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__orig_prpty_val_amt.export_mor", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__orig_prpty_val_amt.export_tng", key="parquet") }}}}'
        ], union_by_name=true)
    )
    """
):
    pass
