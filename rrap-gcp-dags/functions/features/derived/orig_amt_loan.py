import pyarrow as pa
from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

UPSTREAM_ASSET = ['ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT',
                'ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT',
                'ingestion.MORT_MTH_SNAPSHOT',
                'ingestion.BASEL_ACCT_PRFM_FACT',
                'ingestion.TNG_ACCT_MO',
                'ingestion.BASEL_ACCT_DIM']

DOWNSTREAM_ASSET = 'features.ORIG_AMT_LOAN'

DEPENDENCIES = {
    "export_ks": ["duckdb_clear_orig_amt_loan"],
    "export_spl": ["duckdb_clear_orig_amt_loan"],
    "export_mor": ["duckdb_clear_orig_amt_loan"],
    "export_tng": ["duckdb_clear_orig_amt_loan"],
    'duckdb_clear_orig_amt_loan': ['duckdb_derive_orig_amt_loan']
}

def export_ks(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    SELECT
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        ks.BASEL_ACCT_ID,
        'KS' AS SRC_SYS_CD,
        LOAN_AMT_AT_INSURED_DATE AS ORIG_AMT_LOAN
    FROM {UPSTREAM_ASSET[0]} ks
    LEFT JOIN {UPSTREAM_ASSET[3]} fact ON
        ks.BASEL_ACCT_ID = fact.BASEL_ACCT_ID
    WHERE
        ks.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    GROUP BY
        ks.BASEL_ACCT_ID,
        LOAN_AMT_AT_INSURED_DATE
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
        LOAN_AMT_AT_INSURED_DATE AS ORIG_AMT_LOAN
    FROM {UPSTREAM_ASSET[1]} spl
    LEFT JOIN {UPSTREAM_ASSET[3]} fact ON
        spl.BASEL_ACCT_ID = fact.BASEL_ACCT_ID
    WHERE
        spl.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    GROUP BY
        spl.BASEL_ACCT_ID,
        LOAN_AMT_AT_INSURED_DATE
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
        LOAN_AMT_AT_INSURED_DATE AS ORIG_AMT_LOAN
    FROM {UPSTREAM_ASSET[2]} mor
    LEFT JOIN {UPSTREAM_ASSET[3]} fact ON
        mor.BASEL_ACCT_ID = fact.BASEL_ACCT_ID
        AND mor.MTH_TM_ID = fact.MTH_TM_ID
    WHERE
        mor.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    GROUP BY
        mor.BASEL_ACCT_ID,
        LOAN_AMT_AT_INSURED_DATE
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
        LOAN_AMT_AT_INSURED_DATE AS ORIG_AMT_LOAN
    FROM {UPSTREAM_ASSET[4]} tng
    INNER JOIN {UPSTREAM_ASSET[5]} dim ON
        dim.SRC_APP_CD = 'TNG-MOR'
        AND dim.SRC_SYS_DEL_F != 'Y'
        AND TRIM(tng.ACCOUNT_ID) = TRIM(dim.SRC_APP_ID)
    LEFT JOIN {UPSTREAM_ASSET[3]} fact ON
        dim.BASEL_ACCT_ID = fact.BASEL_ACCT_ID
        AND fact.MTH_END_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    WHERE tng.MONTH_END_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
"""
):
    pass

def duckdb_clear_orig_amt_loan(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
        DELETE FROM { DOWNSTREAM_ASSET } 
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass

def duckdb_derive_orig_amt_loan(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    INSERT INTO { DOWNSTREAM_ASSET } BY NAME 
    FROM (
        SELECT
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            BASEL_ACCT_ID,
            SRC_SYS_CD,
            ORIG_AMT_LOAN
        FROM read_parquet([
            '{{{{ task_instance.xcom_pull(task_ids="derived__orig_amt_loan.export_ks", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__orig_amt_loan.export_spl", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__orig_amt_loan.export_mor", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__orig_amt_loan.export_tng", key="parquet") }}}}'
        ], union_by_name=true)
    )
    """
):
    pass
