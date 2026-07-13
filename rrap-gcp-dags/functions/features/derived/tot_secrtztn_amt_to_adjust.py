import pyarrow as pa
from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

UPSTREAM_ASSET = ['features.ADJUSTED_OS_BAL_AMT_SECURITIZATION',
                  'ingestion.SPL_SOURCE_FILE_AMOUNT',
                  'ingestion.SPL_SOURCE_FILE_ACCOUNTS',
                  'ingestion.CC_SOURCE_FILE_SECURITIZATION',
                  'ingestion.CC_SOURCE_FILE_AMOUNT',
                  'ingestion.CL_SOURCE_FILE_SECURITIZATION',
                  'ingestion.CL_SOURCE_FILE_AMOUNT'
]

DOWNSTREAM_ASSET = 'features.TOT_SECRTZTN_AMT_TO_ADJUST'

DEPENDENCIES = {
    'export_auto': ['duckdb_clear_tot_secrtztn_amt_to_adjust'],
    'export_cc': ['duckdb_clear_tot_secrtztn_amt_to_adjust'],
    'export_cl': ['duckdb_clear_tot_secrtztn_amt_to_adjust'],
    'duckdb_clear_tot_secrtztn_amt_to_adjust': ['duckdb_derive_tot_secrtztn_amt_to_adjust'],
}


def export_auto(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
        SELECT
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            'AUTO' AS SECRTZTN_TP_CD,
            TRUNC((SUM(ADJUSTED_OS_BAL_AMT_SECURITIZATION) * r.reduction_rate), 2) AS TOT_SECRTZTN_AMT_TO_ADJUST
        FROM {UPSTREAM_ASSET[0]} a
        INNER JOIN {UPSTREAM_ASSET[2]} b ON 
            b.ACCOUNT_NUMBER = a.ACCOUNT_NUMBER
            AND a.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
        INNER JOIN {UPSTREAM_ASSET[1]} r ON
            b.EFFECTIVE_DATE = r.EFFECTIVE_DATE
        WHERE b.EFFECTIVE_DATE = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
        GROUP BY r.reduction_rate
    """
):
    pass

def export_cc(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
        SELECT
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            'CC' AS SECRTZTN_TP_CD,
            TRUNC((t.SECURITIZED_AMOUNT::DECIMAL(32,2)*cc.RETAIL_PERCENTAGE::DECIMAL(32,10))::DECIMAL(38,5), 2) AS TOT_SECRTZTN_AMT_TO_ADJUST
        FROM {UPSTREAM_ASSET[3]} t
        LEFT JOIN {UPSTREAM_ASSET[4]} cc ON
            t.EFFECTIVE_DATE = cc.EFFECTIVE_DATE
        WHERE t.EFFECTIVE_DATE = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass

def export_cl(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
        SELECT
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            'CL' AS SECRTZTN_TP_CD,
            TRUNC((t.SECURITIZED_AMOUNT::DECIMAL(32,2)*cl.RETAIL_PERCENTAGE::DECIMAL(32,10))::DECIMAL(38,5), 2) AS TOT_SECRTZTN_AMT_TO_ADJUST
        FROM {UPSTREAM_ASSET[5]} t
        LEFT JOIN {UPSTREAM_ASSET[6]} cl ON
            t.EFFECTIVE_DATE = cl.EFFECTIVE_DATE   
        WHERE t.EFFECTIVE_DATE = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass


def duckdb_clear_tot_secrtztn_amt_to_adjust(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
        DELETE FROM { DOWNSTREAM_ASSET } 
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass


def duckdb_derive_tot_secrtztn_amt_to_adjust(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
        INSERT INTO { DOWNSTREAM_ASSET } BY NAME
            SELECT 
                OBSN_DT,
                SECRTZTN_TP_CD,
                TOT_SECRTZTN_AMT_TO_ADJUST
            FROM read_parquet([
                '{{{{ task_instance.xcom_pull(task_ids="derived__tot_secrtztn_amt_to_adjust.export_auto", key="parquet") }}}}',
                '{{{{ task_instance.xcom_pull(task_ids="derived__tot_secrtztn_amt_to_adjust.export_cc", key="parquet") }}}}',
                '{{{{ task_instance.xcom_pull(task_ids="derived__tot_secrtztn_amt_to_adjust.export_cl", key="parquet") }}}}'
        ], union_by_name=true)
    """
):
    pass
