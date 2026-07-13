import os
from datetime import timedelta
import pendulum

from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import _pull_asset_event_extras, _push_asset_event_extras


UPSTREAM_ASSET = ['ingestion.SPL_SOURCE_FILE_ACCOUNTS',
                  'ingestion.CC_SOURCE_FILE_ACCOUNTS',
                  'ingestion.CL_SOURCE_FILE_ACCOUNTS',
                  'ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT',
                  'ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT']

DOWNSTREAM_ASSET = "features.SECURITIZATION_DATE"

DEPENDENCIES = {
    'export_auto': ['duckdb_clear_securitization_date'],
    'export_cc': ['duckdb_clear_securitization_date'],
    'export_cl': ['duckdb_clear_securitization_date'],
    'duckdb_clear_securitization_date': ['duckdb_derive_securitization_date'],
}

def export_auto(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
        SELECT
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate")  }}}}' AS OBSN_DT,
            spl.BASEL_ACCT_ID,
            'AUTO' AS SECRTZTN_TP_CD,
            LAST_DAY(CAST(strptime(SECURITIZATION_DATE::VARCHAR, '%Y%m') AS DATE)) AS SECURITIZATION_DATE
        FROM {UPSTREAM_ASSET[0]} auto
        LEFT JOIN {UPSTREAM_ASSET[3]} spl ON
            auto.ACCOUNT_NUMBER = CAST(TRIM(spl.CRNT_BR_LOCTN_TRNST) || TRIM(LOAN_NUM) AS BIGINT)
        WHERE spl.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id")}}}}
    """
):
    pass

def export_cc(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
        SELECT
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate")  }}}}' AS OBSN_DT,
            ks.BASEL_ACCT_ID,
            'CC' AS SECRTZTN_TP_CD,
            EFFECTIVE_DATE AS SECURITIZATION_DATE
        FROM {UPSTREAM_ASSET[1]} cc
        LEFT JOIN {UPSTREAM_ASSET[4]} ks ON
            cc.VISA_ACCT_NUM = CAST(LTRIM(TRIM(ks.ACCT_NUM), '0') AS BIGINT)
        WHERE ks.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id")}}}}

    """
):
    pass

def export_cl(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
        SELECT
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate")  }}}}' AS OBSN_DT,
            ks.BASEL_ACCT_ID,
            'CL' AS SECRTZTN_TP_CD,
            EFFECTIVE_DATE AS SECURITIZATION_DATE
        FROM {UPSTREAM_ASSET[2]} cl
        LEFT JOIN {UPSTREAM_ASSET[4]} ks ON
            cl.VISA_ACCT_NUM = CAST(LTRIM(TRIM(ks.ACCT_NUM), '0') AS BIGINT)
        WHERE ks.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id")}}}}

    """
):
    pass

def duckdb_clear_securitization_date(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    DELETE FROM { DOWNSTREAM_ASSET } 
    WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass

def duckdb_derive_securitization_date(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} (
        SELECT 
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate")  }}}}' AS OBSN_DT,
            BASEL_ACCT_ID,
            SECRTZTN_TP_CD,
            SECURITIZATION_DATE
        FROM read_parquet([
            '{{{{ task_instance.xcom_pull(task_ids="derived__securitization_date.export_auto", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__securitization_date.export_cc", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__securitization_date.export_cl", key="parquet") }}}}'
        ], union_by_name=true)
    )
    """
):
    pass
