import os
from datetime import timedelta
import pendulum

from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import _pull_asset_event_extras, _push_asset_event_extras


UPSTREAM_ASSET = ['features.BASEL_ACCT_ID_CCAR_MATCHED',
                  'ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT',
                  'features.AF_ADJ_OS_BAL_AMT',
                  'features.SECRTZTN_OS_ADJ_FACTR',
                  'features.BEFORE_ZERO_NET_DRAWN_AMT',
                  'features.SRC_SYS_CD',
                  'ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT',
                  'features.ADJUSTED_OS_BAL_AMT_SECURITIZATION']

DOWNSTREAM_ASSET = "features.DELTA_FOR_DRAWN_AMT"
DEPENDENCIES = {
	'export_ks':['duckdb_clear'],
	'export_spl':['duckdb_clear'],
    'duckdb_clear': ['duckdb_load']
}

def export_ks(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    WITH 
		adjusted_os_bal_ks AS (
            SELECT
                ks.BASEL_ACCT_ID,
                ks.MTH_TM_ID,
                ccar_matched.SECRTZTN_TP_CD,
                CASE 
                    WHEN ccar_matched.BASEL_ACCT_ID_CCAR_MATCHED IS NOT NULL AND ks.MTH_TM_ID>=16516
                    THEN ROUND(af.AF_ADJ_OS_BAL_AMT*(1-factr.SECRTZTN_OS_ADJ_FACTR), 8)
                    ELSE ROUND(af.AF_ADJ_OS_BAL_AMT, 8)
                END AS ADJUSTED_OS_BAL_AMT
            FROM {UPSTREAM_ASSET[0]} ccar_matched
            LEFT JOIN {UPSTREAM_ASSET[1]} ks ON
                ks.BASEL_ACCT_ID = ccar_matched.BASEL_ACCT_ID_CCAR_MATCHED
            LEFT JOIN (SELECT * FROM {UPSTREAM_ASSET[2]} WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}') af ON
                ks.BASEL_ACCT_ID = af.BASEL_ACCT_ID
            LEFT JOIN (SELECT * FROM {UPSTREAM_ASSET[3]} WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}') factr ON
                ccar_matched.SECRTZTN_TP_CD = factr.SECRTZTN_TP_CD
            WHERE ks.MTH_TM_ID = 20836
            AND ccar_matched.SECRTZTN_TP_CD = 'CC'
            AND factr.SECRTZTN_TP_CD = 'CC'
            AND ADJUSTED_OS_BAL_AMT IS NOT NULL
        )

        SELECT
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            adj.BASEL_ACCT_ID,
            'CC' AS SECRTZTN_TP_CD,
            SUM(bf.BEFORE_ZERO_NET_DRAWN_AMT)-SUM(adj.ADJUSTED_OS_BAL_AMT) AS DELTA_FOR_DRAWN_AMT 
        FROM {UPSTREAM_ASSET[4]} bf
        LEFT JOIN adjusted_os_bal_ks adj ON
            bf.BASEL_ACCT_ID = adj.BASEL_ACCT_ID
        LEFT JOIN (SELECT * FROM {UPSTREAM_ASSET[5]} WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}') cd ON
            bf.BASEL_ACCT_ID = cd.BASEL_ACCT_ID
            AND TRIM(cd.SRC_SYS_CD) = 'KS'
        WHERE adj.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
        GROUP BY
            adj.BASEL_ACCT_ID
    """
):
    pass

def export_spl(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    SELECT 
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        spl.BASEL_ACCT_ID,
        'AUTO' AS SECRTZTN_TP_CD,
        SUM(bf.BEFORE_ZERO_NET_DRAWN_AMT)-SUM(adj.ADJUSTED_OS_BAL_AMT_SECURITIZATION) AS DELTA_FOR_DRAWN_AMT 
    FROM {UPSTREAM_ASSET[6]} spl 
    LEFT JOIN {UPSTREAM_ASSET[4]} bf ON
        spl.BASEL_ACCT_ID = bf.BASEL_ACCT_ID
    LEFT JOIN (SELECT * FROM {UPSTREAM_ASSET[7]} WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}') adj ON
        spl.BASEL_ACCT_ID = adj.BASEL_ACCT_ID
    LEFT JOIN (SELECT * FROM {UPSTREAM_ASSET[5]} WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}') cd ON
        spl.BASEL_ACCT_ID = cd.BASEL_ACCT_ID
        AND TRIM(cd.SRC_SYS_CD) = 'SPL'
    WHERE spl.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    GROUP BY
        spl.BASEL_ACCT_ID
    """
):
    pass

def duckdb_clear(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    DELETE FROM { DOWNSTREAM_ASSET } 
    WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass


def duckdb_load(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} BY NAME
    FROM (
            SELECT 
                OBSN_DT, 
                BASEL_ACCT_ID,
                SECRTZTN_TP_CD,
                DELTA_FOR_DRAWN_AMT            
            FROM read_parquet([
            '{{{{ task_instance.xcom_pull(task_ids="derived__delta_for_drawn_amt.export_ks", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__delta_for_drawn_amt.export_spl", key="parquet") }}}}'], union_by_name=True)
    )
    """
):
    pass
