import os
from datetime import timedelta
import pendulum

from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import _pull_asset_event_extras, _push_asset_event_extras


UPSTREAM_ASSET = [ 'ingestion.AIRB_MORT_MTH_SNAPSHOT','features.INDEX_TERANETV']
DOWNSTREAM_ASSET = "features.LTV"
DEPENDENCIES = {
    'duckdb_clear_derive_ltv': ['duckdb_derive_ltv'],
}


def duckdb_clear_derive_ltv(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    DELETE FROM { DOWNSTREAM_ASSET } 
    WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass

def duckdb_derive_ltv(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} 
    WITH BASE AS (
                SELECT
                CRNT_BAL,
                INEREST_ACCR_AMT,
                TOT_SUSP_BAL,
                MORT_NUM
                from {UPSTREAM_ASSET[0]}
                where tm_id = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
            )
            ,CURRENT_BAL AS (
                SELECT 
                MORT_NUM,
                CASE
                    WHEN CRNT_BAL IS NULL THEN 0 
                    ELSE CRNT_BAL 
                END AS CURRENT_BAL,
               INEREST_ACCR_AMT,
               TOT_SUSP_BAL
                FROM 
                BASE
            ),TOTAL_BAL AS(
                SELECT 
                MORT_NUM, 
                MAX(GREATEST(CURRENT_BAL + INEREST_ACCR_AMT, -TOT_SUSP_BAL)) AS TOTAL_BAL
                FROM CURRENT_BAL
                GROUP BY MORT_NUM
            ), LTV AS(
                SELECT 
                    a.MORT_NUM,
                    CASE 
                        WHEN b.index_teranetv != 0 THEN 
                            a.TOTAL_BAL / b.index_teranetv
                        ELSE NULL
                    END AS LTV
                FROM 
                    TOTAL_BAL a
                LEFT JOIN 
                    {UPSTREAM_ASSET[1]} b
                ON 
                a.MORT_NUM = b.MORT_NUM 
                WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
                GROUP BY 
                    a.MORT_NUM, LTV
            ) 
            SELECT
            MORT_NUM,
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            LTV 
            from LTV
    """

):
    pass

