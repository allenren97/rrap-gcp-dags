import os
from datetime import timedelta
import pendulum

from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import _pull_asset_event_extras, _push_asset_event_extras


UPSTREAM_ASSET = ['features.REVISED_EXPSR_AMT',
                  'features.BEFORE_ZERO_NET_DRAWN_AMT',
                  'ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT',
                  'ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT',
                  'ingestion.BASELAYER_MOR',
                  'ingestion.MORT_MTH_SNAPSHOT',
                  'features.BASEL_ACCT_ID']

DOWNSTREAM_ASSET = "features.BEFORE_ZERO_NET_UNDRAWN_AMT"
DEPENDENCIES = {
	'export_ks':['duckdb_clear'],
	'export_spl':['duckdb_clear'],
	'export_mor':['duckdb_clear'],
	'export_tng':['duckdb_clear'],
    'duckdb_clear': ['duckdb_load']
}

def export_ks(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    WITH REVISED_EXPSR_AMT as (
        SELECT 
            BASEL_ACCT_ID,
            REVISED_EXPSR_AMT
        FROM {UPSTREAM_ASSET[0]}
        where OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    ),
    AF_ADJ_OS_BAL_AMT AS (
        SELECT
        BASEL_ACCT_ID,
        CAST(ROUND(BEFORE_ZERO_NET_DRAWN_AMT, 8) AS DECIMAL(38,8)) AS VADJUSTED_OS_BAL_AMT
        FROM {UPSTREAM_ASSET[1]} as main
        where main.OBSN_DT='{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    ),
    JOINED as (
        SELECT
        a.BASEL_ACCT_ID,
        VADJUSTED_OS_BAL_AMT
        FROM
        {UPSTREAM_ASSET[2]} a 
        LEFT JOIN AF_ADJ_OS_BAL_AMT b 
        on a.BASEL_ACCT_ID=b.BASEL_ACCT_ID
        WHERE mth_tm_id={{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    )
        SELECT 
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        T1.BASEL_ACCT_ID,
        CASE WHEN
            (COALESCE(REVISED_EXPSR_AMT, 0) - COALESCE(VADJUSTED_OS_BAL_AMT, 0) < 0) AND (COALESCE(REVISED_EXPSR_AMT, 0) - COALESCE(VADJUSTED_OS_BAL_AMT, 0)) IS NOT NULL THEN 0
            ELSE ROUND((COALESCE(REVISED_EXPSR_AMT, 0) - COALESCE(VADJUSTED_OS_BAL_AMT, 0)), 4)
        END AS BEFORE_ZERO_NET_UNDRAWN_AMT
        FROM JOINED T1
        JOIN REVISED_EXPSR_AMT T2
            ON T1.BASEL_ACCT_ID=T2.BASEL_ACCT_ID
        GROUP BY T1.BASEL_ACCT_ID, REVISED_EXPSR_AMT, VADJUSTED_OS_BAL_AMT
    """
):
    pass

def export_spl(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    SELECT
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        BASEL_ACCT_ID,
        0 AS BEFORE_ZERO_NET_UNDRAWN_AMT
    FROM {UPSTREAM_ASSET[3]}
    WHERE MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    """
):
    pass

def export_mor(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    SELECT
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        mor.BASEL_ACCT_ID,
        BF_ZERO_NET_UNDRAWN_AMT AS BEFORE_ZERO_NET_UNDRAWN_AMT
    FROM {UPSTREAM_ASSET[5]} mor
    LEFT JOIN {UPSTREAM_ASSET[4]} base ON
        mor.MORT_NUM = base.MORT_NUM
        AND mor.MTH_END_DT = base.MTH_END_DT
    WHERE mor.MTH_END_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass

def export_tng(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    SELECT
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        BASEL_ACCT_ID,
        0 AS BEFORE_ZERO_NET_UNDRAWN_AMT
    FROM {UPSTREAM_ASSET[6]} acct
    WHERE 
        acct.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
        AND acct.SRC_SYS_CD = 'TNG-MOR'
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
                BEFORE_ZERO_NET_UNDRAWN_AMT            
            FROM read_parquet([
            '{{{{ task_instance.xcom_pull(task_ids="derived__before_zero_net_undrawn_amt.export_ks", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__before_zero_net_undrawn_amt.export_spl", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__before_zero_net_undrawn_amt.export_mor", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__before_zero_net_undrawn_amt.export_tng", key="parquet") }}}}'], union_by_name=True)
    )
    """
):
    pass
