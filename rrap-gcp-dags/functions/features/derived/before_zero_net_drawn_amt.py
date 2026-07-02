import os
from datetime import timedelta
import pendulum

from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import _pull_asset_event_extras, _push_asset_event_extras


UPSTREAM_ASSET = ['features.GENL_LEDGER_BALCNG_ADJ_AMT',
                  'ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT',
                  'ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT',
                  'ingestion.BASELAYER_MOR',
                  'ingestion.MORT_MTH_SNAPSHOT',
                  'features.ADJUSTED_OS_BAL_AMT_SECURITIZATION',
                  'ingestion.TNG_ACCT_MO',
                  'ingestion.BASEL_ACCT_DIM',
                  'features.TOT_NEW_BAL_AMT']

DOWNSTREAM_ASSET = "features.BEFORE_ZERO_NET_DRAWN_AMT"
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
    WITH GENL_LEDGER_BALCNG_ADJ_AMT AS 
        (
			SELECT
            BASEL_ACCT_ID,
            GENL_LEDGER_BALCNG_ADJ_AMT
            FROM
            {UPSTREAM_ASSET[0]}
            where OBSN_DT='{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
            and TRIM(SRC_SYS_CD) = 'KS'
            AND GENL_LEDGER_BALCNG_ADJ_AMT IS NOT NULL
        ),

		TOT_NEW_BAL_AMT as 
			(
            SELECT
            BASEL_ACCT_ID,
            TOT_NEW_BAL_AMT
            FROM 
            {UPSTREAM_ASSET[8]}
            where OBSN_DT='{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
			),
            
		final AS (
            SELECT
            T2.BASEL_ACCT_ID,
                CAST(ROUND(COALESCE(T2.TOT_NEW_BAL_AMT, 0)
            + COALESCE(T1.GENL_LEDGER_BALCNG_ADJ_AMT, 0), 8) AS DECIMAL(38,8))
                AS AF_ADJ_OS_BAL_AMT
            FROM GENL_LEDGER_BALCNG_ADJ_AMT T1
            JOIN TOT_NEW_BAL_AMT T2
            ON T1.BASEL_ACCT_ID = T2.BASEL_ACCT_ID
            ),
        
        AF_ADJ_OS_BAL_AMT AS(
            select
            BASEL_ACCT_ID, 
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            'KS' AS SRC_SYS_CD,
            AF_ADJ_OS_BAL_AMT
            from final
    )
    SELECT
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        ks.BASEL_ACCT_ID,
        'KS' AS SRC_SYS_CD,
        CAST(AF_ADJ_OS_BAL_AMT AS DECIMAL(17,3)) AS BEFORE_ZERO_NET_DRAWN_AMT 
        FROM {UPSTREAM_ASSET[1]} ks
        LEFT JOIN AF_ADJ_OS_BAL_AMT af ON
            ks.BASEL_ACCT_ID = af.BASEL_ACCT_ID
        WHERE ks.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    """
):
    pass

def export_spl(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    SELECT
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        spl.BASEL_ACCT_ID,
        'SPL' AS SRC_SYS_CD,
        ADJUSTED_OS_BAL_AMT_SECURITIZATION AS BEFORE_ZERO_NET_DRAWN_AMT
    FROM {UPSTREAM_ASSET[2]} spl
    LEFT JOIN {UPSTREAM_ASSET[5]} adj ON
        spl.BASEL_ACCT_ID = adj.BASEL_ACCT_ID
        AND adj.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
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
        'MOR' AS SRC_SYS_CD,
        BF_ZERO_NET_DRAWN_AMT AS BEFORE_ZERO_NET_DRAWN_AMT
    FROM {UPSTREAM_ASSET[4]} mor
    LEFT JOIN {UPSTREAM_ASSET[3]} base ON
        mor.MORT_NUM = base.MORT_NUM
        AND base.MTH_END_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    WHERE mor.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    """
):
    pass

def export_tng(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    SELECT
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        dim.BASEL_ACCT_ID,
        'TNG-MOR' AS SRC_SYS_CD,
        END_PRINCIPAL_BALANCE AS BEFORE_ZERO_NET_DRAWN_AMT
    FROM {UPSTREAM_ASSET[6]} tng
    INNER JOIN {UPSTREAM_ASSET[7]} dim ON
        dim.SRC_APP_CD = 'TNG-MOR'
        AND dim.SRC_SYS_DEL_F != 'Y'
        AND TRIM(tng.ACCOUNT_ID) = TRIM(dim.SRC_APP_ID)
    WHERE tng.MONTH_END_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
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
                SRC_SYS_CD,
                BEFORE_ZERO_NET_DRAWN_AMT            
            FROM read_parquet([
            '{{{{ task_instance.xcom_pull(task_ids="derived__before_zero_net_drawn_amt.export_ks", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__before_zero_net_drawn_amt.export_spl", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__before_zero_net_drawn_amt.export_mor", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__before_zero_net_drawn_amt.export_tng", key="parquet") }}}}',], union_by_name=True)
    )
    """
):
    pass
