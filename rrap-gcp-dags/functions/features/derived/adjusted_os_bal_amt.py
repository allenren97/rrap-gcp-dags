import pyarrow as pa
from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

UPSTREAM_ASSET = ['features.AF_ADJ_OS_BAL_AMT',
                  'ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT',
                  'features.BASEL_ACCT_ID_CCAR_MATCHED',
                  'features.SECRTZTN_OS_ADJ_FACTR',
                  'ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT',
                  'features.TRNST_EXCLSN_F',
                  'features.OS_BAL_AMT_V2',
                  'features.PIT_STATUS_CROSS_DEFAULT_ORIG',
                  'features.TREATMENT_F',
                  'features.PRD_ID',
                  'ingestion.MORT_MTH_SNAPSHOT',
                  'features.GENL_LEDGER_BALCNG_ADJ_AMT',
                  'ingestion.TNG_ACCT_MO',
                  'ingestion.BASEL_ACCT_DIM',
                  'features.TOT_NEW_BAL_AMT']

DOWNSTREAM_ASSET = 'features.ADJUSTED_OS_BAL_AMT'

DEPENDENCIES = {
    'export_ks': ['duckdb_clear_adjusted_os_bal'],
    'export_spl': ['duckdb_clear_adjusted_os_bal'],
    'export_mor': ['duckdb_clear_adjusted_os_bal'],
    'export_tng': ['duckdb_clear_adjusted_os_bal'],
    'duckdb_clear_adjusted_os_bal': ['duckdb_load_adjusted_os_bal']
}

def export_ks(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
    WITH GENL_LEDGER_BALCNG_ADJ_AMT AS 
        (
			SELECT
            BASEL_ACCT_ID,
            GENL_LEDGER_BALCNG_ADJ_AMT
            FROM
            {UPSTREAM_ASSET[11]}
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
            {UPSTREAM_ASSET[14]}
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
        
        af_adj_os_bal_amt AS(
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
        CASE 
            WHEN ccar_matched.BASEL_ACCT_ID_CCAR_MATCHED IS NOT NULL AND ccar_matched.BASEL_ACCT_ID_CCAR_MATCHED::VARCHAR <> '' AND MTH_TM_ID>=16516
            THEN ROUND(ij.AF_ADJ_OS_BAL_AMT*(1-factr.SECRTZTN_OS_ADJ_FACTR), 8)
            ELSE ROUND(ij.AF_ADJ_OS_BAL_AMT, 8)
        END AS ADJUSTED_OS_BAL_AMT
    FROM {UPSTREAM_ASSET[1]} ks
    LEFT JOIN {UPSTREAM_ASSET[2]} ccar_matched ON
        ks.BASEL_ACCT_ID = ccar_matched.BASEL_ACCT_ID_CCAR_MATCHED
        AND ccar_matched.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    LEFT JOIN {UPSTREAM_ASSET[3]} factr ON
        ccar_matched.SECRTZTN_TP_CD = factr.SECRTZTN_TP_CD
        AND ccar_matched.OBSN_DT = factr.OBSN_DT
    LEFT JOIN af_adj_os_bal_amt ij ON
        ks.BASEL_ACCT_ID = ij.BASEL_ACCT_ID
        AND ij.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    WHERE 
        ks.MTH_TM_ID={{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id")  }}}}
    """
):
    pass

def export_spl(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
    WITH updated_os_bal AS(
        SELECT
            spl.BASEL_ACCT_ID,
            CASE 
                WHEN TRIM(pit.PIT_STATUS_CROSS_DEFAULT_ORIG) = 'CUR' 
                    AND TRIM(treatm.TREATMENT_F) = 'A' 
                    AND TRIM(exclsn.TRNST_EXCLSN_F) = 'N' 
                    AND TRIM(prd.PRD_ID) IN ('S09', 'S10')
                THEN af.AF_ADJ_OS_BAL_AMT 
                ELSE COALESCE(bal.OS_BAL_AMT_V2, 0)
            END AS UPDATED_OS_BAL_AMT
        FROM {UPSTREAM_ASSET[4]} spl
        LEFT JOIN (SELECT * FROM {UPSTREAM_ASSET[5]} WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}') exclsn ON
            spl.BASEL_ACCT_ID = exclsn.BASEL_ACCT_ID
        LEFT JOIN (SELECT * FROM {UPSTREAM_ASSET[6]} WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}') bal ON
            spl.BASEL_ACCT_ID = bal.BASEL_ACCT_ID
        LEFT JOIN (SELECT * FROM {UPSTREAM_ASSET[7]} WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}') pit ON
            spl.BASEL_ACCT_ID = pit.BASEL_ACCT_ID
        LEFT JOIN (SELECT * FROM {UPSTREAM_ASSET[8]} WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}') treatm ON
            spl.BASEL_ACCT_ID = treatm.BASEL_ACCT_ID
        LEFT JOIN (SELECT * FROM {UPSTREAM_ASSET[9]} WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}') prd ON
            spl.BASEL_ACCT_ID = prd.BASEL_ACCT_ID
        LEFT JOIN {UPSTREAM_ASSET[0]} af ON
            spl.BASEL_ACCT_ID = af.BASEL_ACCT_ID
            AND af.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
        WHERE 
            spl.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id")  }}}}
        )

        SELECT
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            'SPL' AS SRC_SYS_CD,
            spl.BASEL_ACCT_ID,
            CASE
                WHEN ccar_match.BASEL_ACCT_ID_CCAR_MATCHED IS NOT NULL
                THEN sec_adj.UPDATED_OS_BAL_AMT*(1-os_adj.SECRTZTN_OS_ADJ_FACTR)
                ELSE sec_adj.UPDATED_OS_BAL_AMT
            END AS ADJUSTED_OS_BAL_AMT
        FROM {UPSTREAM_ASSET[4]} spl
        LEFT JOIN {UPSTREAM_ASSET[2]} ccar_match ON
            spl.BASEL_ACCT_ID = ccar_match.BASEL_ACCT_ID_CCAR_MATCHED
            AND ccar_match.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
        LEFT JOIN updated_os_bal sec_adj ON
            spl.BASEL_ACCT_ID = sec_adj.BASEL_ACCT_ID
        LEFT JOIN {UPSTREAM_ASSET[3]} os_adj ON
            ccar_match.SECRTZTN_TP_CD = os_adj.SECRTZTN_TP_CD
            AND os_adj.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
            AND os_adj.SECRTZTN_TP_CD = 'AUTO'
        WHERE spl.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id")  }}}}
    """
):
    pass

def export_mor(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
    SELECT
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        mor.BASEL_ACCT_ID,
        'MOR' AS SRC_SYS_CD,
        CASE
            WHEN TRIM(pit.PIT_STATUS_CROSS_DEFAULT_ORIG) ='DEF' 
            AND TRIM(PD_OFF_F) ='Y' 
            AND TOT_SUSP_BAL_AMT < 0  
            THEN (-1*TOT_SUSP_BAL_AMT)
        ELSE CAST(mor.CRNT_BAL_AMT + genl.GENL_LEDGER_BALCNG_ADJ_AMT AS DECIMAL(38,8))
    END AS ADJUSTED_OS_BAL_AMT
    FROM {UPSTREAM_ASSET[10]} mor
    LEFT JOIN (SELECT * FROM {UPSTREAM_ASSET[7]} WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}') pit ON
        mor.BASEL_ACCT_ID = pit.BASEL_ACCT_ID
    LEFT JOIN (SELECT * FROM {UPSTREAM_ASSET[11]} WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}') genl ON
        mor.BASEL_ACCT_ID = genl.BASEL_ACCT_ID
    WHERE mor.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id")  }}}}
    """
):
    pass

def export_tng(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
    SELECT
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        dim.BASEL_ACCT_ID,
        'TNG' AS SRC_SYS_CD,
        END_PRINCIPAL_BALANCE::DECIMAL(38,8) AS ADJUSTED_OS_BAL_AMT
    FROM {UPSTREAM_ASSET[12]} tng
    INNER JOIN {UPSTREAM_ASSET[13]} dim ON
        dim.SRC_APP_CD = 'TNG-MOR'
        AND dim.SRC_SYS_DEL_F != 'Y'
        AND trim(tng.ACCOUNT_ID) = TRIM(dim.SRC_APP_ID)
    WHERE tng.MONTH_END_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass

def duckdb_clear_adjusted_os_bal(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
        DELETE FROM { DOWNSTREAM_ASSET } 
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass

def duckdb_load_adjusted_os_bal(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} by name
    FROM (
        SELECT
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            BASEL_ACCT_ID,
            SRC_SYS_CD,
            ADJUSTED_OS_BAL_AMT
        FROM read_parquet([
            '{{{{ task_instance.xcom_pull(task_ids="derived__adjusted_os_bal_amt.export_ks", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__adjusted_os_bal_amt.export_spl", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__adjusted_os_bal_amt.export_mor", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__adjusted_os_bal_amt.export_tng", key="parquet") }}}}'
        ], union_by_name=true)
    )
    """,
):
    pass