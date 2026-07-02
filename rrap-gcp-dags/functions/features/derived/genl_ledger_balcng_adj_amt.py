import pyarrow as pa
from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

UPSTREAM_ASSET = ['ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT',
                  'features.SML_BUS_F',
                  'features.CONSM_PRD_TREATMNT_CD',
                  'features.TRNST_EXCLSN_F',
                  'features.PIT_STATUS_CROSS_DEFAULT_ORIG',
                  'ingestion.AIRB_RECON_APRVD_SNAPSHOT',
                  'features.TREATMENT_F',
                  'ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT',
                  'ingestion.TM_DIM',
                  'ingestion.MORT_MTH_SNAPSHOT',
                  'ingestion.BASEL_ACCT_DIM',
                  'features.OS_BAL_AMT_V2',
                  'ingestion.TNG_ACCT_MO'
                  ]

DOWNSTREAM_ASSET = 'features.GENL_LEDGER_BALCNG_ADJ_AMT'

DEPENDENCIES = {
    'export_ks':['duckdb_clear_genl_ledger_balcng_adj_amt'],
    'export_spl':['duckdb_clear_genl_ledger_balcng_adj_amt'],
    'export_mor':['duckdb_clear_genl_ledger_balcng_adj_amt'],
    "export_tng": ["duckdb_clear_genl_ledger_balcng_adj_amt"],
    'duckdb_clear_genl_ledger_balcng_adj_amt': ['duckdb_derive_genl_ledger_balcng_adj_amt']
}

def export_ks(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
        WITH base AS (
            select 
                a.basel_acct_id 
            from {UPSTREAM_ASSET[0]} a
            left join (select * from {UPSTREAM_ASSET[1]} where OBSN_DT='{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}') b
                on a.basel_acct_id=b.basel_acct_id
            left join (select * from {UPSTREAM_ASSET[2]} where OBSN_DT='{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}') c
                on a.basel_acct_id = c.basel_acct_id
            left join (select * from {UPSTREAM_ASSET[3]} where OBSN_DT='{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}') d 
                on a.basel_acct_id=d.basel_acct_id
            left join (select * from {UPSTREAM_ASSET[4]} where OBSN_DT='{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}') e 
                on a.basel_acct_id=e.basel_acct_id
            WHERE 
                a.mth_tm_id = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
                AND TRIM(b.SML_BUS_F) = 'N' 
                AND TRIM(c.CONSM_PRD_TREATMNT_CD) = 'A' 
                AND TRIM(d.TRNST_EXCLSN_F) = 'N' 
                AND TRIM(e.PIT_STATUS_CROSS_DEFAULT_ORIG) IN ('CUR','DEF')
            ),
            derive_gl_sum AS (
                SELECT
                    a.gl_acct_num, a.gl_trnst_num, a.mth_end_dt, a.crncy_cd, a.AIRB_Adj_Coa_Amt AS Adj_Amt, 
                    SUM(ABS(TOT_NEW_BAL_AMT)) AS GL_TOT_NEW_BAL_SUM
                FROM 
                    {UPSTREAM_ASSET[5]} a,
                    {UPSTREAM_ASSET[0]} b,
                    base c 
                WHERE 
                    a.gl_acct_num = b.gl_acct_num
                    AND a.gl_trnst_num = b.gl_trnst_num
                    AND b.basel_acct_id = c.basel_acct_id
                    AND MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}} 
                    AND MTH_END_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
                GROUP BY a.gl_acct_num, a.gl_trnst_num, a.mth_end_dt, a.crncy_cd, a.AIRB_Adj_Coa_Amt
            ),

            genl_og AS (
            SELECT 
                b.basel_acct_id, 
                (ROUND((ABS(b.TOT_NEW_BAL_AMT) / a.GL_TOT_NEW_BAL_SUM), 6) * a.Adj_Amt)::DECIMAL(38,7) AS GENL_LEDGER_BALCNG_ADJ_AMT
            FROM 
                derive_gl_sum a ,
                {UPSTREAM_ASSET[0]} b,
                base c 
            WHERE 
                MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}      
                AND TRIM(a.GL_ACCT_NUM) = TRIM(b.GL_ACCT_NUM)
                AND TRIM(a.GL_TRNST_NUM) = TRIM(b.GL_TRNST_NUM)
                AND b.basel_acct_id = c.basel_acct_id
            )

            SELECT 
                '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
                'KS' AS SRC_SYS_CD,
                ks.BASEL_ACCT_ID,
                ROUND(genl.GENL_LEDGER_BALCNG_ADJ_AMT, 3) AS GENL_LEDGER_BALCNG_ADJ_AMT
            FROM {UPSTREAM_ASSET[0]} ks
            LEFT JOIN genl_og genl ON
                ks.BASEL_ACCT_ID = genl.BASEL_ACCT_ID
            WHERE ks.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}      

    """
):
    pass

def export_spl(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
        WITH 
        e_filt AS(
        SELECT
            o.basel_acct_id,
            o.OS_BAL_AMT_V2,
            t.TREATMENT_F,
            f.TRNST_EXCLSN_F,
            o.OBSN_DT
        FROM {UPSTREAM_ASSET[11]} o
        JOIN {UPSTREAM_ASSET[6]} t
        ON o.OBSN_DT = t.OBSN_DT
        AND o.BASEL_ACCT_ID = t.BASEL_ACCT_ID
        JOIN {UPSTREAM_ASSET[3]} f
        ON o.OBSN_DT = f.OBSN_DT
        AND o.BASEL_ACCT_ID = f.BASEL_ACCT_ID
        WHERE 
        o.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate")  }}}}'
        AND TRIM(t.TREATMENT_F) = 'A'
        AND TRIM(f.TRNST_EXCLSN_F) = 'N'
        ),

        e_pit_filter as (
            SELECT
                pit.BASEL_ACCT_ID, 
                spl.OS_BAL_AMT_V2,
                spl.TREATMENT_F,
                spl.TRNST_EXCLSN_F, 
                pit.PIT_STATUS_CROSS_DEFAULT_ORIG,
                pit.SRC_SYS_CD,
                pit.OBSN_DT  
            FROM {UPSTREAM_ASSET[4]} pit 
            JOIN e_filt spl
                ON pit.OBSN_DT = spl.OBSN_DT
                AND pit.BASEL_ACCT_ID = spl.BASEL_ACCT_ID
            WHERE 
                pit.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate")  }}}}'
                AND TRIM(SRC_SYS_CD)  = 'SPL' 
                AND TRIM(PIT_STATUS_CROSS_DEFAULT_ORIG) IN ('CUR','DEF') 
        ),
        
        basel_filtered as (
            SELECT 
                e.BASEL_ACCT_ID,
                cast(TRIM(spl.crnt_br_loctn_trnst) || TRIM(spl.loan_num) as bigint) as account_number,
                spl.GL_ACCT_NUM, 
                spl.GL_TRNST_NUM, 
                e.OS_BAL_AMT_V2,
                e.TRNST_EXCLSN_F,
                e.TREATMENT_F, 
                e.PIT_STATUS_CROSS_DEFAULT_ORIG,
                e.SRC_SYS_CD,
                e.OBSN_DT,
                spl.MTH_TM_ID
            FROM e_pit_filter e
        JOIN {UPSTREAM_ASSET[7]} spl
            ON spl.BASEL_ACCT_ID = e.BASEL_ACCT_ID
            WHERE 
            spl.mth_tm_id = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id")  }}}}
        ),

        TOTAL_SPL_by_GL as (
            SELECT
                a.gl_acct_num,
                a.gl_trnst_num,
                a.mth_end_dt,
                a.crncy_cd,
                a.AIRB_Adj_Coa_Amt AS Adj_Amt,
                SUM(ABS(spl.OS_BAL_AMT_V2)) AS GL_TOT_CRNT_BAL_SUM
            FROM  basel_filtered spl
        JOIN {UPSTREAM_ASSET[5]} a
            ON a.gl_acct_num = spl.gl_acct_num
            AND a.gl_trnst_num = spl.gl_trnst_num
            WHERE 
                TRIM(a.src_sys_cd) = 'SPL'
                AND TRIM(a.crncy_cd)   = 'CAD'
                AND a.MTH_END_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate")  }}}}'
            GROUP BY
                a.gl_acct_num,
                a.gl_trnst_num,
                a.mth_end_dt,
                a.crncy_cd,
                a.AIRB_Adj_Coa_Amt
        ),

        GENL_LEDGER_BAL_ADJ_AMT AS (
            SELECT
                spl.BASEL_ACCT_ID,
                spl.MTH_TM_ID,
                CAST(ROUND(ROUND(ABS(bal.OS_BAL_AMT_V2)/(gl.GL_TOT_CRNT_BAL_SUM),6)*gl.Adj_Amt,3) as DECIMAL(17,3)) AS GENL_LEDGER_BAL_ADJ_AMT
            FROM TOTAL_SPL_by_GL gl
            LEFT JOIN {UPSTREAM_ASSET[7]} spl ON
                TRIM(spl.GL_ACCT_NUM) = TRIM(gl.GL_ACCT_NUM)
                AND TRIM(spl.GL_TRNST_NUM) = TRIM(gl.GL_TRNST_NUM)
            LEFT JOIN {UPSTREAM_ASSET[8]} dim ON
                gl.MTH_END_DT = dim.TM_LVL_END_DT
                AND spl.MTH_TM_ID = dim.TM_ID
                AND dim.TM_LVL = 'Month'
            LEFT JOIN (SELECT * FROM {UPSTREAM_ASSET[11]} WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate")  }}}}') bal ON
                spl.BASEL_ACCT_ID = bal.BASEL_ACCT_ID
            LEFT JOIN (SELECT * FROM {UPSTREAM_ASSET[6]} WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate")  }}}}') treat ON
                spl.BASEL_ACCT_ID = treat.BASEL_ACCT_ID
            LEFT JOIN (SELECT * FROM {UPSTREAM_ASSET[4]} WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate")  }}}}') pit ON
                spl.BASEL_ACCT_ID = pit.BASEL_ACCT_ID
            LEFT JOIN (SELECT * FROM {UPSTREAM_ASSET[3]} WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate")  }}}}') trnst ON 
                spl.BASEL_ACCT_ID = trnst.BASEL_ACCT_ID
            WHERE 
                gl.GL_TOT_CRNT_BAL_SUM IS NOT NULL 
                AND spl.MTH_TM_ID={{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id")  }}}} 
                AND bal.OS_BAL_AMT_V2 IS NOT NULL
                AND TRIM(treat.TREATMENT_F) = 'A'
                AND TRIM(trnst.TRNST_EXCLSN_F) = 'N'
                AND TRIM(pit.PIT_STATUS_CROSS_DEFAULT_ORIG) IN ('CUR','DEF')
                AND TRIM(gl.CRNCY_CD) = 'CAD'
                AND spl.BASEL_ACCT_ID NOT IN (
                    SELECT spl.BASEL_ACCT_ID
                    FROM {UPSTREAM_ASSET[7]} spl
                    LEFT JOIN {UPSTREAM_ASSET[8]} dim ON
                        spl.MTH_TM_ID = dim.TM_ID
                    LEFT JOIN (SELECT * FROM {UPSTREAM_ASSET[4]} WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate")  }}}}') pit ON
                        spl.BASEL_ACCT_ID = pit.BASEL_ACCT_ID
                    LEFT JOIN (SELECT * FROM {UPSTREAM_ASSET[3]} WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate")  }}}}') trnst ON 
                        spl.BASEL_ACCT_ID = trnst.BASEL_ACCT_ID
                    WHERE 
                        dim.TM_LVL = 'Month'
                        AND TRIM(trnst.TRNST_EXCLSN_F) = 'N'
                        AND TRIM(pit.PIT_STATUS_CROSS_DEFAULT_ORIG) IN ('CUR','DEF')
                        AND TRIM(spl.COMM_LOAN_CD) = '1'
                        AND TRIM(spl.SCRTY_CD) <> '99'
                        AND dim.TM_LVL_END_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate")  }}}}'
            )
        )

        SELECT
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            spl.BASEL_ACCT_ID,
            'SPL' AS SRC_SYS_CD,
            COALESCE(genl.GENL_LEDGER_BAL_ADJ_AMT, 0) AS GENL_LEDGER_BALCNG_ADJ_AMT
        FROM {UPSTREAM_ASSET[7]} spl
        LEFT JOIN GENL_LEDGER_BAL_ADJ_AMT genl ON
            spl.BASEL_ACCT_ID = genl.BASEL_ACCT_ID
            AND spl.MTH_TM_ID = genl.MTH_TM_ID
        WHERE spl.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    """
):
    pass

def export_mor(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
        WITH gl_crnt_sums_mor AS(
        SELECT
            recon.GL_ACCT_NUM,
            recon.GL_TRNST_NUM,
            recon.MTH_END_DT,
            recon.CRNCY_CD,
            recon.AIRB_ADJ_COA_AMT AS Adj_Amt,
            'N' AS SML_BUS_F,
            SUM(CASE WHEN TRIM(pit.PIT_STATUS_CROSS_DEFAULT_ORIG) = 'CUR' then ABS(mor.CRNT_BAL_AMT) ELSE 0 END) as GL_CRNT_BAL_SUM_CUR,
            SUM(CASE WHEN TRIM(pit.PIT_STATUS_CROSS_DEFAULT_ORIG) = 'DEF' then ABS(mor.CRNT_BAL_AMT) ELSE 0 END) as GL_CRNT_BAL_SUM_DEF,
            SUM(ABS(mor.CRNT_BAL_AMT)) as GL_CRNT_BAL_SUM_ALL
        FROM {UPSTREAM_ASSET[9]} mor
        LEFT JOIN {UPSTREAM_ASSET[5]} recon ON
            TRIM(recon.GL_ACCT_NUM) = TRIM(mor.GL_ACCT_NUM)
            AND TRIM(recon.GL_TRNST_NUM) = TRIM(mor.GL_TRNST_NUM)
            AND recon.MTH_END_DT = mor.MTH_END_DT
            AND recon.SRC_SYS_CD = 'MO'
            AND TRIM(recon.CRNCY_CD) = TRIM(mor.CRNCY_CD)
        LEFT JOIN (SELECT * FROM {UPSTREAM_ASSET[4]} WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}') pit ON
            mor.BASEL_ACCT_ID = pit.BASEL_ACCT_ID
        LEFT JOIN (SELECT * FROM {UPSTREAM_ASSET[2]} WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}') consm ON
            mor.BASEL_ACCT_ID = consm.BASEL_ACCT_ID
        LEFT JOIN (SELECT * FROM {UPSTREAM_ASSET[3]} WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}') trnst ON
            mor.BASEL_ACCT_ID = trnst.BASEL_ACCT_ID
        WHERE 
            mor.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
            AND recon.MTH_END_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
            AND TRIM(consm.CONSM_PRD_TREATMNT_CD) = 'A'
            AND TRIM(pit.PIT_STATUS_CROSS_DEFAULT_ORIG) IN ('CUR', 'DEF')
            AND TRIM(SML_BUS_F) = 'N'
            AND TRIM(trnst.TRNST_EXCLSN_F) = 'N'
            AND TRIM(mor.CRNCY_CD) = 'CAD'
            AND TRIM(recon.SRC_SYS_CD) = 'MO'
            AND mor.GL_TRNST_NUM IS NOT NULL
        GROUP BY
            recon.GL_ACCT_NUM, 
            recon.GL_TRNST_NUM,
            recon.MTH_END_DT,
            recon.CRNCY_CD, 
            recon.AIRB_ADJ_COA_AMT
    ),

    genl_og AS (
    SELECT
        mor.BASEL_ACCT_ID,
        CASE
            WHEN TRIM(pit.PIT_STATUS_CROSS_DEFAULT_ORIG) = 'DEF' AND gl_sums.GL_CRNT_BAL_SUM_ALL = gl_sums.GL_CRNT_BAL_SUM_DEF AND gl_sums.GL_CRNT_BAL_SUM_CUR = 0 
            THEN ROUND(ABS(mor.CRNT_BAL_AMT)/gl_sums.GL_CRNT_BAL_SUM_DEF,6)*gl_sums.Adj_Amt
            WHEN TRIM(pit.PIT_STATUS_CROSS_DEFAULT_ORIG) = 'DEF' AND gl_sums.GL_CRNT_BAL_SUM_ALL <> gl_sums.GL_CRNT_BAL_SUM_DEF  
            THEN 0
            ELSE ROUND((ABS(mor.CRNT_BAL_AMT)/gl_sums.GL_CRNT_BAL_SUM_CUR),6)*gl_sums.Adj_Amt
        END AS GENL_LEDGER_BALCNG_ADJ_AMT
        FROM gl_crnt_sums_mor gl_sums
        LEFT JOIN {UPSTREAM_ASSET[9]} mor ON
            TRIM(gl_sums.GL_ACCT_NUM) = TRIM(mor.GL_ACCT_NUM)
            AND TRIM(gl_sums.GL_TRNST_NUM) = TRIM(mor.GL_TRNST_NUM)
            AND gl_sums.MTH_END_DT = mor.MTH_END_DT
            AND TRIM(gl_sums.CRNCY_CD) = TRIM(mor.CRNCY_CD)
        LEFT JOIN {UPSTREAM_ASSET[10]} acct_dim ON
            LPAD(mor.MORT_NUM::VARCHAR, 23, '0') = acct_dim.ACCT_NUM
        LEFT JOIN (SELECT * FROM {UPSTREAM_ASSET[4]} WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}') pit ON
            mor.BASEL_ACCT_ID = pit.BASEL_ACCT_ID
        LEFT JOIN (SELECT * FROM {UPSTREAM_ASSET[2]} WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}') consm ON
            mor.BASEL_ACCT_ID = consm.BASEL_ACCT_ID
        LEFT JOIN (SELECT * FROM {UPSTREAM_ASSET[3]} WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}') trnst ON
            mor.BASEL_ACCT_ID = trnst.BASEL_ACCT_ID
        WHERE mor.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
        AND TRIM(consm.CONSM_PRD_TREATMNT_CD) IN ('A', 'Z')
        AND TRIM(pit.PIT_STATUS_CROSS_DEFAULT_ORIG) IN ('CUR', 'DEF')
        AND TRIM(gl_sums.SML_BUS_F) = 'N'
        AND TRIM(trnst.TRNST_EXCLSN_F) = 'N'
        AND TRIM(mor.CRNCY_CD) = 'CAD'
        AND TRIM(acct_dim.SRC_APP_CD) = 'MO'
        AND mor.GL_TRNST_NUM IS NOT NULL
        AND acct_dim.SRC_SYS_DEL_DT = '9999-12-31 00:00:00'
        AND TRIM(acct_dim.SRC_SYS_DEL_F) = 'N'
        AND gl_sums.MTH_END_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    )

    SELECT
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        'MOR' AS SRC_SYS_CD,
        mor.BASEL_ACCT_ID,
        (TRUNC(genl.GENL_LEDGER_BALCNG_ADJ_AMT, 3))::DECIMAL(38,3) AS GENL_LEDGER_BALCNG_ADJ_AMT
    FROM {UPSTREAM_ASSET[9]} mor
    LEFT JOIN genl_og genl ON
        mor.BASEL_ACCT_ID = genl.BASEL_ACCT_ID
    WHERE mor.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}

    """
):
    pass

def export_tng(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
    SELECT
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        dim.BASEL_ACCT_ID,
        'TNG-MOR' AS SRC_SYS_CD,
        0.0 AS GENL_LEDGER_BALCNG_ADJ_AMT
    FROM {UPSTREAM_ASSET[10]} dim
    LEFT JOIN {UPSTREAM_ASSET[12]} tng ON
        dim.SRC_APP_CD = 'TNG-MOR'
        AND dim.SRC_SYS_DEL_F != 'Y'
        AND TRIM(tng.ACCOUNT_ID) = TRIM(dim.SRC_APP_ID)
    WHERE 
        tng.MONTH_END_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'

    """,
):
    pass

def duckdb_clear_genl_ledger_balcng_adj_amt(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
        DELETE FROM { DOWNSTREAM_ASSET } 
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass

def duckdb_derive_genl_ledger_balcng_adj_amt(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    INSERT INTO { DOWNSTREAM_ASSET } BY NAME 
        SELECT
            OBSN_DT,
            SRC_SYS_CD,
            BASEL_ACCT_ID,
            GENL_LEDGER_BALCNG_ADJ_AMT
        FROM read_parquet(
            ['{{{{ task_instance.xcom_pull(task_ids="derived__genl_ledger_balcng_adj_amt.export_ks", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__genl_ledger_balcng_adj_amt.export_spl", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__genl_ledger_balcng_adj_amt.export_mor", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__genl_ledger_balcng_adj_amt.export_tng", key="parquet") }}}}'
            ], union_by_name = true)
    """
):
    pass