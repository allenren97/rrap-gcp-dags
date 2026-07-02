import os
from datetime import timedelta
import pendulum

from airflow.sdk import get_current_context, Param

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)


UPSTREAM_ASSET = [
    'ingestion.AIRB_RECON_APRVD_SNAPSHOT',#0
    'features.OS_BAL_AMT_V2',#1
    'features.TRNST_EXCLSN_F',#2
    'features.TREATMENT_F',#3
    'features.PIT_STATUS_CROSS_DEFAULT_ORIG',#4     
    'ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT',#5
    'ingestion.TM_DIM',#6
    'ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT',#7
    'features.TOT_NEW_BAL_AMT',#8
    'features.GENL_LEDGER_BALCNG_ADJ_AMT'#9
    
]
DOWNSTREAM_ASSET = "features.AF_ADJ_OS_BAL_AMT"
DEPENDENCIES = {
    "duckdb_delete": ["export_spl","export_ks"],
    "export_ks": ["duckdb_load"],
    "export_spl":["duckdb_load"]
}

def duckdb_delete(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    DELETE FROM { DOWNSTREAM_ASSET } 
    WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass

def export_spl(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    
        WITH 
        e_filt AS(
        SELECT
            o.basel_acct_id,
            o.OS_BAL_AMT_V2,
            t.TREATMENT_F,
            f.TRNST_EXCLSN_F,
            o.OBSN_DT
        FROM {UPSTREAM_ASSET[1]} o
        JOIN {UPSTREAM_ASSET[3]} t
        ON o.OBSN_DT = t.OBSN_DT
        AND o.BASEL_ACCT_ID = t.BASEL_ACCT_ID
        JOIN {UPSTREAM_ASSET[2]} f
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
        JOIN {UPSTREAM_ASSET[5]} spl
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
        JOIN {UPSTREAM_ASSET[0]} a
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
        )
        SELECT
                spl.OBSN_DT,
                spl.BASEL_ACCT_ID,
                CAST(ROUND(ROUND(ABS(spl.OS_BAL_AMT_V2)/gl.GL_TOT_CRNT_BAL_SUM, 6) * gl.Adj_Amt + spl.OS_BAL_AMT_V2, 3) as decimal(17,3))
                    AS AF_ADJ_OS_BAL_AMT,
                spl.SRC_SYS_CD
            FROM TOTAL_SPL_by_GL gl
        JOIN basel_filtered spl
            ON gl.gl_acct_num = spl.gl_acct_num
            AND gl.gl_trnst_num = spl.gl_trnst_num
            WHERE
                spl.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate")  }}}}'
                AND spl.MTH_TM_ID= {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id")  }}}}
                AND spl.BASEL_ACCT_ID NOT IN (
                    SELECT spl.basel_acct_id 
                        FROM {UPSTREAM_ASSET[5]} spl
                    JOIN {UPSTREAM_ASSET[0]} airb ON
                            spl.GL_ACCT_NUM = airb.GL_ACCT_NUM
                            AND spl.GL_TRNST_NUM = airb.GL_TRNST_NUM
                    JOIN {UPSTREAM_ASSET[6]} dim ON
                            spl.MTH_TM_ID = dim.TM_ID
                        WHERE 
                            spl.MTH_TM_ID={{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id")  }}}}
                            AND TRIM(dim.TM_LVL)='Month'
                            AND TRIM(spl.TRNST_EXCLSN_F) = 'N'
                            AND TRIM(spl.PIT_STATUS_CROSS_DEFAULT_ORIG) IN ('CUR','DEF')
                            AND TRIM(spl.COMM_LOAN_CD) =  '1'    
                            AND TRIM(spl.SCRTY_CD) <>'99'               
                            AND dim.TM_LVL_END_DT= '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate")  }}}}'
                )

"""
):
    pass


def export_ks(
    duckdb_conn_id='duckdb-conn',
    sql=f"""

		WITH GENL_LEDGER_BALCNG_ADJ_AMT AS 
        (
			SELECT
            BASEL_ACCT_ID,
            GENL_LEDGER_BALCNG_ADJ_AMT
            FROM
            {UPSTREAM_ASSET[9]}
            where OBSN_DT='{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
            and TRIM(SRC_SYS_CD) = 'KS'

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
					COALESCE(T2.TOT_NEW_BAL_AMT, 0)
				+ COALESCE(T1.GENL_LEDGER_BALCNG_ADJ_AMT, 0)
					AS AF_ADJ_OS_BAL_AMT
				FROM GENL_LEDGER_BALCNG_ADJ_AMT T1
				JOIN TOT_NEW_BAL_AMT T2
				ON T1.BASEL_ACCT_ID = T2.BASEL_ACCT_ID
			)
		select
		BASEL_ACCT_ID, 
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        'KS' AS SRC_SYS_CD ,
		AF_ADJ_OS_BAL_AMT
		from final
"""
):
    pass



def duckdb_load(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} BY NAME
    FROM (
     
            SELECT OBSN_DT, 
            BASEL_ACCT_ID, 
            AF_ADJ_OS_BAL_AMT,
            SRC_SYS_CD
            FROM read_parquet('{{{{ task_instance.xcom_pull(task_ids="derived__af_adj_os_bal_amt.export_ks", key="parquet") }}}}')
            UNION 
            SELECT OBSN_DT, 
            BASEL_ACCT_ID, 
            AF_ADJ_OS_BAL_AMT,
            SRC_SYS_CD
            FROM read_parquet('{{{{ task_instance.xcom_pull(task_ids="derived__af_adj_os_bal_amt.export_spl", key="parquet") }}}}')
    

    )
    """
):
    pass