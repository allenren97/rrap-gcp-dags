from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)


UPSTREAM_ASSET = [
    "ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT",
    "ingestion.MORT_MTH_SNAPSHOT",
    "ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT",
    "features.COMM_TP_CD",
    "reference.SRC_PRD_LKP",
    "reference.TRNST_EXCLSN_LKP",
    "ingestion.BASEL_ACCT_DIM",
    "ingestion.TNG_ACCT_MO",
    "features.PD_OFF_DT",
    "features.OS_BAL_AMT_IF",
    "features.TREATMENT_F"
]
DOWNSTREAM_ASSET = "features.CONSM_PRD_TREATMNT_CD_IF"
DEPENDENCIES = {
    "export_ks": ["duckdb_delete"],
    "export_mor": ["duckdb_delete"],
    "export_spl": ["duckdb_delete"],
    "export_tng": ["duckdb_delete"],
    "duckdb_delete": ["duckdb_load"],
}
SRC_PRD_LKP = "reference.SRC_PRD_LKP"

def export_ks(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        SELECT
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            BASEL_ACCT_ID,
            CASE
                WHEN (
                    CR_LMT_AMT <= 0
                    AND TOT_NEW_BAL_AMT <= 0
                ) THEN 'Z'
                WHEN TOT_NEW_BAL_AMT <= 0
                AND (
                    SUBSTR (TRIM(BLOCK_RECL_CD), 1, 1) = 'V'
                    OR TRIM(BLOCK_RECL_CD) = 'FX'
                ) THEN 'Z'
                ELSE TRIM(CONSM_PRD_TREATMNT_CD)
            END AS CONSM_PRD_TREATMNT_CD_IF,
            'KS' as SRC_SYS_CD
        FROM
            (
                SELECT
                    BASEL_ACCT_ID,
                    PRIM_BASEL_CUST_ID AS BASEL_CUST_ID,
                    CR_LMT_AMT,
                    TOT_NEW_BAL_AMT,
                    BLOCK_RECL_CD,
                    CONSM_PRD_TREATMNT_CD
                FROM
                    {UPSTREAM_ASSET[0]} a
                    LEFT JOIN (
                        SELECT
                            SRC_PRD_CD,
                            SRC_SUB_PRD_CD,
                            CONSM_PRD_TREATMNT_CD
                        FROM
                            {SRC_PRD_LKP}
                        WHERE
                            strftime ('%Y%m', DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}') BETWEEN EFF_FROM_YR_MTH AND EFF_TO_YR_MTH
                    ) SP ON TRIM(A.PRD_CD) = TRIM(SP.SRC_PRD_CD)
                    AND TRIM(A.SUB_PRD_CD) = TRIM(SP.SRC_SUB_PRD_CD)
                WHERE
                    MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
            )   
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
            'A' AS CONSM_PRD_TREATMNT_CD_IF
        FROM {UPSTREAM_ASSET[1]} mor
        WHERE MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    """,
):
    pass


def export_spl(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    SELECT
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        a.BASEL_ACCT_ID,
        'SPL' AS SRC_SYS_CD,
        CASE 
            WHEN a.RECD_STAT_CD IN (4,5,6,7,8) THEN TREATMENT_F 
            ELSE NULL
        END AS CONSM_PRD_TREATMNT_CD_IF
    FROM {UPSTREAM_ASSET[2]} a
    LEFT JOIN {UPSTREAM_ASSET[10]} treat ON
        a.BASEL_ACCT_ID = treat.BASEL_ACCT_ID
        AND treat.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    WHERE a.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    """,
):
    pass

def export_tng(
    duckdb_conn_id = 'duckdb-conn',
    sql=f"""
    SELECT
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        dim.BASEL_ACCT_ID,
        'TNG-MOR' AS SRC_SYS_CD,
        CASE
            WHEN pd.PD_OFF_DT IS NULL AND os_bal.OS_BAL_AMT_IF > 0 THEN 'A' ELSE NULL
        END AS CONSM_PRD_TREATMNT_CD_IF
    FROM {UPSTREAM_ASSET[6]} dim
    LEFT JOIN {UPSTREAM_ASSET[7]} tng ON
        dim.SRC_APP_CD = 'TNG-MOR'
        AND dim.SRC_SYS_DEL_F != 'Y'
        AND TRIM(tng.ACCOUNT_ID) = TRIM(dim.SRC_APP_ID)
    LEFT JOIN {UPSTREAM_ASSET[8]} pd ON
        dim.BASEL_ACCT_ID = pd.BASEL_ACCT_ID
        AND pd.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    LEFT JOIN {UPSTREAM_ASSET[9]} os_bal ON
        dim.BASEL_ACCT_ID = os_bal.BASEL_ACCT_ID
        AND os_bal.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    WHERE tng.MONTH_END_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass

def duckdb_delete(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    delete from {DOWNSTREAM_ASSET} where obsn_dt = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """,
):
    pass


def duckdb_load(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} by name
    FROM (
        select * 
        from read_parquet(
        ['{{{{ task_instance.xcom_pull(task_ids="derived__consm_prd_treatmnt_cd_if.export_ks", key="parquet") }}}}',
        '{{{{ task_instance.xcom_pull(task_ids="derived__consm_prd_treatmnt_cd_if.export_mor", key="parquet") }}}}',
        '{{{{ task_instance.xcom_pull(task_ids="derived__consm_prd_treatmnt_cd_if.export_spl", key="parquet") }}}}',
        '{{{{ task_instance.xcom_pull(task_ids="derived__consm_prd_treatmnt_cd_if.export_tng", key="parquet") }}}}'
        ], union_by_name = true)
    )
    """,
):
    pass
