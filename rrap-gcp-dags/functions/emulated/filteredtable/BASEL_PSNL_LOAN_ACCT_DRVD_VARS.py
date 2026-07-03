"""
Rewrite of J_RRII_KS10_2105_BASEL_PSNL_LOAN_ACCT_DRVD_VARS.sas only.

Builds emulated.BASEL_PSNL_LOAN_ACCT_DRVD_VARS in a single DuckDB pipeline.
"""

UPSTREAM_ASSET = [
    "ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT",
    "ingestion.BASEL_ACCT_DIM",
    "reference.TRNST_EXCLSN_LKP",
]

DOWNSTREAM_ASSET = "emulated.BASEL_PSNL_LOAN_ACCT_DRVD_VARS"

DEPENDENCIES = {
    "duckdb_delete": ["duckdb_load"],
}


def duckdb_delete(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    DELETE FROM {DOWNSTREAM_ASSET}
    WHERE MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    """,
):
    pass


def duckdb_load(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} BY NAME
    WITH snap AS (
        SELECT
            a.MTH_TM_ID,
            a.BASEL_ACCT_ID,
            a.PRIM_BASEL_CUST_ID,
            a.STEP_PLN_SNAPSHOT_ID,
            a.RECD_STAT_CD,
            a.DAY_ODUE,
            TRIM(ba.ACCT_NUM) AS ACCT_NUM,
            br.EXCLUDED_TRNST_NUM,
            ROUND(a.TOT_CRNT_BAL_AMT + a.ADD_ON_BAL_AMT + a.ACCR_INTR, 3) AS OS_BAL_AMT
        FROM ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT a
        LEFT JOIN ingestion.BASEL_ACCT_DIM ba
            ON a.BASEL_ACCT_ID = ba.BASEL_ACCT_ID
        LEFT JOIN reference.TRNST_EXCLSN_LKP br
            ON a.CRNT_BR_LOCTN_TRNST = br.EXCLUDED_TRNST_NUM
        WHERE a.MTH_TM_ID =
            {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    )
    SELECT
        MTH_TM_ID,
        BASEL_ACCT_ID,
        PRIM_BASEL_CUST_ID AS BASEL_CUST_ID,
        ACCT_NUM,
        CASE
            WHEN COALESCE(TRIM(EXCLUDED_TRNST_NUM), '') <> ''
                 OR OS_BAL_AMT <= 0
            THEN 'Z'
            ELSE 'A'
        END AS CONSM_PRD_TREATMNT_CD,
        OS_BAL_AMT,
        CASE
            WHEN TRIM(COALESCE(RECD_STAT_CD, '')) = '4' AND DAY_ODUE <= 90 THEN 'CUR'
            WHEN TRIM(COALESCE(RECD_STAT_CD, '')) = '4' AND DAY_ODUE > 90 THEN 'DEF'
            WHEN TRIM(COALESCE(RECD_STAT_CD, '')) = '5' THEN 'DEF'
            WHEN TRIM(COALESCE(RECD_STAT_CD, '')) = '6' THEN 'CHG'
            WHEN TRIM(COALESCE(RECD_STAT_CD, '')) = '7' THEN 'CHG'
            WHEN TRIM(COALESCE(RECD_STAT_CD, '')) = '8' THEN 'CHG'
        END AS PIT_STAT_VER_1_CD,
        CASE WHEN STEP_PLN_SNAPSHOT_ID > 0 THEN 'Y' ELSE 'N' END AS STEP_F,
        CASE
            WHEN COALESCE(TRIM(EXCLUDED_TRNST_NUM), '') = '' THEN 'N'
            ELSE 'Y'
        END AS TRNST_EXCLSN_F,
        CURRENT_TIMESTAMP AS INSRT_PROCESS_TMSTMP,
        CURRENT_TIMESTAMP AS UPDT_PROCESS_TMSTMP
    FROM snap
    """,
):
    pass
