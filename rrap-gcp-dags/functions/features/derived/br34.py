from airflow.sdk import get_current_context

UPSTREAM_ASSET = [
    'ingestion.BASEL_MORT_MTH_SNAPSHOT',
    'ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT',
    'ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT',
    'ingestion.BASEL_CUST_ACCT_RLTNP_SNAPSHOT',
    'ingestion.CR_BUREAU_DELI_MTH_SNAPSHOT',
]
DOWNSTREAM_ASSET = 'features.BR34'
DEPENDENCIES = {
    'duckdb_clear_br34': ['duckdb_load_br34'],
}

def duckdb_clear_br34(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
        DELETE FROM {DOWNSTREAM_ASSET}
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass


def duckdb_load_br34(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        INSERT INTO {DOWNSTREAM_ASSET} BY NAME
        WITH
        month_ctx AS (
            SELECT
                {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}::BIGINT AS MTH_TM_ID,
                DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT
        ),

        /* ---------------------------------------------
           2105  SCRD_CUSTOMER_LIST_SPL
           --------------------------------------------- */

        mortgage_customers AS (
            SELECT DISTINCT
                m.PRIM_BASEL_CUST_ID AS BASEL_CUST_ID
            FROM ingestion.BASEL_MORT_MTH_SNAPSHOT m
            INNER JOIN month_ctx x
                ON m.MTH_TM_ID = x.MTH_TM_ID
            WHERE m.PRIM_BASEL_CUST_ID IS NOT NULL
              AND m.PRIM_BASEL_CUST_ID <> -1
              AND m.CRNT_BAL_AMT <> 0
              AND m.PD_OFF_F = 'N'
              AND UPPER(
                    CASE
                        WHEN SUBSTR(TRIM(COALESCE(m.SCRTY_TP_2, '')), 1, 1) = '6'
                             OR TRY_CAST(RIGHT(TRIM(COALESCE(m.SCRTY_TP_2, '')), 3) AS INTEGER) >= 5
                        THEN 'COMMERCIAL'
                        ELSE 'RESIDENTIAL'
                    END
                  ) = 'RESIDENTIAL'
        ),

        personal_loans_customers AS (
            SELECT DISTINCT
                m.PRIM_BASEL_CUST_ID AS BASEL_CUST_ID
            FROM ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT m
            INNER JOIN month_ctx x
                ON m.MTH_TM_ID = x.MTH_TM_ID
            WHERE m.PRIM_BASEL_CUST_ID IS NOT NULL
              AND m.PRIM_BASEL_CUST_ID <> -1
              AND m.RECD_STAT_CD IN (4,5,6,7,8)
        ),

        revolving_credit_customers AS (
            SELECT DISTINCT
                m.PRIM_BASEL_CUST_ID AS BASEL_CUST_ID
            FROM ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT m
            INNER JOIN month_ctx x
                ON m.MTH_TM_ID = x.MTH_TM_ID
            WHERE m.PRIM_BASEL_CUST_ID IS NOT NULL
              AND m.PRIM_BASEL_CUST_ID <> -1
              AND (m.TOT_NEW_BAL_AMT > 0 OR m.CR_LMT_AMT > 0)
              AND m.PRD_CD NOT IN ('BLV')
              AND m.SUB_PRD_CD NOT IN ('CC')
              AND m.PRD_CD IN ('VIC','SCL','VLR','VGD','VCL','LOC','SSL','')
        ),

        scrd_customer_list_spl AS (
            SELECT DISTINCT BASEL_CUST_ID
            FROM (
                SELECT BASEL_CUST_ID FROM mortgage_customers
                UNION ALL
                SELECT BASEL_CUST_ID FROM personal_loans_customers
                UNION ALL
                SELECT BASEL_CUST_ID FROM revolving_credit_customers
            )
        ),

        /* ---------------------------------------------
           2106  BASEL_PSNL_LOAN_CUST_DRVD_VARS (population only)
           --------------------------------------------- */
        basel_psnl_loan_cust_drvd_vars_population AS (
            SELECT DISTINCT
                r.BASEL_CUST_ID,
                r.MTH_TM_ID
            FROM ingestion.BASEL_CUST_ACCT_RLTNP_SNAPSHOT r
            INNER JOIN scrd_customer_list_spl s
                ON r.BASEL_CUST_ID = s.BASEL_CUST_ID
            INNER JOIN month_ctx x
                ON r.MTH_TM_ID = x.MTH_TM_ID
            WHERE r.BASEL_CUST_ID IS NOT NULL
              AND r.BASEL_CUST_ID <> -1
        ),

        /* ---------------------------------------------
           CR_BUREAU_DELI  collapse to customer month grain
           --------------------------------------------- */
        deli_br34 AS (
            SELECT
                BASEL_CUST_ID,
                MTH_TM_ID,
                MAX(TOT_UTLTN_BNK_REVLVNG_CRD_AMT) AS BR34
            FROM ingestion.CR_BUREAU_DELI_MTH_SNAPSHOT
            WHERE MTH_TM_ID = (SELECT MTH_TM_ID FROM month_ctx)
              AND BASEL_CUST_ID <> -1
            GROUP BY BASEL_CUST_ID, MTH_TM_ID
        )

        /* ---------------------------------------------
           2107  SCRD_VAR_STATS_INPUTS_SPL_CUST (BR34 only)
           --------------------------------------------- */
        SELECT
            x.OBSN_DT,
            p.BASEL_CUST_ID,
            d.BR34
        FROM basel_psnl_loan_cust_drvd_vars_population p
        LEFT JOIN deli_br34 d
            ON p.BASEL_CUST_ID = d.BASEL_CUST_ID
           AND p.MTH_TM_ID     = d.MTH_TM_ID
        CROSS JOIN month_ctx x
    """
):
    pass