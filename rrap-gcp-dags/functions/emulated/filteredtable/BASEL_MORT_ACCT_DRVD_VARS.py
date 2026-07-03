"""
Rewrite of J_RRII_KS10_2107_BASEL_MORT_ACCT_DRVD_VARS.sas only.

Builds emulated.BASEL_MORT_ACCT_DRVD_VARS in a single DuckDB pipeline mirroring
the SAS job (delinquency, commercial/residential, PIT v1, step and exclusion flags).
"""

UPSTREAM_ASSET = [
    "ingestion.BASEL_MORT_MTH_SNAPSHOT",
    "ingestion.BASEL_ACCT_DIM",
    "ingestion.TM_DIM",
    "reference.TRNST_EXCLSN_LKP",
]

DOWNSTREAM_ASSET = "emulated.BASEL_MORT_ACCT_DRVD_VARS"

DEPENDENCIES = {
    "duckdb_delete": ["duckdb_load"],
}


def duckdb_delete(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    DELETE FROM {DOWNSTREAM_ASSET}
    WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
      AND STREAM = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}'
    """,
):
    pass


def duckdb_load(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} BY NAME
    WITH
        tm_month AS (
            SELECT
                TM_ID,
                TM_LVL_ST_DT
            FROM ingestion.TM_DIM
            WHERE TM_ID =
                {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
        ),
        last_business_day AS (
            SELECT MAX(t1.DAY_DT) AS LAST_BUSINESS_DAY
            FROM ingestion.TM_DIM t1
            INNER JOIN (
                SELECT TM_ID, CLNDR_YR, MTH_CLNDR_CD
                FROM ingestion.TM_DIM
                WHERE TM_ID =
                    {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
            ) t2
                ON t1.CLNDR_YR = t2.CLNDR_YR
               AND t1.MTH_CLNDR_CD = t2.MTH_CLNDR_CD
            WHERE TRIM(t1.TM_LVL) = 'Day'
              AND TRIM(t1.DAY_OF_WK_DESC) IN (
                  'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday'
              )
        ),
        snap AS (
            SELECT
                a.MTH_TM_ID,
                a.BASEL_ACCT_ID,
                a.PRIM_BASEL_CUST_ID,
                a.STEP_PLN_SNAPSHOT_ID,
                a.FLOAT_CD,
                a.PD_OFF_F,
                a.PD_OFF_DT,
                a.FRCLSR_F,
                a.FUND_CD,
                a.SCRTY_TP_2,
                a.CRNT_BAL_AMT,
                a.INTR_ACCR_AMT,
                a.WK_FRST_UNPAID_DT,
                a.FRST_UNPAID_DT,
                a.SERV_BR_TRNST_NUM,
                TRIM(ba.ACCT_NUM) AS ACCT_NUM,
                br.EXCLUDED_TRNST_NUM
            FROM ingestion.BASEL_MORT_MTH_SNAPSHOT a
            LEFT JOIN reference.TRNST_EXCLSN_LKP br
                ON a.SERV_BR_TRNST_NUM = br.EXCLUDED_TRNST_NUM
            LEFT JOIN ingestion.BASEL_ACCT_DIM ba
                ON a.BASEL_ACCT_ID = ba.BASEL_ACCT_ID
               AND ba.SRC_APP_CD = 'MO'
            WHERE a.MTH_TM_ID =
                {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
        ),
        derived AS (
            SELECT
                s.*,
                s.PRIM_BASEL_CUST_ID AS BASEL_CUST_ID,
                s.CRNT_BAL_AMT + s.INTR_ACCR_AMT AS OS_BAL_AMT,
                CASE
                    WHEN SUBSTR(TRIM(COALESCE(s.SCRTY_TP_2, '')), 1, 1) = '6'
                         OR TRY_CAST(
                             SUBSTR(
                                 TRIM(COALESCE(s.SCRTY_TP_2, '')),
                                 GREATEST(LENGTH(TRIM(COALESCE(s.SCRTY_TP_2, ''))) - 2, 1),
                                 3
                             ) AS INTEGER
                         ) >= 5
                    THEN 'COMMERCIAL'
                    ELSE 'RESIDENTIAL'
                END AS COMM_TP_CD,
                CASE
                    WHEN (
                        TRY_CAST(TRIM(s.FUND_CD) AS INTEGER) BETWEEN 2000 AND 2199
                        OR TRY_CAST(TRIM(s.FUND_CD) AS INTEGER) BETWEEN 2202 AND 2249
                        OR TRY_CAST(TRIM(s.FUND_CD) AS INTEGER) BETWEEN 6490 AND 6499
                    )
                    THEN 'Y'
                    ELSE 'N'
                END AS LAND_RGSTRN_ACT_STAT_F,
                CASE
                    WHEN s.PD_OFF_DT IS NOT NULL OR TRIM(COALESCE(s.PD_OFF_F, '')) = 'Y'
                    THEN 0
                    WHEN TRIM(COALESCE(s.FLOAT_CD, '')) IN ('W', 'B', 'S')
                         AND s.WK_FRST_UNPAID_DT IS NOT NULL
                    THEN
                        CASE
                            WHEN lbd.LAST_BUSINESS_DAY < s.WK_FRST_UNPAID_DT THEN 0
                            ELSE DATE_DIFF('day', s.WK_FRST_UNPAID_DT, lbd.LAST_BUSINESS_DAY)
                        END
                    WHEN s.FRST_UNPAID_DT IS NOT NULL
                    THEN
                        CASE
                            WHEN lbd.LAST_BUSINESS_DAY < s.FRST_UNPAID_DT THEN 0
                            ELSE DATE_DIFF('day', s.FRST_UNPAID_DT, lbd.LAST_BUSINESS_DAY)
                        END
                    ELSE NULL
                END AS DLQNT_DAY_CNT,
                CASE
                    WHEN COALESCE(TRIM(s.EXCLUDED_TRNST_NUM), '') = '' THEN 'N'
                    ELSE 'Y'
                END AS TRNST_EXCLSN_F,
                CASE
                    WHEN s.STEP_PLN_SNAPSHOT_ID IN (-1, -2) THEN 'N'
                    ELSE 'Y'
                END AS STEP_F
            FROM snap s
            CROSS JOIN last_business_day lbd
        ),
        with_mth_cnt AS (
            SELECT
                d.*,
                CASE
                    WHEN d.DLQNT_DAY_CNT = 0 THEN 0
                    WHEN TRIM(COALESCE(d.FLOAT_CD, '')) IN ('W', 'B', 'S')
                    THEN
                        CASE
                            WHEN d.WK_FRST_UNPAID_DT IS NULL THEN 0
                            ELSE GREATEST(
                                DATE_DIFF(
                                    'month',
                                    DATE_TRUNC('day', d.WK_FRST_UNPAID_DT),
                                    tm.TM_LVL_ST_DT
                                ) + 1,
                                0
                            )
                        END
                    ELSE
                        CASE
                            WHEN d.FRST_UNPAID_DT IS NULL THEN 0
                            ELSE GREATEST(
                                DATE_DIFF(
                                    'month',
                                    DATE_TRUNC('day', d.FRST_UNPAID_DT),
                                    tm.TM_LVL_ST_DT
                                ) + 1,
                                0
                            )
                        END
                END AS DLQNT_MTH_CNT,
                CASE
                    WHEN d.COMM_TP_CD <> 'RESIDENTIAL'
                         OR d.PD_OFF_DT IS NOT NULL
                         OR d.OS_BAL_AMT <= 0
                    THEN 'Z'
                    ELSE 'A'
                END AS CONSM_PRD_TREATMNT_CD
            FROM derived d
            CROSS JOIN tm_month tm
        )
    SELECT
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}' AS STREAM,
        MTH_TM_ID,
        BASEL_ACCT_ID,
        BASEL_CUST_ID,
        ACCT_NUM,
        COMM_TP_CD,
        CONSM_PRD_TREATMNT_CD,
        DLQNT_DAY_CNT,
        DLQNT_MTH_CNT,
        LAND_RGSTRN_ACT_STAT_F,
        OS_BAL_AMT,
        CASE
            WHEN COMM_TP_CD = 'RESIDENTIAL'
                 AND CRNT_BAL_AMT <> 0
                 AND PD_OFF_DT IS NULL
                 AND (DLQNT_MTH_CNT <= 3 OR DLQNT_MTH_CNT IS NULL)
                 AND TRIM(COALESCE(FRCLSR_F, '')) <> 'Y'
                 AND TRIM(COALESCE(LAND_RGSTRN_ACT_STAT_F, '')) IN ('', 'N')
            THEN 'CUR'
            WHEN CRNT_BAL_AMT <> 0
                 AND COMM_TP_CD = 'RESIDENTIAL'
                 AND PD_OFF_DT IS NULL
                 AND (
                     DLQNT_MTH_CNT IS NULL
                     OR DLQNT_MTH_CNT > 3
                     OR TRIM(COALESCE(FRCLSR_F, '')) IN ('', 'Y')
                     OR TRIM(COALESCE(LAND_RGSTRN_ACT_STAT_F, '')) IN ('', 'Y')
                 )
            THEN 'DEF'
        END AS PIT_STAT_VER_1_CD,
        STEP_F,
        TRNST_EXCLSN_F,
        CURRENT_TIMESTAMP AS INSRT_PROCESS_TMSTMP,
        CURRENT_TIMESTAMP AS UPDT_PROCESS_TMSTMP
    FROM with_mth_cnt
    """,
):
    pass
