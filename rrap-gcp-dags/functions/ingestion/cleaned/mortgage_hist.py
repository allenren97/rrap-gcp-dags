"""
Rewrite of RRAP_MOR_ACCT_01_LOAD_MORTGAGE_G.sas (core transform + bulk ind).

Builds ingestion.MORTGAGE_HIST incrementally: one slice per process month from
ingestion.AIRB_MORT_MTH_SNAPSHOT, with gen_lkp joins and prepay/province
fill-forward from prior stacked history.

SAS partition/gather/unload orchestration is replaced by delete+insert on
PROCESS_DATE. Amort backfill for 200904 / 201001 / 201004 / 201007 is deferred
(phase 2); raw AIRB amort is loaded as-is.
"""

UPSTREAM_ASSET = [
    "ingestion.AIRB_MORT_MTH_SNAPSHOT",
    "reference.GEN_LKP",
    "reference.GENWORTH_BULKINS",
]

DOWNSTREAM_ASSET = "ingestion.MORTGAGE_HIST"

DEPENDENCIES = {
    "duckdb_delete": ["duckdb_load"],
}


def duckdb_delete(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    DELETE FROM {DOWNSTREAM_ASSET}
    WHERE CAST(PROCESS_DATE AS DATE) = DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """,
):
    pass


def duckdb_load(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} BY NAME
    FROM (
        WITH
            airb AS (
                SELECT bm.*
                FROM ingestion.AIRB_MORT_MTH_SNAPSHOT bm
                WHERE bm.TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
                  AND bm.MTH_END_DT >= DATE '2009-01-01'
            ),
            mapped AS (
                SELECT
                    CAST(bm.MORT_NUM AS BIGINT) AS MORTGAGE_NO,
                    TRIM(bm.COMM_TP) AS COMM_TYPE,
                    TRY_CAST(gl.GENL_LKP_ID AS INTEGER) AS FLOAT_LKP_ID,
                    TRY_CAST(gl.GENL_LKP_ID AS INTEGER) AS GENL_LKP_ID,
                    CAST(bm.CRNT_BAL AS DOUBLE) AS CURRENT_BAL,
                    bm.DLQNT_DAY AS DELQ_DAYS,
                    bm.DLQNT_MTH AS DELQ_MONTHS,
                    TRIM(gl.GENL_LKP_CD) AS FLOAT_IND,
                    NULLIF(TRIM(bm.FRCLSR_F), '') AS FORECLOSE_IND,
                    CAST(bm.INEREST_ACCR_AMT AS DOUBLE) AS INT_ACCR_AMT,
                    NULLIF(TRIM(bm.LRA_STAT), '') AS LRA_STATUS,
                    DATE_TRUNC('day', bm.PD_OFF_DT) AS PAID_OFF_DATE,
                    TRIM(bm.PD_OFF_F) AS PAID_OFF_IND,
                    CAST(bm.MTH_END_DT AS TIMESTAMP) AS PROCESS_DATE,
                    CAST(bm.TOT_SUSP_BAL AS DOUBLE) AS TOTAL_SUSPENSE,
                    DATE_TRUNC('day', bm.UNPAID_MTH_PAY_DT) AS UNPD_MTH_PAY_DATE,
                    DATE_TRUNC('day', bm.UNPAID_WKLY_PAY_DT) AS UNPD_WKL_PAY_DATE,
                    bm.MADE_DT AS MADE_DATE,
                    CAST(bm.MTH_END_DT AS TIMESTAMP) AS TIME_KEY,
                    bm.INTR_ADJ_DT AS INT_ADJ_DATE,
                    CAST(
                        EXTRACT(YEAR FROM bm.MTH_END_DT) * 100
                        + EXTRACT(MONTH FROM bm.MTH_END_DT) AS VARCHAR
                    ) AS YYMTH,
                    CASE
                        WHEN UPPER(TRIM(COALESCE(bm.INSUR_GRP, ''))) = 'CONV'
                        THEN 'Uninsured'
                        WHEN UPPER(TRIM(COALESCE(bm.INSUR_GRP, ''))) IN (
                            'GEM SPEC',
                            'GEMICO',
                            'GEMICO(NO DOWN)',
                            'MICC',
                            'CMHC',
                            'GUARANTY'
                        )
                        THEN 'Insured'
                        ELSE ''
                    END AS INSURANCE,
                    TRY_CAST(cl.GENL_LKP_CD AS INTEGER) AS CLASS,
                    CAST(bm.MTH_END_DT AS TIMESTAMP) AS EFF_TMSTMP,
                    TRIM(bm.PROPERTY_ADDR_1) AS PRPTY_DESC_1,
                    TRIM(bm.PROPERTY_ADDR_2) AS PRPTY_DESC_2,
                    TRIM(bm.PROPERTY_ADDR_3) AS PRPTY_DESC_3,
                    CAST(bm.LEND_VALUE AS DOUBLE) AS LEND_VAL2,
                    TRIM(bm.TRNST) AS TRNST,
                    CAST(bm.AUTH_AMT AS DOUBLE) AS AUTH_AMT,
                    bm.MAT_DT AS MAT_DT,
                    TRIM(bm.CAB) AS CAB,
                    CAST(bm.TOT_ADVNC_AMT AS DOUBLE) AS TOT_ADVNC_AMT,
                    TRY_CAST(TRIM(bm.PRIM_CUST_ID) AS BIGINT) AS PRIM_CUST_ID,
                    ' ' AS CIF_KEY,
                    TRIM(bm.STEP_F) AS STEP_F,
                    TRIM(bm.INSUR_GRP) AS INSUR_GRP,
                    bm.AMORT AS AMORT,
                    CAST(bm.PREPAY_YTD AS DOUBLE) AS raw_prepay_ytd,
                    TRIM(bm.PROP_PROV) AS raw_province
                FROM airb bm
                LEFT JOIN reference.GEN_LKP gl
                    ON TRIM(bm.FLOAT_IND) = TRIM(gl.GENL_LKP_CD)
                   AND gl.GENL_LKP_TP_NM = 'FLOAT_LKP_ID'
                   AND gl.TBL_NM = 'BASEL_MORT'
                LEFT JOIN reference.GEN_LKP cl
                    ON TRIM(bm.CLASS) = TRIM(cl.GENL_LKP_CD)
                   AND cl.GENL_LKP_TP_NM = 'CL_CD_LKP_ID'
                   AND cl.TBL_NM = 'BASEL_MORT'
            ),
            with_bulk AS (
                SELECT
                    m.*,
                    CASE
                        WHEN m.CLASS IN (54, 74)
                             OR (
                                 m.CLASS IN (71, 72)
                                 AND gb.LENDER_LOAN IS NOT NULL
                             )
                        THEN 'Y'
                        ELSE 'N'
                    END AS BULK_IND
                FROM mapped m
                LEFT JOIN reference.GENWORTH_BULKINS gb
                    ON gb.LENDER_LOAN = m.MORTGAGE_NO
            ),
            has_prior AS (
                SELECT DISTINCT MORTGAGE_NO
                FROM ingestion.MORTGAGE_HIST
                WHERE CAST(PROCESS_DATE AS DATE) < DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
            ),
            prior_prepay AS (
                SELECT MORTGAGE_NO, PREPAY_YTD AS last_prepay_ytd
                FROM (
                    SELECT
                        MORTGAGE_NO,
                        PREPAY_YTD,
                        ROW_NUMBER() OVER (
                            PARTITION BY MORTGAGE_NO
                            ORDER BY PROCESS_DATE DESC
                        ) AS rn
                    FROM ingestion.MORTGAGE_HIST
                    WHERE CAST(PROCESS_DATE AS DATE) < DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
                      AND PREPAY_YTD IS NOT NULL
                ) t
                WHERE rn = 1
            ),
            prior_province AS (
                SELECT MORTGAGE_NO, PROVINCE AS last_province
                FROM (
                    SELECT
                        MORTGAGE_NO,
                        PROVINCE,
                        ROW_NUMBER() OVER (
                            PARTITION BY MORTGAGE_NO
                            ORDER BY PROCESS_DATE DESC
                        ) AS rn
                    FROM ingestion.MORTGAGE_HIST
                    WHERE CAST(PROCESS_DATE AS DATE) < DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
                      AND PROVINCE IS NOT NULL
                      AND TRIM(PROVINCE) <> ''
                ) t
                WHERE rn = 1
            )
        SELECT
            w.MORTGAGE_NO,
            w.COMM_TYPE,
            w.FLOAT_LKP_ID,
            w.GENL_LKP_ID,
            w.CURRENT_BAL,
            w.DELQ_DAYS,
            w.DELQ_MONTHS,
            w.FLOAT_IND,
            w.FORECLOSE_IND,
            w.INT_ACCR_AMT,
            w.LRA_STATUS,
            w.PAID_OFF_DATE,
            w.PAID_OFF_IND,
            w.PROCESS_DATE,
            w.TOTAL_SUSPENSE,
            w.UNPD_MTH_PAY_DATE,
            w.UNPD_WKL_PAY_DATE,
            w.MADE_DATE,
            w.TIME_KEY,
            w.INT_ADJ_DATE,
            w.YYMTH,
            w.INSURANCE,
            w.CLASS,
            w.EFF_TMSTMP,
            w.PRPTY_DESC_1,
            w.PRPTY_DESC_2,
            w.PRPTY_DESC_3,
            w.LEND_VAL2,
            w.TRNST,
            w.AUTH_AMT,
            w.MAT_DT,
            w.CAB,
            w.TOT_ADVNC_AMT,
            w.PRIM_CUST_ID,
            w.CIF_KEY,
            w.STEP_F,
            w.INSUR_GRP,
            w.BULK_IND,
            w.AMORT,
            CASE
                WHEN hp.MORTGAGE_NO IS NULL THEN 0
                WHEN w.raw_prepay_ytd IS NULL THEN pp.last_prepay_ytd
                ELSE w.raw_prepay_ytd
            END AS PREPAY_YTD,
            CASE
                WHEN w.raw_province IS NOT NULL AND TRIM(w.raw_province) <> ''
                THEN w.raw_province
                ELSE pprov.last_province
            END AS PROVINCE
        FROM with_bulk w
        LEFT JOIN has_prior hp ON w.MORTGAGE_NO = hp.MORTGAGE_NO
        LEFT JOIN prior_prepay pp ON w.MORTGAGE_NO = pp.MORTGAGE_NO
        LEFT JOIN prior_province pprov ON w.MORTGAGE_NO = pprov.MORTGAGE_NO
    )
    """,
):
    pass
