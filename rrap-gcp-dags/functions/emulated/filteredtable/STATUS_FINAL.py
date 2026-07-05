"""
Rewrite of RRAP_MOR_MODEL_01_DEFINE_STATUS_G.sas + RRAP_MOR_MODEL_01A_GATHER_LOAD_G.sas.

Builds emulated.STATUS_FINAL for the process month:
  export_result — define status + exclusions from ingestion.MORTGAGE_HIST
  duckdb_load   — insert parquet into DuckLake

Logic mirrors 01_define_status.sas. Reuses existing features where available:
  STATUS            — features.PIT_STATUS_CROSS_DEFAULT_ORIG (MOR) via features.MORT_NUM
  DELQ_DAYS_2/MONTHS_2 — computed from ingestion.MORTGAGE_HIST (no feature; hist UNPD dates)
  MODEL_EXCL        — derived inline from STATUS + hist paid-off/balance (no MOR feature)
  Pass-through cols — ingestion.MORTGAGE_HIST (required by downstream PD/LGD jobs)
"""

UPSTREAM_ASSET = [
    "ingestion.MORTGAGE_HIST",
    "features.MORT_NUM",
    "features.PIT_STATUS_CROSS_DEFAULT_ORIG",
]

DOWNSTREAM_ASSET = "emulated.STATUS_FINAL"

_TASK_GROUP = "filteredtable__STATUS_FINAL"

DEPENDENCIES = {
    "duckdb_delete": ["export_result"],
    "export_result": ["duckdb_load"],
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


def export_result(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    WITH
        hist AS (
            SELECT *
            FROM ingestion.MORTGAGE_HIST
            WHERE CAST(PROCESS_DATE AS DATE) = DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
              AND CAST(PROCESS_DATE AS DATE) >= DATE '2010-01-31'
        ),
        with_delq AS (
            SELECT
                h.*,
                CASE
                    WHEN UPPER(TRIM(COALESCE(h.FLOAT_IND, ''))) IN ('W', 'B', 'S')
                    THEN DATE_DIFF(
                        'day',
                        DATE_TRUNC('day', h.UNPD_WKL_PAY_DATE),
                        DATE_TRUNC('day', h.PROCESS_DATE)
                    )
                    ELSE DATE_DIFF(
                        'day',
                        DATE_TRUNC('day', h.UNPD_MTH_PAY_DATE),
                        DATE_TRUNC('day', h.PROCESS_DATE)
                    )
                END AS temp_delq_days_2
            FROM hist h
        ),
        with_delq_days AS (
            SELECT
                d.*,
                CASE
                    WHEN d.PAID_OFF_DATE IS NOT NULL
                         OR UPPER(TRIM(COALESCE(d.PAID_OFF_IND, ''))) = 'Y'
                         OR d.temp_delq_days_2 IS NULL
                         OR d.temp_delq_days_2 < 0
                    THEN 0
                    ELSE d.temp_delq_days_2
                END AS delq_days_2
            FROM with_delq d
        ),
        with_delq_mth AS (
            SELECT
                d.* EXCLUDE (temp_delq_days_2),
                CASE
                    WHEN d.delq_days_2 = 0 THEN 0
                    WHEN UPPER(TRIM(COALESCE(d.FLOAT_IND, ''))) IN ('W', 'B', 'S')
                    THEN GREATEST(
                        DATE_DIFF(
                            'month',
                            DATE_TRUNC('day', d.UNPD_WKL_PAY_DATE),
                            DATE_TRUNC('day', d.PROCESS_DATE)
                        ) + 1,
                        0
                    )
                    ELSE GREATEST(
                        DATE_DIFF(
                            'month',
                            DATE_TRUNC('day', d.UNPD_MTH_PAY_DATE),
                            DATE_TRUNC('day', d.PROCESS_DATE)
                        ) + 1,
                        0
                    )
                END AS temp_delq_months_2
            FROM with_delq_days d
        ),
        with_status AS (
            SELECT
                m.* EXCLUDE (temp_delq_months_2),
                CASE
                    WHEN m.delq_days_2 >= 90 AND m.temp_delq_months_2 = 3 THEN 4
                    ELSE m.temp_delq_months_2
                END AS delq_months_2,
                pit.PIT_STATUS_CROSS_DEFAULT_ORIG AS new_status
            FROM with_delq_mth m
            LEFT JOIN features.MORT_NUM mn
                ON mn.SRC_SYS_CD = 'MOR'
               AND mn.OBSN_DT = DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
               AND m.MORTGAGE_NO = TRY_CAST(mn.MORT_NUM AS BIGINT)
            LEFT JOIN features.PIT_STATUS_CROSS_DEFAULT_ORIG pit
                ON pit.SRC_SYS_CD = 'MOR'
               AND pit.OBSN_DT = DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
               AND pit.BASEL_ACCT_ID = mn.BASEL_ACCT_ID
        )
    SELECT
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}' AS STREAM,
        {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}} AS MTH_TM_ID,
        MORTGAGE_NO,
        COMM_TYPE,
        FLOAT_LKP_ID,
        GENL_LKP_ID,
        CURRENT_BAL,
        DELQ_DAYS,
        DELQ_MONTHS,
        FLOAT_IND,
        FORECLOSE_IND,
        INT_ACCR_AMT,
        LRA_STATUS,
        PAID_OFF_DATE,
        PAID_OFF_IND,
        PROCESS_DATE,
        TOTAL_SUSPENSE,
        UNPD_MTH_PAY_DATE,
        UNPD_WKL_PAY_DATE,
        MADE_DATE,
        TIME_KEY,
        INT_ADJ_DATE,
        YYMTH,
        INSURANCE,
        CLASS,
        EFF_TMSTMP,
        PRPTY_DESC_1,
        PRPTY_DESC_2,
        PRPTY_DESC_3,
        LEND_VAL2,
        TRNST,
        AUTH_AMT,
        MAT_DT,
        CAB,
        TOT_ADVNC_AMT,
        PRIM_CUST_ID,
        CIF_KEY,
        STEP_F,
        INSUR_GRP,
        BULK_IND,
        AMORT,
        PREPAY_YTD,
        PROVINCE,
        delq_days_2 AS DELQ_DAYS_2,
        delq_months_2 AS DELQ_MONTHS_2,
        new_status AS STATUS,
        CASE
            WHEN UPPER(TRIM(COALESCE(PAID_OFF_IND, ''))) = 'Y'
                 OR UPPER(TRIM(COALESCE(new_status, ''))) <> 'CUR'
                 OR COALESCE(CURRENT_BAL, 0) <= 0
            THEN 'Y'
            ELSE 'N'
        END AS MODEL_EXCL,
        CURRENT_TIMESTAMP AS INSRT_PROCESS_TMSTMP,
        CURRENT_TIMESTAMP AS UPDT_PROCESS_TMSTMP
    FROM with_status
    """,
):
    pass


def duckdb_load(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} BY NAME
    SELECT *
    FROM read_parquet(
        '{{{{ task_instance.xcom_pull(task_ids="{_TASK_GROUP}.export_result", key="parquet") }}}}'
    )
    """,
):
    pass
