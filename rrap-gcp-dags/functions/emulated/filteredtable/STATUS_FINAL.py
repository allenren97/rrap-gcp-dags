"""
Rewrite of RRAP_MOR_MODEL_01_DEFINE_STATUS_G.sas + RRAP_MOR_MODEL_01A_GATHER_LOAD_G.sas.

Builds emulated.STATUS_FINAL for the process month:
  export_result — define status + exclusions from ingestion.MORTGAGE_HIST
  duckdb_load   — insert parquet into DuckLake

Logic mirrors 01_define_status.sas (delq_days_2/months_2, CUR/DEF status,
model_excl, PIT_STATUS_PRE_STEP Step cross-default override RRMSS-2842).
Processes mortgage_hist rows whose PROCESS_DATE equals the current rundate
(month-end), with process dates on/after 2010-01-31 (SAS gather-load filter).
"""

UPSTREAM_ASSET = [
    "ingestion.MORTGAGE_HIST",
    "ingestion.PIT_STATUS_PRE_STEP",
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
                d.delq_days_2,
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
            FROM with_delq d
        ),
        with_status AS (
            SELECT
                m.* EXCLUDE (temp_delq_months_2),
                CASE
                    WHEN m.delq_days_2 >= 90 AND m.temp_delq_months_2 = 3 THEN 4
                    ELSE m.temp_delq_months_2
                END AS delq_months_2,
                CASE
                    WHEN pit.MORT_NUM IS NOT NULL THEN pit.CROSS_DFLT_PIT_STATUS
                    WHEN UPPER(TRIM(COALESCE(m.COMM_TYPE, ''))) = 'RESIDENTIAL'
                         AND m.PAID_OFF_DATE IS NULL
                         AND (
                             (
                                 COALESCE(m.DELQ_DAYS, 0) < 90
                                 AND COALESCE(m.DELQ_MONTHS, 0) < 4
                                 AND UPPER(TRIM(COALESCE(m.FORECLOSE_IND, ''))) <> 'Y'
                                 AND COALESCE(m.CURRENT_BAL, 0) <> 0
                                 AND UPPER(TRIM(COALESCE(m.LRA_STATUS, ''))) <> 'Y'
                             )
                             OR COALESCE(m.CURRENT_BAL, 0) < 0
                         )
                    THEN 'CUR'
                    WHEN (
                        UPPER(TRIM(COALESCE(m.COMM_TYPE, ''))) = 'RESIDENTIAL'
                        AND m.PAID_OFF_DATE IS NULL
                        AND (
                            COALESCE(m.DELQ_DAYS, 0) >= 90
                            OR COALESCE(m.DELQ_MONTHS, 0) >= 4
                            OR UPPER(TRIM(COALESCE(m.FORECLOSE_IND, ''))) = 'Y'
                            OR UPPER(TRIM(COALESCE(m.LRA_STATUS, ''))) = 'Y'
                        )
                        AND COALESCE(m.CURRENT_BAL, 0) > 0
                    )
                    OR (
                        UPPER(TRIM(COALESCE(m.COMM_TYPE, ''))) = 'RESIDENTIAL'
                        AND UPPER(TRIM(COALESCE(m.FORECLOSE_IND, ''))) = 'Y'
                        AND UPPER(TRIM(COALESCE(m.PAID_OFF_IND, ''))) = 'Y'
                        AND GREATEST(
                            COALESCE(m.CURRENT_BAL, 0),
                            COALESCE(-m.TOTAL_SUSPENSE, 0)
                        ) > 0
                    )
                    THEN 'DEF'
                END AS new_status
            FROM with_delq_mth m
            LEFT JOIN ingestion.PIT_STATUS_PRE_STEP pit
                ON pit.SRC_SYS_CD = 'MOR'
               AND pit.CROSS_DFLT_PIT_OVERRIDE_F = 'Y'
               AND m.MORTGAGE_NO = pit.MORT_NUM
               AND m.PROCESS_DATE = pit.MORT_PROCESS_DATE
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
