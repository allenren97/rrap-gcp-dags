-- Upstream checks before running STATUS_FINAL (emulated).
-- Edit process month below (must match handle_month_context rundate).

-- process: 2026-01-31 | stream: NON_RESL

-- ---------------------------------------------------------------------------
-- 1) Core inputs for one process month
-- ---------------------------------------------------------------------------
SELECT 'mortgage_hist' AS tbl, COUNT(*) AS row_count
FROM ingestion.MORTGAGE_HIST
WHERE CAST(PROCESS_DATE AS DATE) = DATE '2026-01-31'

UNION ALL

SELECT 'mortgage_hist_residential', COUNT(*)
FROM ingestion.MORTGAGE_HIST
WHERE CAST(PROCESS_DATE AS DATE) = DATE '2026-01-31'
  AND UPPER(TRIM(COALESCE(COMM_TYPE, ''))) = 'RESIDENTIAL'

UNION ALL

SELECT 'pit_cross_default_orig_mor', COUNT(*)
FROM features.PIT_STATUS_CROSS_DEFAULT_ORIG
WHERE SRC_SYS_CD = 'MOR'
  AND OBSN_DT = DATE '2026-01-31'

UNION ALL

SELECT 'pit_cross_default_orig_mor_override', COUNT(*)
FROM features.PIT_STATUS_CROSS_DEFAULT_ORIG
WHERE SRC_SYS_CD = 'MOR'
  AND CROSS_DFLT_PIT_OVERRIDE_F = 'Y'
  AND OBSN_DT = DATE '2026-01-31'

UNION ALL

SELECT 'mort_num_mor', COUNT(*)
FROM features.MORT_NUM
WHERE SRC_SYS_CD = 'MOR'
  AND OBSN_DT = DATE '2026-01-31';

-- ---------------------------------------------------------------------------
-- 2) Feature join coverage (MORTGAGE_HIST → MORT_NUM → PIT_STATUS_CROSS_DEFAULT_ORIG)
-- ---------------------------------------------------------------------------
SELECT
    COUNT(h.MORTGAGE_NO) AS hist_rows,
    COUNT(mn.MORT_NUM) AS mort_num_matched,
    COUNT(pit.BASEL_ACCT_ID) AS pit_join_matched,
    COUNT(h.MORTGAGE_NO) - COUNT(pit.BASEL_ACCT_ID) AS pit_join_miss
FROM ingestion.MORTGAGE_HIST h
LEFT JOIN features.MORT_NUM mn
    ON mn.SRC_SYS_CD = 'MOR'
   AND mn.OBSN_DT = DATE '2026-01-31'
   AND h.MORTGAGE_NO = TRY_CAST(mn.MORT_NUM AS BIGINT)
LEFT JOIN features.PIT_STATUS_CROSS_DEFAULT_ORIG pit
    ON pit.SRC_SYS_CD = 'MOR'
   AND pit.OBSN_DT = DATE '2026-01-31'
   AND pit.BASEL_ACCT_ID = mn.BASEL_ACCT_ID
WHERE CAST(h.PROCESS_DATE AS DATE) = DATE '2026-01-31';

SELECT COUNT(*) AS override_join_matched
FROM ingestion.MORTGAGE_HIST h
INNER JOIN features.MORT_NUM mn
    ON mn.SRC_SYS_CD = 'MOR'
   AND mn.OBSN_DT = DATE '2026-01-31'
   AND h.MORTGAGE_NO = TRY_CAST(mn.MORT_NUM AS BIGINT)
INNER JOIN features.PIT_STATUS_CROSS_DEFAULT_ORIG pit
    ON pit.SRC_SYS_CD = 'MOR'
   AND pit.OBSN_DT = DATE '2026-01-31'
   AND pit.BASEL_ACCT_ID = mn.BASEL_ACCT_ID
   AND pit.CROSS_DFLT_PIT_OVERRIDE_F = 'Y'
WHERE CAST(h.PROCESS_DATE AS DATE) = DATE '2026-01-31';

-- ---------------------------------------------------------------------------
-- 3) Expected STATUS distribution (mirrors export_result logic)
-- ---------------------------------------------------------------------------
WITH
    hist AS (
        SELECT *
        FROM ingestion.MORTGAGE_HIST
        WHERE CAST(PROCESS_DATE AS DATE) = DATE '2026-01-31'
          AND CAST(PROCESS_DATE AS DATE) >= DATE '2010-01-31'
    ),
    with_delq AS (
        SELECT
            h.*,
            CASE
                WHEN UPPER(TRIM(COALESCE(h.FLOAT_IND, ''))) IN ('W', 'B', 'S')
                THEN DATE_DIFF('day', DATE_TRUNC('day', h.UNPD_WKL_PAY_DATE), DATE_TRUNC('day', h.PROCESS_DATE))
                ELSE DATE_DIFF('day', DATE_TRUNC('day', h.UNPD_MTH_PAY_DATE), DATE_TRUNC('day', h.PROCESS_DATE))
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
                THEN GREATEST(DATE_DIFF('month', DATE_TRUNC('day', d.UNPD_WKL_PAY_DATE), DATE_TRUNC('day', d.PROCESS_DATE)) + 1, 0)
                ELSE GREATEST(DATE_DIFF('month', DATE_TRUNC('day', d.UNPD_MTH_PAY_DATE), DATE_TRUNC('day', d.PROCESS_DATE)) + 1, 0)
            END AS temp_delq_months_2
        FROM with_delq_days d
    ),
    hist_status AS (
        SELECT
            m.* EXCLUDE (temp_delq_months_2),
            CASE
                WHEN m.delq_days_2 >= 90 AND m.temp_delq_months_2 = 3 THEN 4
                ELSE m.temp_delq_months_2
            END AS delq_months_2
        FROM with_delq_mth m
    ),
    with_status AS (
        SELECT
            h.*,
            pit.PIT_STATUS_CROSS_DEFAULT_ORIG AS new_status
        FROM hist_status h
        LEFT JOIN features.MORT_NUM mn
            ON mn.SRC_SYS_CD = 'MOR'
           AND mn.OBSN_DT = DATE '2026-01-31'
           AND h.MORTGAGE_NO = TRY_CAST(mn.MORT_NUM AS BIGINT)
        LEFT JOIN features.PIT_STATUS_CROSS_DEFAULT_ORIG pit
            ON pit.SRC_SYS_CD = 'MOR'
           AND pit.OBSN_DT = DATE '2026-01-31'
           AND pit.BASEL_ACCT_ID = mn.BASEL_ACCT_ID
    )
SELECT
    COUNT(*) AS total_rows,
    COUNT(*) FILTER (WHERE new_status = 'CUR') AS status_cur,
    COUNT(*) FILTER (WHERE new_status = 'DEF') AS status_def,
    COUNT(*) FILTER (WHERE new_status IS NULL) AS status_null,
    COUNT(*) FILTER (
        WHERE UPPER(TRIM(COALESCE(PAID_OFF_IND, ''))) = 'Y'
           OR UPPER(TRIM(COALESCE(new_status, ''))) <> 'CUR'
           OR COALESCE(CURRENT_BAL, 0) <= 0
    ) AS model_excl_y
FROM with_status;

-- ---------------------------------------------------------------------------
-- 4) After run: output partition
-- ---------------------------------------------------------------------------
SELECT STATUS, MODEL_EXCL, COUNT(*) AS n
FROM emulated.STATUS_FINAL
WHERE OBSN_DT = DATE '2026-01-31'
  AND STREAM = 'NON_RESL'
GROUP BY 1, 2
ORDER BY 1, 2;
