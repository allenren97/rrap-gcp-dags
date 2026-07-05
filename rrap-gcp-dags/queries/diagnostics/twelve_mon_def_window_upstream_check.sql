-- Upstream checks before running TWELVE_MON_DEF_WINDOW (emulated).
-- Edit process month below (must match handle_month_context rundate).

-- process: 2026-01-31 | stream: NON_RESL

-- ---------------------------------------------------------------------------
-- 1) STATUS_FINAL history depth (need start_period .. end_period, ~39 months)
-- ---------------------------------------------------------------------------
WITH
    params AS (
        SELECT DATE '2026-01-31' AS end_period
    ),
    periods AS (
        SELECT
            end_period,
            LAST_DAY(DATE_TRUNC('month', end_period) - INTERVAL 38 MONTH) AS start_period
        FROM params
    )
SELECT
    p.start_period,
    p.end_period,
    COUNT(DISTINCT CAST(sf.PROCESS_DATE AS DATE)) AS status_final_months_present,
    COUNT(*) AS status_final_rows,
    MIN(CAST(sf.PROCESS_DATE AS DATE)) AS min_process_date,
    MAX(CAST(sf.PROCESS_DATE AS DATE)) AS max_process_date
FROM periods p
LEFT JOIN emulated.STATUS_FINAL sf
    ON sf.STREAM = 'NON_RESL'
   AND CAST(sf.PROCESS_DATE AS DATE) BETWEEN p.start_period AND p.end_period
GROUP BY 1, 2;

-- Missing month-ends in STATUS_FINAL for the PD window range:
WITH
    params AS (
        SELECT DATE '2026-01-31' AS end_period
    ),
    periods AS (
        SELECT
            LAST_DAY(DATE_TRUNC('month', end_period) - INTERVAL 38 MONTH) AS start_period,
            end_period
        FROM params
    ),
    expected AS (
        SELECT LAST_DAY(d::DATE) AS month_end
        FROM periods p
        CROSS JOIN generate_series(
            DATE_TRUNC('month', p.start_period)::DATE,
            DATE_TRUNC('month', p.end_period)::DATE,
            INTERVAL 1 MONTH
        ) AS t(d)
    ),
    actual AS (
        SELECT DISTINCT LAST_DAY(CAST(PROCESS_DATE AS DATE)) AS month_end
        FROM emulated.STATUS_FINAL
        WHERE STREAM = 'NON_RESL'
    )
SELECT e.month_end AS missing_status_final_month
FROM expected e
LEFT JOIN actual a ON e.month_end = a.month_end
WHERE a.month_end IS NULL
ORDER BY 1;

-- ---------------------------------------------------------------------------
-- 2) Expected output volume for obs-start = end_period (current month window)
-- ---------------------------------------------------------------------------
WITH
    params AS (
        SELECT DATE '2026-01-31' AS obs_start
    ),
    windowed AS (
        SELECT sf.*
        FROM emulated.STATUS_FINAL sf
        CROSS JOIN params p
        WHERE sf.STREAM = 'NON_RESL'
          AND LAST_DAY(CAST(sf.PROCESS_DATE AS DATE)) >= p.obs_start
          AND LAST_DAY(CAST(sf.PROCESS_DATE AS DATE)) <= LAST_DAY(
                DATE_TRUNC('month', p.obs_start) + INTERVAL 12 MONTH
              )
    )
SELECT
    COUNT(DISTINCT MORTGAGE_NO) AS mortgages_in_current_window,
    COUNT(*) AS status_rows_in_window
FROM windowed;

-- ---------------------------------------------------------------------------
-- 3) After run: output partition summary
-- ---------------------------------------------------------------------------
SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT MORTGAGE_NO) AS mortgages,
    COUNT(DISTINCT PROCESS_DATE) AS obs_start_months,
    MIN(PROCESS_DATE) AS min_obs_start,
    MAX(PROCESS_DATE) AS max_obs_start,
    COUNT(*) FILTER (WHERE STATUS1 = 'CUR') AS status1_cur,
    COUNT(*) FILTER (WHERE DEFAULT_IND = 1) AS default_ind_1,
    COUNT(*) FILTER (WHERE DEFAULT_IND = 0) AS default_ind_0,
    COUNT(*) FILTER (WHERE DEFAULT_IND IS NULL) AS default_ind_null
FROM emulated.TWELVE_MON_DEF_WINDOW
WHERE OBSN_DT = DATE '2026-01-31'
  AND STREAM = 'NON_RESL';
