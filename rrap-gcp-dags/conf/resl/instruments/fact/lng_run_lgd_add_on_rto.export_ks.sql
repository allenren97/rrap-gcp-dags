WITH vars AS(
    SELECT
        ks.BASEL_ACCT_ID,
        DLGD_F,
        PRPTY_VAL_CORR_PCTG,
        CRNT_LTV_RTO
    FROM ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT ks
    LEFT JOIN instruments.DLGD_F dlgd ON
        ks.BASEL_ACCT_ID = dlgd.BASEL_ACCT_ID
        AND dlgd.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
        AND TRIM(dlgd.STREAM) = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
    LEFT JOIN instruments.PRPTY_VAL_CORR_PCTG prpty ON
        ks.BASEL_ACCT_ID = prpty.BASEL_ACCT_ID
        AND prpty.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
        AND TRIM(prpty.STREAM) = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
    LEFT JOIN instruments.CRNT_LTV_RTO crnt ON
        ks.BASEL_ACCT_ID = crnt.BASEL_ACCT_ID
        AND crnt.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
        AND TRIM(crnt.STREAM) = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
    WHERE 
        ks.MTH_TM_ID = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}
),

base AS (
SELECT
    *,
    -- v11_MAX
    CASE
        WHEN INDEXED_CCLTV_RTO IS NULL OR PRPTY_VAL_CORR_PCTG::DECIMAL(30,20) IS NULL THEN NULL
        ELSE ROUND(GREATEST(INDEXED_CCLTV_RTO - (0.8 * (1 - PRPTY_VAL_CORR_PCTG::DECIMAL(30,20))), 0), 8)
    END AS v11_MAX,

    -- v11_MAX_LNG_STEP2
    ROUND(GREATEST(a.CRNT_LTV_RTO - (0.8 * (1 - PRPTY_VAL_CORR_PCTG::DECIMAL(30,20))), 0), 8
    ) AS v11_MAX_LNG_STEP2,

    -- V12_MAX
    CASE
        WHEN INDEXED_CCLTV_RTO IS NULL THEN NULL
        ELSE GREATEST(INDEXED_CCLTV_RTO - 0.8, 0)
    END AS V12_MAX,

    -- V12_MAX_LNG_STEP2
    CASE
        WHEN INDEXED_CCLTV_RTO IS NULL THEN NULL
        ELSE GREATEST(a.CRNT_LTV_RTO - 0.8, 0)
    END AS V12_MAX_LNG_STEP2

FROM vars a
LEFT JOIN instruments.INDEXED_CCLTV_RTO b ON
    a.BASEL_ACCT_ID = b.BASEL_ACCT_ID
    AND b.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
    AND b.STREAM = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
),

step2 AS (
SELECT
    *,

    -- V13_MIN
    LEAST(INDEXED_CCLTV_RTO, v11_MAX) AS V13_MIN,

    -- v14_MINUS_13_12
    (LEAST(INDEXED_CCLTV_RTO, v11_MAX) - V12_MAX) AS v14_MINUS_13_12,

    -- V15_DIVID
    CASE
        WHEN CRNT_LTV_RTO IS NOT NULL AND CRNT_LTV_RTO <> 0
        THEN ROUND((LEAST(INDEXED_CCLTV_RTO, v11_MAX) - V12_MAX) / CRNT_LTV_RTO, 9)
        ELSE NULL
    END AS V15_DIVID

FROM base
),

step3 AS (
SELECT
    *,

    -- v16_MAX
    GREATEST(V15_DIVID, 0) AS v16_MAX,

    -- V21_CCLTV_NULL
    (v11_MAX_LNG_STEP2 - V12_MAX_LNG_STEP2) AS V21_CCLTV_NULL,

    -- V22_DIVID
    CASE
        WHEN CRNT_LTV_RTO IS NOT NULL AND CRNT_LTV_RTO <> 0
        THEN ROUND(V21_CCLTV_NULL / CRNT_LTV_RTO, 9)
        ELSE NULL
    END AS V22_DIVID

FROM step2
),

step4 AS (
SELECT
    *,

    -- v_LNG_RUN_LGD_ADD_ON_RTO
    CASE
        WHEN DLGD_F = 'Y'
             AND CRNT_LTV_RTO IS NOT NULL
             AND PRPTY_VAL_CORR_PCTG::DECIMAL(30,20) IS NOT NULL
             AND INDEXED_CCLTV_RTO IS NOT NULL
        THEN v16_MAX

        WHEN DLGD_F = 'Y'
             AND CRNT_LTV_RTO IS NOT NULL
             AND PRPTY_VAL_CORR_PCTG::DECIMAL(30,20) IS NOT NULL
             AND INDEXED_CCLTV_RTO IS NULL
        THEN V22_DIVID

        ELSE NULL
    END AS v_LNG_RUN_LGD_ADD_ON_RTO

FROM step3
),

final AS(
SELECT
    *,
    CAST(ROUND(v_LNG_RUN_LGD_ADD_ON_RTO, 8) AS DECIMAL(28,8)) AS LNG_RUN_LGD_ADD_ON_RTO
FROM step4
)

SELECT
    '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' AS OBSN_DT,
    ks.BASEL_ACCT_ID,
    'KS' AS SRC_SYS_CD,
    '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}' AS STREAM,
    LNG_RUN_LGD_ADD_ON_RTO
FROM ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT ks
LEFT JOIN final ON
    ks.BASEL_ACCT_ID = final.BASEL_ACCT_ID
WHERE ks.MTH_TM_ID = 21076