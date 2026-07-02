WITH vars AS(
    SELECT
        acct.BASEL_ACCT_ID,
        acct.SRC_SYS_CD,
        DLGD_F,
        PRPTY_VAL_CORR_PCTG,
        CRNT_LTV_RTO
    FROM features.BASEL_ACCT_ID acct
    LEFT JOIN instruments.DLGD_F dlgd ON
        acct.BASEL_ACCT_ID = dlgd.BASEL_ACCT_ID
        AND dlgd.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
        AND TRIM(dlgd.STREAM) = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
    LEFT JOIN instruments.PRPTY_VAL_CORR_PCTG prpty ON
        acct.BASEL_ACCT_ID = prpty.BASEL_ACCT_ID
        AND prpty.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
        AND TRIM(prpty.STREAM) = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
    LEFT JOIN instruments.CRNT_LTV_RTO crnt ON
        acct.BASEL_ACCT_ID = crnt.BASEL_ACCT_ID
        AND crnt.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
        AND TRIM(crnt.STREAM) = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
    WHERE 
        acct.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
        AND TRIM(acct.SRC_SYS_CD) IN ('MOR','TNG-MOR')
)

SELECT
    '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' AS OBSN_DT,
    v.BASEL_ACCT_ID,
    v.SRC_SYS_CD,
    '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}' AS STREAM,
    CASE
        WHEN DLGD_F = 'Y'
            AND CRNT_LTV_RTO IS NOT NULL
            AND PRPTY_VAL_CORR_PCTG::DECIMAL(30,20) IS NOT NULL
            AND INDEXED_CCLTV_RTO IS NOT NULL
        THEN 
            CAST(TRUNC(GREATEST((LEAST(CRNT_LTV_RTO, GREATEST(INDEXED_CCLTV_RTO - 0.8 * (1 - PRPTY_VAL_CORR_PCTG::DECIMAL(30,20)), 0)) - GREATEST(INDEXED_CCLTV_RTO - 0.8, 0)) / NULLIF(CRNT_LTV_RTO, 0), 0), 8) AS DECIMAL(28,8))
        WHEN DLGD_F = 'Y'
            AND CRNT_LTV_RTO IS NOT NULL
            AND PRPTY_VAL_CORR_PCTG::DECIMAL(30,20) IS NOT NULL
            AND INDEXED_CCLTV_RTO IS NULL
        THEN 
            CAST(TRUNC((GREATEST(CRNT_LTV_RTO - 0.8 * (1 - PRPTY_VAL_CORR_PCTG::DECIMAL(30,20)), 0)- GREATEST(CRNT_LTV_RTO - 0.8, 0)) / NULLIF(CRNT_LTV_RTO, 0), 8) AS DECIMAL(28,8))
        ELSE NULL
    END AS LNG_RUN_LGD_ADD_ON_RTO
FROM vars v
LEFT JOIN instruments.INDEXED_CCLTV_RTO ind ON
    v.BASEL_ACCT_ID = ind.BASEL_ACCT_ID
    AND ind.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
    AND TRIM(ind.STREAM) = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'