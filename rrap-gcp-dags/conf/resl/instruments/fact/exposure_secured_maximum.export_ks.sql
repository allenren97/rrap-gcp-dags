SELECT
    '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' AS OBSN_DT,
    '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}' AS STREAM,
    ks.BASEL_ACCT_ID,
    'KS' AS SRC_SYS_CD,
    EXPOSURE * (1+H_E) AS EXPOSURE_SECURED_MAXIMUM
FROM ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT ks
LEFT JOIN features.H_E h_e ON
    ks.BASEL_ACCT_ID = h_e.BASEL_ACCT_ID
    AND h_e.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
LEFT JOIN instruments.EXPOSURE ex ON
    ks.BASEL_ACCT_ID = ex.BASEL_ACCT_ID
    AND ex.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
    AND TRIM(STREAM) = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
WHERE ks.MTH_TM_ID = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}