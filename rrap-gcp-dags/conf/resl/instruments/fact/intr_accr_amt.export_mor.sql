SELECT
    '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' AS OBSN_DT,
    '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}' AS STREAM,
    BASEL_ACCT_ID,
    'MOR' AS SRC_SYS_CD,
    INEREST_ACCR_AMT AS INTR_ACCR_AMT
FROM ingestion.AIRB_MORT_MTH_SNAPSHOT airb
LEFT JOIN ingestion.MORT_MTH_SNAPSHOT mor ON
    airb.MORT_NUM = mor.MORT_NUM
WHERE 
    airb.MTH_END_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
    AND mor.MTH_TM_ID = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}