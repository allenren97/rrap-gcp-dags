SELECT
    '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' AS OBSN_DT,
    '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}' AS STREAM,
    spl.BASEL_ACCT_ID,
    'SPL' AS SRC_SYS_CD,
    NULL AS INSUR_F
FROM ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT spl
WHERE spl.MTH_TM_ID = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}