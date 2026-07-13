SELECT
    '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' AS OBSN_DT,
    '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}' AS STREAM,
    'SPL' AS SRC_SYS_CD,
    BASEL_ACCT_ID,
    LOAN_TERM AS AMORT
FROM ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT
WHERE MTH_TM_ID = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}