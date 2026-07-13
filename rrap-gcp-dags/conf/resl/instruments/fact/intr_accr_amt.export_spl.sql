SELECT
    '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' AS OBSN_DT,
    '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}' AS STREAM,
    BASEL_ACCT_ID,
    'SPL' AS SRC_SYS_CD,
    ACCR_INTR AS INTR_ACCR_AMT
FROM ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT spl
WHERE MTH_TM_ID = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}