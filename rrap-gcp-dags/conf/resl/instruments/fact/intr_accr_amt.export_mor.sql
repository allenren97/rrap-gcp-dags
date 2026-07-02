SELECT
    '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' AS OBSN_DT,
    '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}' AS STREAM,
    BASEL_ACCT_ID,
    'MOR' AS SRC_SYS_CD,
    INTR_ACCR_AMT
FROM ingestion.MORT_MTH_SNAPSHOT mor
WHERE MTH_TM_ID = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}