SELECT
    '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' AS OBSN_DT,
    '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}' AS STREAM,
    'MOR' AS SRC_SYS_CD,
    BASEL_ACCT_ID,
    AMORT_MTH AS AMORT
FROM ingestion.MORT_MTH_SNAPSHOT
WHERE MTH_TM_ID = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}