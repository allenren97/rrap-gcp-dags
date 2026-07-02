SELECT
    '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' AS OBSN_DT,
    '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}' AS STREAM,
    mor.BASEL_ACCT_ID,
    'MOR' AS SRC_SYS_CD,
    NULL AS EXPOSURE_SECURED_MAXIMUM
FROM ingestion.MORT_MTH_SNAPSHOT mor
WHERE mor.MTH_TM_ID = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}