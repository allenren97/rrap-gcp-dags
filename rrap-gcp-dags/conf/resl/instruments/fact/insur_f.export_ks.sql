SELECT
    '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' AS OBSN_DT,
    '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}' AS STREAM,
    ks.BASEL_ACCT_ID,
    'KS' AS SRC_SYS_CD,
    NULL AS INSUR_F
FROM ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT ks
WHERE ks.MTH_TM_ID = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}