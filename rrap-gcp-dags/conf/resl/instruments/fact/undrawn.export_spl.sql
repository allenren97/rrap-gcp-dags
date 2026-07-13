SELECT
    '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' AS OBSN_DT,
    spl.BASEL_ACCT_ID,
    0 AS UNDRAWN,
    'SPL' AS SRC_SYS_CD
FROM
    ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT spl
WHERE 
    spl.MTH_TM_ID = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}