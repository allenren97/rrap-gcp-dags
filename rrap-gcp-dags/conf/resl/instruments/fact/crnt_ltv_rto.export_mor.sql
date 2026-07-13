SELECT
    mor.BASEL_ACCT_ID,
    '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' AS OBSN_DT,
    '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}' AS STREAM,
    rto.CRNT_LTV_RTO
FROM ingestion.MORT_MTH_SNAPSHOT mor
LEFT JOIN '{{ task_instance.xcom_pull(task_ids="fact__crnt_ltv_rto.export_crnt_ltv_rto", key="parquet") }}' rto
    ON rto.BASEL_ACCT_ID = mor.BASEL_ACCT_ID
WHERE mor.MTH_TM_ID = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}