SELECT
    ks.BASEL_ACCT_ID,
    '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' AS OBSN_DT,
    '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}' AS STREAM,
    CASE
        WHEN f.DLGD_F = 'N' THEN NULL
        ELSE rto.CRNT_LTV_RTO
    END AS CRNT_LTV_RTO
FROM ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT AS ks
LEFT JOIN '{{ task_instance.xcom_pull(task_ids="fact__crnt_ltv_rto.export_crnt_ltv_rto", key="parquet") }}' rto
    ON rto.BASEL_ACCT_ID = ks.BASEL_ACCT_ID
LEFT JOIN instruments.DLGD_F AS f
    ON ks.BASEL_ACCT_ID = f.BASEL_ACCT_ID
    AND f.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
    AND f.STREAM = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
WHERE ks.MTH_TM_ID = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}


