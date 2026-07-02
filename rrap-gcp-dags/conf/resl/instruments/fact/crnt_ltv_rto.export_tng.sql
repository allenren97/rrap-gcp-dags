SELECT
    dim.BASEL_ACCT_ID,
    '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' AS OBSN_DT,
    '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}' AS STREAM,
    rto.CRNT_LTV_RTO
FROM ingestion.BASEL_ACCT_DIM dim
LEFT JOIN ingestion.TNG_ACCT_MO tng
    ON dim.SRC_APP_CD = 'TNG-MOR'
    AND dim.SRC_SYS_DEL_F != 'Y'
    AND TRIM(tng.ACCOUNT_ID) = TRIM(dim.SRC_APP_ID)
LEFT JOIN '{{ task_instance.xcom_pull(task_ids="fact__crnt_ltv_rto.export_crnt_ltv_rto", key="parquet") }}' rto
    ON rto.BASEL_ACCT_ID = dim.BASEL_ACCT_ID
WHERE tng.MONTH_END_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
