SELECT
    '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' AS OBSN_DT,
    '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}' AS STREAM,
    dim.BASEL_ACCT_ID,
    'TNG-MOR' AS SRC_SYS_CD,
    date_diff('Month', MONTH_END_DT, mat.MAT_DT) AS RESIDUAL_MAT
FROM ingestion.TNG_ACCT_MO tng
INNER JOIN ingestion.BASEL_ACCT_DIM dim ON
    dim.SRC_APP_CD = 'TNG-MOR'
    AND dim.SRC_SYS_DEL_F != 'Y'
    AND trim(tng.ACCOUNT_ID) = TRIM(dim.SRC_APP_ID)
LEFT JOIN features.MAT_DT mat ON
    dim.BASEL_ACCT_ID = mat.BASEL_ACCT_ID
    AND mat.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
WHERE tng.MONTH_END_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
