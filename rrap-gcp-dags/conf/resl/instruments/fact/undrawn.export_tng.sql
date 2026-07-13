SELECT
    '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' AS OBSN_DT,
    dim.BASEL_ACCT_ID,
    0 AS UNDRAWN,
    'TNG-MOR' AS SRC_SYS_CD 
FROM ingestion.TNG_ACCT_MO tng
INNER JOIN ingestion.BASEL_ACCT_DIM dim ON
    dim.SRC_APP_CD = 'TNG-MOR'
    AND dim.SRC_SYS_DEL_F != 'Y'
    AND TRIM(tng.ACCOUNT_ID) = TRIM(dim.SRC_APP_ID)
WHERE 
    tng.MONTH_END_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
GROUP BY
    dim.BASEL_ACCT_ID