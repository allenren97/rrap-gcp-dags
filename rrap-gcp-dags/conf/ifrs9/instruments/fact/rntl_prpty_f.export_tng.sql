SELECT
    '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' AS OBSN_DT,
    dim.BASEL_ACCT_ID,
    'TNG-MOR' AS SRC_SYS_CD,
    CASE 
        WHEN TRIM(RNTL_INCM_DPNDCY_F) IS NOT NULL THEN TRIM(RNTL_INCM_DPNDCY_F)
        ELSE NULL
    END AS RNTL_PRPTY_F_IFRS9
FROM ingestion.BASEL_ACCT_DIM dim
LEFT JOIN ingestion.TNG_ACCT_MO tng ON
    dim.SRC_APP_CD = 'TNG-MOR'
    AND dim.SRC_SYS_DEL_F != 'Y'
    AND TRIM(tng.ACCOUNT_ID) = TRIM(dim.SRC_APP_ID)
LEFT JOIN ingestion.BASEL_ACCT_PRFM_FACT fact ON
    dim.BASEL_ACCT_ID = fact.BASEL_ACCT_ID
    AND fact.MTH_END_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
WHERE 
    tng.MONTH_END_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'