SELECT
    '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' AS OBSN_DT,
    spl.BASEL_ACCT_ID,
    'SPL' AS SRC_SYS_CD,
    CASE 
        WHEN TRIM(RNTL_INCM_DPNDCY_F) IS NOT NULL THEN TRIM(RNTL_INCM_DPNDCY_F)
        ELSE NULL
    END AS RNTL_PRPTY_F_IFRS9
FROM ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT spl
LEFT JOIN ingestion.BASEL_ACCT_PRFM_FACT fact ON
    spl.BASEL_ACCT_ID = fact.BASEL_ACCT_ID
    AND spl.MTH_TM_ID = fact.MTH_TM_ID
LEFT JOIN features.SRC_SYS_CD sys_cd ON
    spl.BASEL_ACCT_ID = sys_cd.BASEL_ACCT_ID
    AND sys_cd.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
WHERE 
    spl.MTH_TM_ID = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}
    AND sys_cd.SRC_SYS_CD = 'SPL'