SELECT
    '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' AS OBSN_DT,
    ks.BASEL_ACCT_ID,
    'KS' AS SRC_SYS_CD,
    CASE 
        WHEN TRIM(RNTL_INCM_DPNDCY_F) IS NOT NULL THEN TRIM(RNTL_INCM_DPNDCY_F)
        ELSE NULL
    END AS RNTL_PRPTY_F_IFRS9
FROM ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT ks
LEFT JOIN ingestion.BASEL_ACCT_PRFM_FACT fact ON
    ks.BASEL_ACCT_ID = fact.BASEL_ACCT_ID
    AND fact.MTH_END_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
LEFT JOIN features.SRC_SYS_CD sys_cd ON
    ks.BASEL_ACCT_ID = sys_cd.BASEL_ACCT_ID
    AND sys_cd.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
WHERE
    ks.MTH_TM_ID = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}
    AND sys_cd.SRC_SYS_CD = 'KS'