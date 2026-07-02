SELECT
    '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' AS OBSN_DT,
    '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}' AS STREAM,
    ks.BASEL_ACCT_ID,
    'KS' AS SRC_SYS_CD,
    CASE
        WHEN TRIM(tp_cd.BASEL_PRD_TP_CD) = 'HELOC' AND INSURER_CD IS NOT NULL
        THEN 'YES' ELSE NULL 
    END AS INSUR_F
FROM ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT ks
LEFT JOIN features.BASEL_PRD_TP_CD tp_cd ON
    ks.BASEL_ACCT_ID = tp_cd.BASEL_ACCT_ID
    AND tp_cd.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
LEFT JOIN ingestion.BASEL_STEP_PLN_MTH_SNAPSHOT step ON
    ks.PRIM_BASEL_CUST_ID = step.PRIM_BASEL_CUST_ID
    AND ks.MTH_TM_ID = step.MTH_TM_ID
    AND ks.STEP_PLN_AGRMNT_NUM = step.STEP_PLN_AGRMNT_NUM
    AND ks.PRIM_CUST_CID = step.PRIM_CUST_CID
WHERE ks.MTH_TM_ID = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}