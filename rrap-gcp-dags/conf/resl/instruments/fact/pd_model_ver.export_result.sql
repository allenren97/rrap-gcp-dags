SELECT DISTINCT
    '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' AS OBSN_DT,
    acct.BASEL_ACCT_ID,
    acct.SRC_SYS_CD,
    '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}' AS STREAM,
    CASE
        WHEN PD_BASEL_SEG_NUM IS NULL THEN NULL
        ELSE mod.MODEL_VER 
    END AS PD_MODEL_VER
FROM features.BASEL_ACCT_ID acct
INNER JOIN ingestion.TM_DIM dim ON
    acct.OBSN_DT = dim.TM_LVL_END_DT
LEFT JOIN instruments.PD_BASEL_SEG_NUM pd ON
    acct.BASEL_ACCT_ID = pd.BASEL_ACCT_ID
    AND pd.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
    AND TRIM(pd.STREAM) = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
LEFT JOIN reference.BASEL_MODEL mod ON
    pd.MODEL =  mod.BASEL_MODEL_ID
WHERE
    acct.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'