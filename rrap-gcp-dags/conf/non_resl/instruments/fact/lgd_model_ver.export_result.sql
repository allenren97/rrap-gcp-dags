SELECT DISTINCT
    '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' AS OBSN_DT,
    acct.BASEL_ACCT_ID,
    acct.SRC_SYS_CD,
    '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}' AS STREAM,
    mod.MODEL_VER AS LGD_MODEL_VER
FROM features.BASEL_ACCT_ID acct
INNER JOIN ingestion.TM_DIM dim ON
    acct.OBSN_DT = dim.TM_LVL_END_DT
LEFT JOIN instruments.LGD_BASEL_SEG_NUM lgd ON
    acct.BASEL_ACCT_ID = lgd.BASEL_ACCT_ID
    AND lgd.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
    AND lgd.STREAM = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
LEFT JOIN reference.BASEL_MODEL mod ON
    lgd.MODEL = mod.BASEL_MODEL_ID
WHERE
    acct.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'