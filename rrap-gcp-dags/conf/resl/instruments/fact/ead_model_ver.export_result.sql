SELECT DISTINCT
    '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' AS OBSN_DT,
    acct.BASEL_ACCT_ID,
    acct.SRC_SYS_CD,
    '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}' AS STREAM,
    CASE 
        WHEN TRIM(acct.SRC_SYS_CD) = 'KS' THEN mod.MODEL_VER 
        ELSE NULL
    END AS EAD_MODEL_VER
FROM features.BASEL_ACCT_ID acct
INNER JOIN ingestion.TM_DIM dim ON
    acct.OBSN_DT = dim.TM_LVL_END_DT
LEFT JOIN instruments.EAD_BASEL_SEG_NUM ead ON
    acct.BASEL_ACCT_ID = ead.BASEL_ACCT_ID
    AND ead.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
    AND ead.STREAM = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
LEFT JOIN reference.BASEL_MODEL mod ON
    ead.MODEL = mod.BASEL_MODEL_ID
WHERE
    acct.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
    AND TRIM(acct.SRC_SYS_CD) IN ('KS','SPL')

UNION ALL

SELECT
    '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' AS OBSN_DT,
    acct.BASEL_ACCT_ID,
    acct.SRC_SYS_CD,
    '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}' AS STREAM,
    NULL AS EAD_MODEL_VER
FROM features.BASEL_ACCT_ID acct
WHERE
    acct.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
    AND TRIM(acct.SRC_SYS_CD) IN ('MOR','TNG-MOR')