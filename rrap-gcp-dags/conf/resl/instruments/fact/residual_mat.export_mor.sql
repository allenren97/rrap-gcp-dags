SELECT
    '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' AS OBSN_DT,
    '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}' AS STREAM,
    mor.BASEL_ACCT_ID,
    'MOR' AS SRC_SYS_CD,
    base.RESIDUAL_MATURITY AS RESIDUAL_MAT
FROM ingestion.MORT_MTH_SNAPSHOT mor
LEFT JOIN ingestion.BASELAYER_MOR base ON
    mor.MORT_NUM = base.MORT_NUM
    AND base.MTH_END_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
WHERE mor.MTH_TM_ID = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}
