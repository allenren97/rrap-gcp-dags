SELECT 
    spl.BASEL_ACCT_ID,
    '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' AS OBSN_DT,
    '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}' AS STREAM,
    CASE
        WHEN seg.MODEL = 'dtl_lgdd' AND seg.LGD_BASEL_SEG_NUM IN (8, 7, 6)
            THEN NULL
        WHEN seg.MODEL = 'dtl_lgdnd' AND seg.LGD_BASEL_SEG_NUM IN (8)
            THEN NULL
        WHEN seg.MODEL = 'itl_lgdnd' AND seg.LGD_BASEL_SEG_NUM IN (7)
            THEN NULL
        WHEN seg.MODEL = 'itl_lgdd' AND seg.LGD_BASEL_SEG_NUM IN (6, 7, 8)
            THEN NULL
        WHEN pit.PIT_STAT_CD IN ('CUR')
            THEN rto.CRNT_LTV_RTO
        ELSE NULL
    END AS CRNT_LTV_RTO
FROM ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT AS spl
LEFT JOIN '{{ task_instance.xcom_pull(task_ids="fact__crnt_ltv_rto.export_crnt_ltv_rto", key="parquet") }}' rto
    ON rto.BASEL_ACCT_ID = spl.BASEL_ACCT_ID
LEFT JOIN instruments.PIT_STAT_CD AS pit
    ON pit.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
    AND pit.BASEL_ACCT_ID = spl.BASEL_ACCT_ID
    AND pit.STREAM = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
LEFT JOIN instruments.LGD_BASEL_SEG_NUM AS seg
    ON seg.BASEL_ACCT_ID = spl.BASEL_ACCT_ID
    AND seg.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
    AND seg.STREAM = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
WHERE spl.MTH_TM_ID = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}
