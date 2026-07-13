SELECT
    brcms.BASEL_ACCT_ID,
    CASE     
        WHEN seg.LGD_BASEL_SEG_NUM >= 90 THEN 'N'
        WHEN heloc.HELOC_F='Y' and brcms.ACCT_OPND_DT >= '2016-11-01' THEN 'Y'
        ELSE 'N'
    END AS DLGD_F
FROM ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT AS brcms
LEFT JOIN instruments.LGD_BASEL_SEG_NUM AS seg
    ON brcms.basel_acct_id = seg.basel_acct_id
    AND seg.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
    AND seg.STREAM = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
LEFT JOIN features.HELOC_F AS heloc
    ON brcms.basel_acct_id = heloc.basel_acct_id
    AND heloc.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
WHERE
    brcms.MTH_TM_ID = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}