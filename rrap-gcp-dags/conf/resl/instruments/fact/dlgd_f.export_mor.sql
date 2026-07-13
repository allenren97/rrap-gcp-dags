SELECT
    B.BASEL_ACCT_ID, 
    B.BASEL_MORT_MTH_SNAPSHOT_ID, 
    B.INTR_ADJ_DT, 
    B.LAST_RNEW_DT,
CASE 
    -- This first when is specific to resl
    WHEN seg.LGD_BASEL_SEG_NUM < 90 THEN 'Y'
    ELSE 'N'
END AS DLGD_F
FROM ingestion.BASEL_MORT_MTH_SNAPSHOT B 
LEFT JOIN (SELECT BASEL_ACCT_ID, PIT_STATUS_CROSS_DEFAULT_ORIG as PIT_STAT_CD 
            FROM features.PIT_STATUS_CROSS_DEFAULT_ORIG
            WHERE OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}') AS a
ON a.basel_acct_id = b.basel_acct_id  
LEFT JOIN (SELECT basel_acct_id, SCRTY_TP_DESC 
            FROM features.SCRTY_TP_DESC 
            WHERE OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}') c 
ON b.basel_acct_id = c.basel_acct_id
LEFT JOIN instruments.LGD_BASEL_SEG_NUM AS seg
    ON b.basel_acct_id = seg.basel_acct_id
    AND seg.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
    AND seg.STREAM = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
WHERE b.mth_tm_id = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}