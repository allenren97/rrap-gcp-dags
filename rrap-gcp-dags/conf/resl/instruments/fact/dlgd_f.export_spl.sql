with dlgd as (
    SELECT
        a.BASEL_ACCT_ID,
        CASE
            -- These checks replace the sas code excluding segment ids in (16136,16135,16162,16155,16128,16156,16163,16164,16134)
            WHEN seg.MODEL IN ('dtl_lgdnd', 'dtl_lgdd', 'itl_lgdnd', 'itl_lgdd') AND seg.LGD_BASEL_SEG_NUM = 8 THEN 'N'
            WHEN seg.MODEL IN ('dtl_lgdd', 'itl_lgdnd', 'itl_lgdd') AND seg.LGD_BASEL_SEG_NUM = 7 THEN 'N'
            WHEN seg.MODEL IN ('dtl_lgdd', 'itl_lgdd') AND seg.LGD_BASEL_SEG_NUM = 6 THEN 'N'
            -- This line is specific to resl
            WHEN seg.LGD_BASEL_SEG_NUM >= 90 THEN 'N'
        ELSE 'Y'
        END AS DLGD_F
    FROM ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT a
    INNER JOIN (SELECT BASEL_ACCT_ID, PRD_ID 
                FROM features.PRD_ID 
                WHERE OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
                ) b
        ON a.BASEL_ACCT_ID = b.BASEL_ACCT_ID
    LEFT JOIN (SELECT BASEL_ACCT_ID, 
                    LGD_BASEL_SEG_NUM,
                    MODEL
                FROM instruments.LGD_BASEL_SEG_NUM
                WHERE OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
                AND STREAM = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
                ) seg
        ON a.basel_acct_id = seg.basel_acct_id
    LEFT JOIN (SELECT BASEL_ACCT_ID,PIT_STATUS_CROSS_DEFAULT_ORIG as PIT_STATUS_V2 
                FROM features.PIT_STATUS_CROSS_DEFAULT_ORIG 
                WHERE OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
                ) pit
        ON pit.BASEL_ACCT_ID=a.basel_acct_id
    WHERE a.MTH_TM_ID = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}
        AND UPPER(b.PRD_ID) IN ('S05','S08') -- This might only be needed for the resl stream
        AND a.NOTE_DT >= '2016-11-01'
        AND seg.LGD_BASEL_SEG_NUM IS NOT NULL
        AND pit.PIT_STATUS_V2 in ('CUR')
)
SELECT
    a.BASEL_ACCT_ID,
    coalesce(b.DLGD_F,'N') as DLGD_F
FROM ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT a
LEFT JOIN dlgd b
    ON a.basel_acct_id = b.basel_acct_id
where a.MTH_TM_ID = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}