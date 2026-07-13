WITH lgdd_segments AS(
    SELECT
        tng.ACCOUNT_ID,
        tng.MONTH_END_DT,
        CONS_DFT_MTH_CNT,
        LGD_BASEL_SEG_NUM
    FROM ingestion.TNG_ACCT_MO tng
    INNER JOIN ingestion.BASEL_ACCT_DIM dim ON
        dim.SRC_APP_CD = 'TNG-MOR'
        AND dim.SRC_SYS_DEL_F != 'Y'
        AND trim(tng.ACCOUNT_ID) = TRIM(dim.SRC_APP_ID)
    LEFT JOIN instruments.CONS_DFT_MTH_CNT cons ON
        dim.BASEL_ACCT_ID = cons.BASEL_ACCT_ID
        AND cons.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
        AND TRIM(cons.STREAM) = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
    LEFT JOIN instruments.LGD_BASEL_SEG_NUM lgd ON
        dim.BASEL_ACCT_ID = lgd.BASEL_ACCT_ID
        AND lgd.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
        AND TRIM(lgd.STREAM) = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
    WHERE tng.MONTH_END_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
)
    
SELECT
    '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' AS OBSN_DT,
    '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}' AS STREAM,
    dim.BASEL_ACCT_ID,
    'TNG-MOR' AS SRC_SYS_CD,
    CASE
        WHEN CONS_DFT_MTH_CNT > 23 AND LGD_BASEL_SEG_NUM = 1 THEN 0
        WHEN CONS_DFT_MTH_CNT > 23 AND LGD_BASEL_SEG_NUM = 11 THEN tng.ACCRUED_INTEREST_AMT
        ELSE 0
    END AS INTR_ACCR_AMT
FROM ingestion.TNG_ACCT_MO tng
INNER JOIN ingestion.BASEL_ACCT_DIM dim ON
    dim.SRC_APP_CD = 'TNG-MOR'
    AND dim.SRC_SYS_DEL_F != 'Y'
    AND trim(tng.ACCOUNT_ID) = TRIM(dim.SRC_APP_ID)
LEFT JOIN lgdd_segments segs ON
    tng.ACCOUNT_ID = segs.ACCOUNT_ID
    AND tng.MONTH_END_DT = segs.MONTH_END_DT
WHERE tng.MONTH_END_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
ORDER BY INTR_ACCR_AMT DESC