WITH
    acct_list AS (
        SELECT
            b.BASEL_ACCT_ID,
            MONTH_END_DT,
            account_id,
            LGD_BASEL_SEG_NUM
        FROM
            ingestion.TNG_ACCT_MO AS a
            INNER JOIN ingestion.BASEL_ACCT_DIM AS b ON a.ACCOUNT_ID = b.SRC_APP_ID
            AND b.SRC_APP_CD = 'TNG-MOR'
            AND b.SRC_SYS_DEL_F != 'Y'
            LEFT JOIN instruments.LGD_BASEL_SEG_NUM AS seg ON seg.BASEL_ACCT_ID = b.BASEL_ACCT_ID
            AND seg.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
            AND STREAM = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
        WHERE
            a.MONTH_END_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
    )
SELECT
    A.*,
    B.OPEN_DT,
    B.LAST_RENEWAL_DT,
    CASE
        WHEN A.MONTH_END_DT >= '2017-11-30'
        AND A.LGD_BASEL_SEG_NUM NOT IN (10, 11)
        AND c.PIT_STAT_CD = 'CUR'
        AND d.SCRTY_TP_DESC IN ('UNINSURED', 'INSURED')
        AND (
            B.OPEN_DT >= '2016-11-01'
            OR B.LAST_RENEWAL_DT >= '2016-11-01'
        ) THEN 'Y'
        WHEN A.MONTH_END_DT < '2017-11-30' --AND A.LGD_BASEL_SEG_NUM NOT IN (10,11) 
        AND c.PIT_STAT_CD = 'CUR'
        AND d.SCRTY_TP_DESC = 'UNINSURED'
        AND (
            B.OPEN_DT >= '2016-11-01'
            OR B.LAST_RENEWAL_DT >= '2016-11-01'
        ) THEN 'Y'
        ELSE 'N'
    END AS DLGD_F
FROM
    acct_list A
    LEFT JOIN ingestion.TNG_ACCT_MO B ON a.account_id = b.account_id
    LEFT JOIN (
        SELECT
            BASEL_ACCT_ID,
            ACCOUNT_ID,
            TNG_PIT_STATUS_CD AS PIT_STAT_CD
        FROM
            features.TNG_PIT_STATUS_CD
        WHERE
            OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
    ) c ON a.basel_acct_id = c.basel_acct_id
    LEFT JOIN (
        SELECT
            BASEL_ACCT_ID,
            SCRTY_TP_DESC
        FROM
            features.SCRTY_TP_DESC
        WHERE
            OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
    ) d ON a.basel_acct_id = d.basel_acct_id
WHERE
    b.MONTH_END_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'