SELECT
    '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' AS OBSN_DT,
    '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}' AS STREAM,
    mor.BASEL_ACCT_ID,
    'MOR' AS SRC_SYS_CD,
    CASE
        WHEN TRIM(sys_cd.SRC_SYS_CD)='MOR' AND TRIM(step.INSURER_CD) <> ''
        THEN 'YES'
        ELSE NULL
    END AS INSUR_F
    FROM ingestion.MORT_MTH_SNAPSHOT mor
    LEFT JOIN features.SRC_SYS_CD sys_cd ON
        mor.BASEL_ACCT_ID = sys_cd.BASEL_ACCT_ID
        AND sys_cd.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
    LEFT JOIN ingestion.BASEL_STEP_PLN_MTH_SNAPSHOT step ON
        mor.PRIM_BASEL_CUST_ID = step.PRIM_BASEL_CUST_ID
        AND mor.MTH_TM_ID = step.MTH_TM_ID
        AND mor.STEP_PLN_AGRMNT_NUM = step.STEP_PLN_AGRMNT_NUM
        AND mor.PRIM_CUST_CID = step.PRIM_CUST_CID
    WHERE mor.MTH_TM_ID = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}