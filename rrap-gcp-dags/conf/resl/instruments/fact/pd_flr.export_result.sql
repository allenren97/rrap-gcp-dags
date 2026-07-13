SELECT
    main.BASEL_ACCT_ID,
    main.SRC_SYS_CD,
    CASE
        WHEN PD_FINAL_RPTG_RTO IS NULL THEN NULL
        WHEN CMHC_F = 'Y' THEN 0.0005
        WHEN ASST_CL_NUM = 3
        AND TRANSACTOR_FLAG_QRR = 'N' THEN 0.001
        ELSE 0.0005
    END AS PD_FLR
FROM
    features.basel_acct_id AS main
    LEFT JOIN features.CMHC_F AS CMHC_F ON CMHC_F.basel_acct_id = main.basel_acct_id
    AND CMHC_F.obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
    LEFT JOIN features.ASST_CL_NUM AS ASST_CL_NUM ON ASST_CL_NUM.basel_acct_id = main.basel_acct_id
    AND ASST_CL_NUM.obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
    LEFT JOIN features.TRANSACTOR_FLAG_QRR AS TRANSACTOR_FLAG_QRR ON TRANSACTOR_FLAG_QRR.basel_acct_id = main.basel_acct_id
    AND TRANSACTOR_FLAG_QRR.obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
    LEFT JOIN instruments.PD_FINAL_RPTG_RTO AS PD_FINAL_RPTG_RTO ON PD_FINAL_RPTG_RTO.basel_acct_id = main.basel_acct_id
    AND PD_FINAL_RPTG_RTO.obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
    AND PD_FINAL_RPTG_RTO.stream = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
WHERE
    main.obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'