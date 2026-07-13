SELECT
    main.BASEL_ACCT_ID,
    main.SRC_SYS_CD,
    CASE
        WHEN PD_FINAL_RPTG_RTO IS NULL THEN NULL
        ELSE greatest (PD_FINAL_RPTG_RTO, PD_FLR)
    END AS PD_FLRD_RPTG_RTO,
FROM
    features.basel_acct_id AS main
    LEFT JOIN instruments.PD_FLR AS PD_FLR ON PD_FLR.basel_acct_id = main.basel_acct_id
    AND PD_FLR.obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
    AND PD_FLR.stream = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
    LEFT JOIN instruments.PD_FINAL_RPTG_RTO AS PD_FINAL_RPTG_RTO ON PD_FINAL_RPTG_RTO.basel_acct_id = main.basel_acct_id
    AND PD_FINAL_RPTG_RTO.obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
    AND PD_FINAL_RPTG_RTO.stream = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
WHERE
    main.obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'