SELECT
    main.BASEL_ACCT_ID,
    main.SRC_SYS_CD,
    CASE
        WHEN TRIM(RNTL_INCM_DPNDCY_F) IS NOT NULL THEN TRIM(RNTL_INCM_DPNDCY_F)
        ELSE NULL
    END AS RNTL_PRPTY_F
FROM
    features.BASEL_ACCT_ID AS main
    LEFT JOIN ingestion.BASEL_ACCT_PRFM_FACT fact ON main.BASEL_ACCT_ID = fact.BASEL_ACCT_ID
    AND fact.MTH_END_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
WHERE
    main.obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'