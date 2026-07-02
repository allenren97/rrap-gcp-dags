SELECT
    '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' AS OBSN_DT,
    pop.BASEL_ACCT_ID,
    CASE
        WHEN lfrr.LGD_FINAL_RPTG_RTO IS NULL THEN NULL
        
        WHEN f.DLGD_F = 'Y'
            AND scrty.SCRTY_TP_DESC = 'Insured'
            AND lrl.LNG_RUN_LGD_ADD_ON_RTO IS NOT NULL
        THEN LEAST(1, pil.PRE_INSURANCE_LGD + lrl.LNG_RUN_LGD_ADD_ON_RTO)

        WHEN f.DLGD_F = 'Y'
            AND scrty.SCRTY_TP_DESC IS DISTINCT FROM 'Insured'
            AND lrl.LNG_RUN_LGD_ADD_ON_RTO IS NOT NULL
        THEN LEAST(1, lurr.LGD_UNADJUSTED_RPTG_RTO + lrl.LNG_RUN_LGD_ADD_ON_RTO)

        ELSE NULL
    END AS DLGD_FLR
FROM
    ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT AS pop
LEFT JOIN instruments.DLGD_F AS f
    ON f.BASEL_ACCT_ID = pop.BASEL_ACCT_ID
    AND f.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
    AND f.STREAM = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
LEFT JOIN features.SCRTY_TP_DESC AS scrty
    ON scrty.BASEL_ACCT_ID = pop.BASEL_ACCT_ID
    AND scrty.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
LEFT JOIN instruments.LNG_RUN_LGD_ADD_ON_RTO AS lrl
    ON lrl.BASEL_ACCT_ID = pop.BASEL_ACCT_ID
    AND lrl.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
    AND lrl.STREAM = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
LEFT JOIN instruments.PRE_INSURANCE_LGD AS pil
    ON pil.BASEL_ACCT_ID = pop.BASEL_ACCT_ID
    AND pil.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
    AND pil.STREAM = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
LEFT JOIN instruments.LGD_UNADJUSTED_RPTG_RTO AS lurr
    ON lurr.BASEL_ACCT_ID = pop.BASEL_ACCT_ID
    AND lurr.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
    AND lurr.STREAM = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
LEFT JOIN instruments.LGD_FINAL_RPTG_RTO AS lfrr
    ON lfrr.BASEL_ACCT_ID = pop.BASEL_ACCT_ID
    AND lfrr.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
    AND lfrr.STREAM = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
WHERE
    pop.MTH_TM_ID = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}