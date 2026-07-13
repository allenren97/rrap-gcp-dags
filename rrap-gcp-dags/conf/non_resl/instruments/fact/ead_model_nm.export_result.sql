WITH
    seg AS (
        SELECT
            basel_acct_id,
            model
        FROM
            models.cc_ead_segment
        WHERE
            obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
            AND stream = 'NON_RESL'
        UNION ALL
        SELECT
            basel_acct_id,
            model
        FROM
            models.heloc_ead_segment
        WHERE
            obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
            AND stream = 'NON_RESL'
        UNION ALL
        SELECT
            basel_acct_id,
            model
        FROM
            models.loc_ead_segment
        WHERE
            obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
            AND stream = 'NON_RESL'
        UNION ALL
        SELECT
            basel_acct_id,
            model
        FROM
            models.ssla_ead_segment
        WHERE
            obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
            AND stream = 'NON_RESL'
    )
SELECT
    main.basel_acct_id,
    MOD.model_nm AS EAD_MODEL_NM,
    seg.model AS EAD_MODEL_ID
FROM
    features.basel_acct_id AS main
    LEFT JOIN seg ON seg.basel_acct_id = main.basel_acct_id
    LEFT JOIN reference.basel_model AS MOD ON MOD.basel_model_id = seg.model
WHERE
    main.obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'