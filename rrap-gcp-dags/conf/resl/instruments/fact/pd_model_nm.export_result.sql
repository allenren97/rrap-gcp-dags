WITH
    seg AS (
        SELECT
            basel_acct_id,
            model,
        FROM
            models.standalone_heloc_pd_segment
        WHERE
            obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
            AND stream = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
        UNION ALL
        SELECT
            basel_acct_id,
            model,
        FROM
            models.standalone_mor_pd_segment
        WHERE
            obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
            AND stream = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
        UNION ALL
        SELECT
            basel_acct_id,
            model,
        FROM
            models.step_heloc_pd_segment
        WHERE
            obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
            AND stream = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
        UNION ALL
        SELECT
            basel_acct_id,
            model,
        FROM
            models.step_mor_pd_segment
        WHERE
            obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
            AND stream = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
        UNION ALL
        SELECT
            basel_acct_id,
            model,
        FROM
            models.step_mix_pd_segment
        WHERE
            obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
            AND stream = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
        UNION ALL
        SELECT
            basel_acct_id,
            model,
        FROM
            models.CC_PD_SEGMENT
        WHERE
            obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
            AND stream = 'NON_RESL'
        UNION ALL
        SELECT
            basel_acct_id,
            model,
        FROM
            models.ITL_PD_SEGMENT
        WHERE
            obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
            AND stream = 'NON_RESL'
        UNION ALL
        SELECT
            basel_acct_id,
            model,
        FROM
            models.LOC_PD_SEGMENT
        WHERE
            obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
            AND stream = 'NON_RESL'
        UNION ALL
        SELECT
            basel_acct_id,
            model,
        FROM
            models.SSLA_PD_SEGMENT
        WHERE
            obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
            AND stream = 'NON_RESL'
        UNION ALL
        SELECT
            basel_acct_id,
            model,
        FROM
            models.SSLB_PD_SEGMENT
        WHERE
            obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
            AND stream = 'NON_RESL'
        UNION ALL
        SELECT
            basel_acct_id,
            model,
        FROM
            models.TNG_MOR_PD_SEGMENT
        WHERE
            obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
            AND stream = 'NON_RESL'
    )
SELECT
    main.basel_acct_id,
    COALESCE(mo.MODEL_NM, mo2.MODEL_NM) AS PD_MODEL_NM,
    COALESCE(seg.model, seg2.model) AS PD_MODEL_ID
FROM
    features.BASEL_ACCT_ID AS main
    LEFT JOIN seg ON seg.basel_acct_id = main.basel_acct_id
    LEFT JOIN reference.basel_model AS mo ON mo.basel_model_id = seg.model
    LEFT JOIN (
        SELECT
            basel_acct_id,
            model,
        FROM
            models.dtl_pd_segment
        WHERE
            obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
            AND stream = 'NON_RESL'
    ) AS seg2 ON seg2.basel_acct_id = main.basel_acct_id
    LEFT JOIN reference.basel_model AS mo2 ON mo2.basel_model_id = seg2.model
WHERE
    main.obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'