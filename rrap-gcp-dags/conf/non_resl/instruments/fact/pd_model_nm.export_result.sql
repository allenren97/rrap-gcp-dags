WITH
    seg AS (
        SELECT
            basel_acct_id,
            model,
        FROM
            models.CC_PD_SEGMENT
        WHERE
            obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
            AND stream = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
        UNION ALL
        SELECT
            basel_acct_id,
            model,
        FROM
            models.DTL_PD_SEGMENT
        WHERE
            obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
            AND stream = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
        UNION ALL
        SELECT
            basel_acct_id,
            model,
        FROM
            models.HELOC_PD_SEGMENT
        WHERE
            obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
            AND stream = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
        UNION ALL
        SELECT
            basel_acct_id,
            model,
        FROM
            models.ITL_PD_SEGMENT
        WHERE
            obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
            AND stream = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
        UNION ALL
        SELECT
            basel_acct_id,
            model,
        FROM
            models.LOC_PD_SEGMENT
        WHERE
            obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
            AND stream = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
        UNION ALL
        SELECT
            basel_acct_id,
            model,
        FROM
            models.MOR_PD_SEGMENT
        WHERE
            obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
            AND stream = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
        UNION ALL
        SELECT
            basel_acct_id,
            model,
        FROM
            models.SSLA_PD_SEGMENT
        WHERE
            obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
            AND stream = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
        UNION ALL
        SELECT
            basel_acct_id,
            model,
        FROM
            models.SSLB_PD_SEGMENT
        WHERE
            obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
            AND stream = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
        UNION ALL
        SELECT
            basel_acct_id,
            model,
        FROM
            models.TNG_MOR_PD_SEGMENT
        WHERE
            obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
            AND stream = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
    )
SELECT
    main.basel_acct_id,
    mo.MODEL_NM AS PD_MODEL_NM,
    seg.model AS PD_MODEL_ID
FROM
    features.BASEL_ACCT_ID AS main
    LEFT JOIN seg ON seg.basel_acct_id = main.basel_acct_id
    LEFT JOIN reference.BASEL_MODEL AS mo ON mo.basel_model_id = seg.model
WHERE
    main.obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'