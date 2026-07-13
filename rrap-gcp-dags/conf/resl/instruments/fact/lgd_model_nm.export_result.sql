WITH
    seg AS (
        SELECT
            basel_acct_id,
            model
        FROM
            models.cc_lgdd_segment
        WHERE
            obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
            AND stream = 'NON_RESL'
        UNION ALL
        SELECT
            basel_acct_id,
            model
        FROM
            models.cc_lgdnd_segment
        WHERE
            obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
            AND stream = 'NON_RESL'
        UNION ALL
        SELECT
            basel_acct_id,
            model
        FROM
            models.standalone_heloc_lgdd_segment
        WHERE
            obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
            AND stream = 'RESL'
        UNION ALL
        SELECT
            basel_acct_id,
            model
        FROM
            models.standalone_heloc_lgdnd_segment
        WHERE
            obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
            AND stream = 'RESL'
        UNION ALL
        SELECT
            basel_acct_id,
            model
        FROM
            models.step_heloc_lgdd_segment
        WHERE
            obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
            AND stream = 'RESL'
        UNION ALL
        SELECT
            basel_acct_id,
            model
        FROM
            models.step_heloc_lgdnd_segment
        WHERE
            obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
            AND stream = 'RESL'
        UNION ALL
        SELECT
            basel_acct_id,
            model
        FROM
            models.itl_lgdd_segment
        WHERE
            obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
            AND stream = 'NON_RESL'
        UNION ALL
        SELECT
            basel_acct_id,
            model
        FROM
            models.itl_lgdnd_segment
        WHERE
            obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
            AND stream = 'NON_RESL'
        UNION ALL
        SELECT
            basel_acct_id,
            model
        FROM
            models.loc_lgdd_segment
        WHERE
            obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
            AND stream = 'NON_RESL'
        UNION ALL
        SELECT
            basel_acct_id,
            model
        FROM
            models.loc_lgdnd_segment
        WHERE
            obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
            AND stream = 'NON_RESL'
        UNION ALL
        SELECT
            basel_acct_id,
            model
        FROM
            models.standalone_mor_lgdd_segment
        WHERE
            obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
            AND stream = 'RESL'
        UNION ALL
        SELECT
            basel_acct_id,
            model
        FROM
            models.standalone_mor_lgdnd_segment
        WHERE
            obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
            AND stream = 'RESL'
        UNION ALL
        SELECT
            basel_acct_id,
            model
        FROM
            models.step_mix_mor_lgdd_segment
        WHERE
            obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
            AND stream = 'RESL'
        UNION ALL
        SELECT
            basel_acct_id,
            model
        FROM
            models.step_mix_mor_lgdnd_segment
        WHERE
            obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
            AND stream = 'RESL'
        UNION ALL
        SELECT
            basel_acct_id,
            model
        FROM
            models.ssla_lgdd_segment
        WHERE
            obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
            AND stream = 'NON_RESL'
        UNION ALL
        SELECT
            basel_acct_id,
            model
        FROM
            models.ssla_lgdnd_segment
        WHERE
            obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
            AND stream = 'NON_RESL'
        UNION ALL
        SELECT
            basel_acct_id,
            model
        FROM
            models.sslb_lgdd_segment
        WHERE
            obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
            AND stream = 'NON_RESL'
        UNION ALL
        SELECT
            basel_acct_id,
            model
        FROM
            models.sslb_lgdnd_segment
        WHERE
            obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
            AND stream = 'NON_RESL'
        UNION ALL
        SELECT
            basel_acct_id,
            model
        FROM
            models.tng_mor_lgdd_segment
        WHERE
            obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
            AND stream = 'NON_RESL'
        UNION ALL
        SELECT
            basel_acct_id,
            model
        FROM
            models.tng_mor_lgdnd_segment
        WHERE
            obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
            AND stream = 'NON_RESL'
    )
SELECT
    main.basel_acct_id,
    COALESCE(MOD.model_nm, MOD2.model_nm) AS lgd_model_nm,
    COALESCE(MOD.basel_model_id, MOD2.basel_model_id) AS LGD_MODEL_ID
FROM
    features.basel_acct_id AS main
    LEFT JOIN seg ON seg.basel_acct_id = main.basel_acct_id
    LEFT JOIN reference.basel_model AS MOD ON MOD.basel_model_id = seg.model
    LEFT JOIN (
        SELECT
            basel_acct_id,
            model
        FROM
            models.dtl_lgdd_segment
        WHERE
            obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
            AND stream = 'NON_RESL'
        UNION ALL
        SELECT
            basel_acct_id,
            model
        FROM
            models.dtl_lgdnd_segment
        WHERE
            obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
            AND stream = 'NON_RESL'
    ) AS seg2 ON seg2.basel_acct_id = main.basel_acct_id
    LEFT JOIN reference.basel_model AS MOD2 ON MOD2.basel_model_id = seg2.model
WHERE
    main.obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'