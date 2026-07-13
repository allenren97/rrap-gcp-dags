WITH
    seg AS (
        {% set tables = [
            "CC_LGDD_SEGMENT",
            "CC_LGDND_SEGMENT",
            "DTL_LGDD_SEGMENT",
            "DTL_LGDND_SEGMENT",
            "HELOC_LGDD_SEGMENT",
            "HELOC_LGDND_SEGMENT",
            "ITL_LGDD_SEGMENT",
            "ITL_LGDND_SEGMENT",
            "LOC_LGDD_SEGMENT",
            "LOC_LGDND_SEGMENT",
            "MOR_LGDD_SEGMENT",
            "MOR_LGDND_SEGMENT",
            "SSLA_LGDD_SEGMENT",
            "SSLA_LGDND_SEGMENT",
            "SSLB_LGDD_SEGMENT",
            "SSLB_LGDND_SEGMENT",
            "TNG_MOR_LGDD_SEGMENT",
            "TNG_MOR_LGDND_SEGMENT",
        ] %}

        {% for table in tables %}
        SELECT
            BASEL_ACCT_ID,
            MODEL
        FROM models.{{ table }}
        WHERE obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
        AND stream = 'NON_RESL'
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
    )
SELECT
    main.basel_acct_id,
    MOD.model_nm AS lgd_model_nm,
    MOD.basel_model_id as LGD_MODEL_ID
FROM
    features.basel_acct_id AS main
    LEFT JOIN seg ON seg.basel_acct_id = main.basel_acct_id
    LEFT JOIN reference.basel_model AS MOD ON MOD.basel_model_id = seg.model
WHERE
    main.obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'