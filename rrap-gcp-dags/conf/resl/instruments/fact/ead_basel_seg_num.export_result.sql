WITH segments AS (
    {% set tables = [
        "step_heloc_ead_segment",
        "standalone_heloc_ead_segment",
        "cc_ead_segment",
        "loc_ead_segment",
        "ssla_ead_segment"
    ] %}

    {% for table in tables %}
    SELECT 
        OBSN_DT,
        BASEL_ACCT_ID,
        MODEL,
        VAR_SEGMENT,
        STREAM
    FROM models.{{ table }}
    WHERE obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
SELECT 
    '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' AS OBSN_DT,
    pop.BASEL_ACCT_ID,
    s.MODEL AS MODEL,
    CASE
        WHEN pop.SRC_SYS_CD = 'MOR' THEN NULL
        WHEN pop.SRC_SYS_CD = 'KS' THEN s.VAR_SEGMENT
        ELSE 1
    END AS EAD_BASEL_SEG_NUM,
    '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}' AS STREAM
FROM features.BASEL_ACCT_ID AS pop 
LEFT JOIN segments AS s
    ON s.basel_acct_id = pop.basel_acct_id
WHERE pop.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'