WITH segments AS (
    {% set tables = [
        "tng_mor_pd_segment",
        "mor_pd_segment",
        "dtl_pd_segment",
        "itl_pd_segment",
        "heloc_pd_segment",
        "loc_pd_segment",
        "cc_pd_segment",
        "ssla_pd_segment",
        "sslb_pd_segment"
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
    pop.OBSN_DT,
    pop.BASEL_ACCT_ID,
    s.MODEL AS MODEL,
    s.VAR_SEGMENT as PD_BASEL_SEG_NUM,
    '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}' AS STREAM
FROM features.BASEL_ACCT_ID AS pop
LEFT JOIN segments AS s
    ON s.basel_acct_id = pop.basel_acct_id
WHERE pop.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'