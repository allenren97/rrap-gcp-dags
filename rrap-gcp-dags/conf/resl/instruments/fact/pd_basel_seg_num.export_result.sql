WITH segments AS (
    {% set tables = [
        "loc_pd_segment",
        "step_mix_pd_segment",
        "itl_pd_segment",
        "step_mor_pd_segment",
        "standalone_heloc_pd_segment",
        "ssla_pd_segment",
        "sslb_pd_segment",
        "cc_pd_segment",
        "step_heloc_pd_segment",
        "standalone_mor_pd_segment",
        "tng_mor_pd_segment"
    ] %}

    {% for table in tables %}
    SELECT 
        OBSN_DT,
        BASEL_ACCT_ID,
        MODEL,
        VAR_SEGMENT
    FROM models.{{ table }}
    WHERE obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
),
dtl AS (
    SELECT
        OBSN_DT,
        BASEL_ACCT_ID,
        MODEL,
        VAR_SEGMENT
    FROM models.dtl_pd_segment
    WHERE obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
)
SELECT 
    pop.OBSN_DT,
    pop.BASEL_ACCT_ID,
    COALESCE(s.MODEL, dtl.MODEL) AS MODEL,
    COALESCE(s.VAR_SEGMENT, dtl.VAR_SEGMENT) as PD_BASEL_SEG_NUM,
    '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}' AS STREAM
FROM features.BASEL_ACCT_ID AS pop
LEFT JOIN segments AS s
    ON s.basel_acct_id = pop.basel_acct_id
LEFT JOIN dtl AS dtl
    ON dtl.basel_acct_id = pop.basel_acct_id
WHERE pop.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'