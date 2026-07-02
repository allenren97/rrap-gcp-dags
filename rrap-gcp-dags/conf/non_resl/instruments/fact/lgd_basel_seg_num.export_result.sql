WITH segments AS (
    {% set tables = [
        "cc",
        "loc",
        "heloc",
        "mor",
        "itl",
        "dtl",
        "ssla",
        "sslb",
        "tng_mor"
    ] %}

    {% for table in tables %}

    SELECT
        lgdnd.OBSN_DT AS OBSN_DT,
        lgdnd.BASEL_ACCT_ID AS BASEL_ACCT_ID,
        '{{ table }}_lgdnd' AS MODEL,
        lgdnd.VAR_SEGMENT AS LGD_BASEL_SEG_NUM
    FROM models.{{ table }}_lgdnd_segment AS lgdnd
    WHERE lgdnd.obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'

    UNION ALL

    SELECT 
        lgdd.OBSN_DT,
        lgdd.BASEL_ACCT_ID,
        '{{ table }}_lgdd' AS MODEL,
        lgdd.VAR_SEGMENT AS LGD_BASEL_SEG_NUM
    FROM models.{{ table }}_lgdd_segment AS lgdd
    WHERE lgdd.obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'

    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
SELECT 
    pop.OBSN_DT,
    pop.BASEL_ACCT_ID,
    s.MODEL,
    s.LGD_BASEL_SEG_NUM,
    'NON_RESL' AS STREAM
FROM features.BASEL_ACCT_ID AS pop
LEFT JOIN segments AS s
    ON pop.basel_acct_id = s.basel_acct_id
WHERE pop.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'