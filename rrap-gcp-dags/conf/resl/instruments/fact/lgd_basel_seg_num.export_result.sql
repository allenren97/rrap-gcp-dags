WITH segments AS (
    {% set tables = [
        "step_heloc",
        "step_mix_mor",
        "standalone_mor",
        "standalone_heloc",
        "cc",
        "itl",
        "loc",
        "ssla",
        "sslb",
        "tng_mor",
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
),
dtl AS (
    SELECT
        lgdnd.OBSN_DT AS OBSN_DT,
        lgdnd.BASEL_ACCT_ID AS BASEL_ACCT_ID,
        'dtl_lgdnd' AS MODEL,
        lgdnd.VAR_SEGMENT AS LGD_BASEL_SEG_NUM
    FROM models.dtl_lgdnd_segment AS lgdnd
    WHERE lgdnd.obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'

    UNION ALL

    SELECT 
        lgdd.OBSN_DT,
        lgdd.BASEL_ACCT_ID,
        'dtl_lgdd' AS MODEL,
        lgdd.VAR_SEGMENT AS LGD_BASEL_SEG_NUM
    FROM models.dtl_lgdd_segment AS lgdd
    WHERE lgdd.obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
)
SELECT
    pop.OBSN_DT,
    pop.BASEL_ACCT_ID,
    COALESCE(s.MODEL, dtl.MODEL) AS MODEL,
    COALESCE(s.LGD_BASEL_SEG_NUM, dtl.LGD_BASEL_SEG_NUM) AS LGD_BASEL_SEG_NUM,
    'RESL' AS STREAM
FROM features.BASEL_ACCT_ID AS pop
LEFT JOIN segments AS s
    ON pop.basel_acct_id = s.basel_acct_id
LEFT JOIN dtl AS dtl
    ON pop.basel_acct_id = dtl.basel_acct_id
WHERE pop.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'