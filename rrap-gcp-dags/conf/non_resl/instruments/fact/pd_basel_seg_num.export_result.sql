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
),
full AS (
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
)
-- set missing pit status or excluded status to 98 to match old sas logic
SELECT
    f.OBSN_DT,
    f.BASEL_ACCT_ID,
    f.MODEL,
    CASE
        WHEN f.PD_BASEL_SEG_NUM IS NOT NULL OR f.MODEL IS NOT NULL OR src.SRC_SYS_CD IS DISTINCT FROM 'MOR' THEN f.PD_BASEL_SEG_NUM
        WHEN pit.PIT_STATUS_ACCOUNT_ORIG IS NULL THEN 98
        WHEN mort.CRNT_BAL_AMT <= 0 AND mort.TOT_SUSP_BAL_AMT = 0 AND mort.PD_OFF_F = 'Y' THEN 98
        ELSE f.PD_BASEL_SEG_NUM
    END AS PD_BASEL_SEG_NUM,
    f.STREAM
FROM
    full AS f
LEFT JOIN features.SRC_SYS_CD AS src
    ON f.BASEL_ACCT_ID = src.BASEL_ACCT_ID
    AND f.OBSN_DT = src.OBSN_DT
LEFT JOIN features.PIT_STATUS_ACCOUNT_ORIG AS pit
    ON f.BASEL_ACCT_ID = pit.BASEL_ACCT_ID
    AND f.OBSN_DT = pit.OBSN_DT
LEFT JOIN (
    SELECT BASEL_ACCT_ID, CRNT_BAL_AMT, TOT_SUSP_BAL_AMT, PD_OFF_F
    FROM ingestion.MORT_MTH_SNAPSHOT
    WHERE MTH_TM_ID = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}
    ) AS mort
    ON f.BASEL_ACCT_ID = mort.BASEL_ACCT_ID