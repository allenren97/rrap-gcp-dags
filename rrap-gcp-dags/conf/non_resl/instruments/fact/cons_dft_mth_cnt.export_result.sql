WITH get_pop AS (

    {% set upstream_asset = [
        "ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT",
        "ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT",
        "ingestion.MORT_MTH_SNAPSHOT",
        "ingestion.TNG_ACCT_MO",
        "ingestion.BASEL_ACCT_DIM",
        "features.MONTH_DEF",
        "features.MONTH_DEF_24M"
    ] %}

    -- KS
    SELECT
        BASEL_ACCT_ID,
        'KS' AS SRC_SYS_CD
    FROM {{ upstream_asset[0] }}
    WHERE MTH_TM_ID = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}

    UNION

    -- MOR
    SELECT
        BASEL_ACCT_ID,
        'MOR' AS SRC_SYS_CD
    FROM {{ upstream_asset[2] }}
    WHERE MTH_TM_ID = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}

    UNION

    -- SPL
    SELECT
        BASEL_ACCT_ID,
        'SPL' AS SRC_SYS_CD
    FROM {{ upstream_asset[1] }}
    WHERE MTH_TM_ID = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}

    UNION

    -- TNG-MOR
    SELECT
        dim.BASEL_ACCT_ID,
        'TNG-MOR' AS SRC_SYS_CD
    FROM {{ upstream_asset[4] }} dim
    INNER JOIN {{ upstream_asset[3] }} tng
        ON dim.SRC_APP_CD = 'TNG-MOR'
        AND dim.SRC_SYS_DEL_F != 'Y'
        AND TRIM(tng.ACCOUNT_ID) = TRIM(dim.SRC_APP_ID)
    WHERE tng.MONTH_END_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
),

final AS (
    SELECT
        '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' AS OBSN_DT,
        pop.BASEL_ACCT_ID,
        pop.SRC_SYS_CD,
        '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}' AS STREAM,

        CASE
            WHEN pop.SRC_SYS_CD = 'SPL'
                THEN md.MONTH_DEF
            WHEN pop.SRC_SYS_CD IN ('KS', 'MOR', 'TNG-MOR')
                THEN md24.MONTH_DEF_24M
            ELSE NULL
        END AS CONS_DFT_MTH_CNT

    FROM get_pop pop

    -- SPL : MONTH_DEF
    LEFT JOIN {{ upstream_asset[5] }} md
        ON pop.BASEL_ACCT_ID = md.BASEL_ACCT_ID
        AND md.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
        AND md.SRC_SYS_CD = pop.SRC_SYS_CD

    -- KS, MOR, TNG : MONTH_DEF_24M
    LEFT JOIN {{ upstream_asset[6] }} md24
        ON pop.BASEL_ACCT_ID = md24.BASEL_ACCT_ID
        AND md24.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
        AND md24.SRC_SYS_CD = pop.SRC_SYS_CD
)

SELECT *
FROM final