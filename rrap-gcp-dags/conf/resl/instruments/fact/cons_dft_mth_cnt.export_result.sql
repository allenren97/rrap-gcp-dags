WITH get_pop AS (
    {% set upstream_asset = [
        "ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT",
        "ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT",
        "ingestion.MORT_MTH_SNAPSHOT",
        "ingestion.TNG_ACCT_MO",
        "ingestion.BASEL_ACCT_DIM",
        "features.STEP_MONTH_DEF_SINCE_LAST_DEF",
        "features.STEP_STANDALONE_MONTH_DEF_SINCE_LAST_DEF",
        "features.MONTH_DEF_24M"
    ] %}

    -- KS
    SELECT BASEL_ACCT_ID, 'KS' AS SRC_SYS_CD, STEP_PLN_AGRMNT_NUM
    FROM {{ upstream_asset[0] }}
    WHERE MTH_TM_ID = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}

    UNION

    -- MOR
    SELECT BASEL_ACCT_ID, 'MOR' AS SRC_SYS_CD, STEP_PLN_AGRMNT_NUM
    FROM {{ upstream_asset[2] }}
    WHERE MTH_TM_ID = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}

    UNION

    -- SPL
    SELECT BASEL_ACCT_ID, 'SPL' AS SRC_SYS_CD, STEP_PLN_AGRMNT_NUM
    FROM {{ upstream_asset[1] }}
    WHERE MTH_TM_ID = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}

    UNION

    -- TNG-MOR
    SELECT
        BASEL_ACCT_ID,
        'TNG-MOR' AS SRC_SYS_CD,
        NULL AS STEP_PLN_AGRMNT_NUM
    FROM {{ upstream_asset[4] }} dim
    LEFT JOIN {{ upstream_asset[3] }} tng
        ON dim.SRC_APP_CD = 'TNG-MOR'
        AND dim.SRC_SYS_DEL_F != 'Y'
        AND TRIM(tng.ACCOUNT_ID) = TRIM(dim.SRC_APP_ID)
    WHERE tng.MONTH_END_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
)

SELECT
    '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' AS OBSN_DT,
    pop.BASEL_ACCT_ID,
    pop.SRC_SYS_CD,
    '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}' AS STREAM,

    /* 
    Logic:
    - TNG-MOR uses regular MONTH_DEF_24M (non-resl only)
    - If STEP is NULL → use standalone feature
    - If STEP exists → use STEP-level feature
    */
    CASE
        WHEN pop.SRC_SYS_CD = 'TNG-MOR'
            THEN tng_feat.MONTH_DEF_24M

        WHEN pop.STEP_PLN_AGRMNT_NUM IS NULL
            THEN stand_feat.STEP_STANDALONE_MONTH_DEF_SINCE_LAST_DEF

        ELSE
            step_feat.STEP_MONTH_DEF_SINCE_LAST_DEF
    END AS CONS_DFT_MTH_CNT

FROM get_pop AS pop

LEFT JOIN {{ upstream_asset[5] }} step_feat
    ON pop.STEP_PLN_AGRMNT_NUM = step_feat.STEP_PLN_AGRMNT_NUM
    AND step_feat.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'

LEFT JOIN {{ upstream_asset[6] }} stand_feat
    ON pop.BASEL_ACCT_ID = stand_feat.BASEL_ACCT_ID
    AND stand_feat.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'

LEFT JOIN {{ upstream_asset[7] }} tng_feat
    ON pop.BASEL_ACCT_ID = tng_feat.BASEL_ACCT_ID
    AND tng_feat.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
    AND tng_feat.SRC_SYS_CD = 'TNG-MOR'