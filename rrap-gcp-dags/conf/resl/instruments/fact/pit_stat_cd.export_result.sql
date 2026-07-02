WITH get_pop as (
    {% set upstream_asset = [
        "ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT",
        "ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT",
        "ingestion.MORT_MTH_SNAPSHOT",
        "ingestion.TNG_ACCT_MO",
        "ingestion.BASEL_ACCT_DIM",
        "features.PIT_STATUS_STEP",
        "features.PIT_STATUS_ACCOUNT",
        "features.TNG_PIT_STATUS_CD"
        ]%}

        SELECT
            BASEL_ACCT_ID,
            'MOR' AS SRC_SYS_CD,
            STEP_PLN_AGRMNT_NUM
        FROM {{upstream_asset[2]}}
        WHERE MTH_TM_ID = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}
        UNION

        SELECT
            BASEL_ACCT_ID,
            'KS' AS SRC_SYS_CD,
            STEP_PLN_AGRMNT_NUM
        FROM {{upstream_asset[0]}}
        WHERE MTH_TM_ID = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}
        UNION

        SELECT
            BASEL_ACCT_ID,
            'TNG-MOR' AS SRC_SYS_CD,
            NULL AS STEP_PLN_AGRMNT_NUM
        FROM {{upstream_asset[4]}} dim
        LEFT JOIN {{upstream_asset[3]}} tng 
        ON dim.SRC_APP_CD = 'TNG-MOR'
            AND dim.SRC_SYS_DEL_F != 'Y'
            AND TRIM(tng.ACCOUNT_ID) = TRIM(dim.SRC_APP_ID)
        
        WHERE tng.MONTH_END_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
        UNION
        
        SELECT
            BASEL_ACCT_ID,
            'SPL' AS SRC_SYS_CD,
            STEP_PLN_AGRMNT_NUM
        FROM {{upstream_asset[1]}}
        WHERE MTH_TM_ID = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}
    )
    SELECT
    '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate")}}' as OBSN_DT,
    pop.BASEL_ACCT_ID,
    pop.SRC_SYS_CD,
    '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}' as STREAM,
    CASE
        WHEN pop.SRC_SYS_CD = 'TNG-MOR' THEN TNG_PIT_STATUS_CD
        WHEN pop.STEP_PLN_AGRMNT_NUM IS NULL THEN PIT_STATUS_ACCOUNT
        ELSE PIT_STATUS_STEP
        END AS PIT_STAT_CD
    FROM get_pop as pop
    LEFT JOIN {{upstream_asset[5]}} as step on pop.BASEL_ACCT_ID = step.BASEL_ACCT_ID AND step.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate")}}'
    LEFT JOIN {{upstream_asset[6]}} as acc on pop.BASEL_ACCT_ID = acc.BASEL_ACCT_ID AND acc.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate")}}'
    LEFT JOIN {{upstream_asset[7]}} as tng on pop.BASEL_ACCT_ID = tng.BASEL_ACCT_ID AND tng.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate")}}'