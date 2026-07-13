WITH
    ks AS (
        SELECT
            main.BASEL_ACCT_ID,
            NULLIF(TRIM(main.STEP_PLN_AGRMNT_NUM), '') AS STEP_PLN_AGRMNT_NUM,
            'KS' AS SRC_SYS_CD,
        FROM
            ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT AS main
        WHERE
            main.MTH_TM_ID = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}
    ),
    mor AS (
        SELECT
            main.BASEL_ACCT_ID,
            NULLIF(TRIM(main.STEP_PLN_AGRMNT_NUM), '') AS STEP_PLN_AGRMNT_NUM,
            'MOR' AS SRC_SYS_CD,
        FROM
            ingestion.MORT_MTH_SNAPSHOT AS main
        WHERE
            main.MTH_TM_ID = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}
    ),
    spl AS (
        SELECT
            main.BASEL_ACCT_ID,
            NULLIF(TRIM(main.STEP_PLN_AGRMNT_NUM), '') AS STEP_PLN_AGRMNT_NUM,
            'SPL' AS SRC_SYS_CD,
        FROM
            ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT AS main
        WHERE
            main.MTH_TM_ID = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}
    ),
    tng AS (
        SELECT
            dim.basel_acct_id,
            NULL AS STEP_PLN_AGRMNT_NUM,
            'TNG-MOR' AS SRC_SYS_CD
        FROM
            ingestion.tng_acct_mo AS main
            LEFT JOIN ingestion.basel_acct_dim AS dim ON TRIM(dim.SRC_APP_CD) = 'TNG-MOR'
            AND dim.SRC_SYS_DEL_F != 'Y'
            AND TRIM(main.ACCOUNT_ID) = TRIM(dim.SRC_APP_ID)
        WHERE
            main.month_end_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
    ),
    uni AS ( -- 
        SELECT
            *
        FROM
            ks
        UNION ALL
        SELECT
            *
        FROM
            mor
        UNION ALL
        SELECT
            *
        FROM
            spl
        UNION ALL
        SELECT
            *
        FROM
            tng
    ),
    rntl_fact AS (
        SELECT
            main.BASEL_ACCT_ID,
            main.SRC_SYS_CD,
            main.STEP_PLN_AGRMNT_NUM,
            TRIM(RNTL_INCM_DPNDCY_F) AS RNTL_INCM_DPNDCY_F
        FROM
            uni AS main
            LEFT JOIN ingestion.BASEL_ACCT_PRFM_FACT fact ON main.BASEL_ACCT_ID = fact.BASEL_ACCT_ID
            AND fact.MTH_END_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
    )
SELECT
    main.BASEL_ACCT_ID,
    main.SRC_SYS_CD,
    CASE
        WHEN xstep.STEP_PLN_AGRMNT_NUM IS NOT NULL THEN 'Y'
        WHEN TRIM(RNTL_INCM_DPNDCY_F) IS NOT NULL THEN TRIM(RNTL_INCM_DPNDCY_F)
        ELSE NULL
    END AS RNTL_PRPTY_F
FROM
    rntl_fact AS main
    LEFT JOIN (
        SELECT DISTINCT
            STEP_PLN_AGRMNT_NUM
        FROM
            rntl_fact
        WHERE
            RNTL_INCM_DPNDCY_F = 'Y'
            AND STEP_PLN_AGRMNT_NUM IS NOT NULL
    ) AS xstep ON xstep.STEP_PLN_AGRMNT_NUM = main.STEP_PLN_AGRMNT_NUM