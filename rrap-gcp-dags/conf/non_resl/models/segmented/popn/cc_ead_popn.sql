WITH
    rvl AS (
        SELECT
            basel_acct_id,
            CASE
                WHEN prim_basel_cust_id <= 0 THEN NULL
                ELSE prim_basel_cust_id
            END AS BASEL_CUST_ID
        FROM
            ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT
        WHERE
            MTH_TM_ID = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}
    ),
    PIT_STATUS AS (
        SELECT
            *
        FROM
            features.PIT_STATUS_CROSS_DEFAULT_ORIG
        WHERE
            obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
    ),
    TREATMENT_F AS (
        SELECT 
            *
        FROM
            features.TREATMENT_F
        WHERE
            obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
    ),
    SUB_PPLTN_FLAG AS (
        SELECT 
            *
        FROM
            features.SUB_PPLTN_FLAG
        WHERE
            obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
    ),
    CONSM_PRD_TREATMNT_CD AS (
        SELECT 
            *
        FROM 
            features.CONSM_PRD_TREATMNT_CD
        WHERE
            obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
    ),
    BASEL_PRD_CD as (
        SELECT
            * 
        FROM 
            features.BASEL_PRD_CD 
        WHERE 
            obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
    ),
    HELOC_F AS (
        SELECT 
            *
        FROM 
            features.HELOC_F
        WHERE
            obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
    ),
    MODEL_EXCL_F AS (
        SELECT
            * 
        FROM 
            features.MODEL_EXCL_F
        WHERE
            obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
    )
SELECT
    main.*,
    PIT_STATUS.PIT_STATUS_CROSS_DEFAULT_ORIG,
    BASEL_PRD_CD.BASEL_PRD_CD,
    CONSM_PRD_TREATMNT_CD.CONSM_PRD_TREATMNT_CD,
    HELOC_F.HELOC_F,
    TREATMENT_F.TREATMENT_F,
    SUB_PPLTN_FLAG.SUB_PPLTN_FLAG,
    MODEL_EXCL_F.MODEL_EXCL_F
    
FROM
    rvl AS main
    LEFT JOIN BASEL_PRD_CD ON main.BASEL_ACCT_ID = BASEL_PRD_CD.BASEL_ACCT_ID
    LEFT JOIN PIT_STATUS AS PIT_STATUS ON main.BASEL_ACCT_ID = PIT_STATUS.BASEL_ACCT_ID
    LEFT JOIN TREATMENT_F AS TREATMENT_F ON main.BASEL_ACCT_ID = TREATMENT_F.BASEL_ACCT_ID
    LEFT JOIN SUB_PPLTN_FLAG AS SUB_PPLTN_FLAG ON main.BASEL_ACCT_ID = SUB_PPLTN_FLAG.BASEL_ACCT_ID
    LEFT JOIN CONSM_PRD_TREATMNT_CD AS CONSM_PRD_TREATMNT_CD ON main.BASEL_ACCT_ID = CONSM_PRD_TREATMNT_CD.BASEL_ACCT_ID
    LEFT JOIN HELOC_F AS HELOC_F ON main.BASEL_ACCT_ID = HELOC_F.BASEL_ACCT_ID
    LEFT JOIN MODEL_EXCL_F AS MODEL_EXCL_F ON main.BASEL_ACCT_ID = MODEL_EXCL_F.BASEL_ACCT_ID

    
WHERE
    TRIM(BASEL_PRD_CD.BASEL_PRD_CD) = 'CC'
    AND TRIM(PIT_STATUS.PIT_STATUS_CROSS_DEFAULT_ORIG) IN ('CUR', 'DEF')
    AND HELOC_F = 'N'
    AND CONSM_PRD_TREATMNT_CD.CONSM_PRD_TREATMNT_CD = 'A'