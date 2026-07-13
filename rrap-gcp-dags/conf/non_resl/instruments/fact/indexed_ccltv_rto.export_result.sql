WITH
    ks AS (
        SELECT
            main.BASEL_ACCT_ID,
            NULLIF(TRIM(main.STEP_PLN_AGRMNT_NUM), '') AS STEP_PLN_AGRMNT_NUM,
            'KS' AS SRC_SYS_CD,
            CASE
                WHEN main.ACCT_OPND_DT >= '2016-11-01'
                AND HELOC_F = 'Y' THEN 'N'
            END AS excluded
        FROM
            ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT AS main
            LEFT JOIN features.LTV_TP_CD AS cd ON cd.basel_acct_id = main.basel_acct_id
            AND cd.obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
            LEFT JOIN features.HELOC_F AS h ON h.basel_acct_id = main.basel_acct_id
            AND h.obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
        WHERE
            main.MTH_TM_ID = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}
    ),
    mor AS (
        SELECT
            main.BASEL_ACCT_ID,
            NULLIF(TRIM(main.STEP_PLN_AGRMNT_NUM), '') AS STEP_PLN_AGRMNT_NUM,
            'MOR' AS SRC_SYS_CD,
            CASE
                WHEN (
                    CASE
                        WHEN main.mth_tm_id >= 17156 THEN UPPER(sc.SCRTY_TP_DESC) IN ('UNINSURED', 'INSURED')
                        ELSE UPPER(sc.SCRTY_TP_DESC) IN ('UNINSURED')
                    END
                )
                AND (
                    main.LAST_RNEW_DT >= '2016-11-01'
                    OR main.INTR_ADJ_DT >= '2016-11-01'
                )
                AND NULLIF(TRIM(main.STEP_PLN_AGRMNT_NUM), '') IS NOT NULL THEN 'N'
            END AS excluded
        FROM
            ingestion.MORT_MTH_SNAPSHOT AS main
            LEFT JOIN features.SCRTY_TP_DESC AS sc ON sc.basel_acct_id = main.basel_acct_id
            AND sc.obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
        WHERE
            main.MTH_TM_ID = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}
    ),
    spl AS (
        SELECT
            main.BASEL_ACCT_ID,
            NULLIF(TRIM(main.STEP_PLN_AGRMNT_NUM), '') AS STEP_PLN_AGRMNT_NUM,
            'SPL' AS SRC_SYS_CD,
            CASE
                WHEN main.NOTE_DT >= '2016-11-01'
                AND UPPER(p.PRD_ID) IN ('S05', 'S08')
                AND main.RECD_STAT_CD IN (4, 5, 6, 7, 8) THEN 'N'
            END AS excluded
        FROM
            ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT AS main
            LEFT JOIN features.PRD_ID AS p ON p.basel_acct_id = main.basel_acct_id
            AND p.obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
        WHERE
            main.MTH_TM_ID = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}
    ),
    tng AS (
        SELECT
            dim.basel_acct_id,
            NULL AS STEP_PLN_AGRMNT_NUM,
            'TNG-MOR' AS SRC_SYS_CD,
            CASE
                WHEN (
                    OPEN_DT >= '2016-11-01'
                    OR LAST_RENEWAL_DT >= '2016-11-01'
                )
                AND (
                    CASE
                        WHEN main.month_end_dt >= '2017-11-30' THEN UPPER(sc.SCRTY_TP_DESC) IN ('UNINSURED', 'INSURED')
                        ELSE UPPER(sc.SCRTY_TP_DESC) IN ('UNINSURED')
                    END
                ) THEN 'N'
            END AS excluded
        FROM
            ingestion.tng_acct_mo AS main
            LEFT JOIN ingestion.basel_acct_dim AS dim ON TRIM(dim.SRC_APP_CD) = 'TNG-MOR'
            AND dim.SRC_SYS_DEL_F != 'Y'
            AND TRIM(main.ACCOUNT_ID) = TRIM(dim.SRC_APP_ID)
            LEFT JOIN features.SCRTY_TP_DESC AS sc ON sc.basel_acct_id = dim.basel_acct_id
            AND sc.obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
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
    sum_ead AS (
        SELECT
            main.basel_acct_id,
            main.STEP_PLN_AGRMNT_NUM,
            SUM(EXPSR_AT_DFT_RTO * MAX_ACCT_BAL_AMT) OVER (
                PARTITION BY
                    STEP_PLN_AGRMNT_NUM
                ORDER BY
                    STEP_PLN_AGRMNT_NUM,
                    ACCT_SENRTY_CD,
                    CRNT_PRPTY_VAL_AMT desc
                    -- ROWS BETWEEN unbounded PRECEDING
                    -- AND CURRENT ROW
            ) AS SUM_EAD_DOLLAR,
            EXPSR_AT_DFT_RTO,
            MAX_ACCT_BAL_AMT,
            ACCT_SENRTY_CD,
            excluded
        FROM
            uni AS main
            LEFT JOIN features.ACCT_SENRTY_CD AS acct_cd ON acct_cd.basel_acct_id = main.basel_acct_id
            AND acct_cd.obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
            LEFT JOIN features.MAX_ACCT_BAL_AMT AS max_bal ON max_bal.basel_acct_id = main.basel_acct_id
            AND max_bal.obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
            LEFT JOIN instruments.EXPSR_AT_DFT_RTO AS expsr ON expsr.basel_acct_id = main.basel_acct_id
            AND expsr.obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
            AND expsr.stream = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
            LEFT JOIN instruments.CRNT_PRPTY_VAL_AMT AS prop ON prop.basel_acct_id = main.basel_acct_id
            AND prop.obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
            AND prop.stream = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
        WHERE
            main.excluded = 'N'
            -- OR (
            --     acct_senrty_cd = 1
            --     AND src_sys_cd = 'MOR'
            -- )
    )
SELECT
    main.BASEL_ACCT_ID,
    main.CRNT_LTV_RTO,
    uni.STEP_PLN_AGRMNT_NUM,
    sum_ead.*,
    CRNT_PRPTY_VAL_AMT,
    CASE
        WHEN uni.STEP_PLN_AGRMNT_NUM IS NULL
        AND sum_ead.STEP_PLN_AGRMNT_NUM IS NULL THEN crnt_ltv_rto
        WHEN CRNT_PRPTY_VAL_AMT IS NULL
        AND SUM_EAD_DOLLAR IS NOT NULL THEN crnt_ltv_rto
        ELSE SUM_EAD_DOLLAR / CRNT_PRPTY_VAL_AMT
    END AS INDEXED_CCLTV_RTO
FROM
    instruments.CRNT_LTV_RTO AS main
    LEFT JOIN sum_ead ON sum_ead.basel_acct_id = main.basel_acct_id
    LEFT JOIN uni ON uni.basel_acct_id = main.basel_acct_id
    LEFT JOIN instruments.CRNT_PRPTY_VAL_AMT AS prop ON prop.basel_acct_id = sum_ead.basel_acct_id
    AND prop.obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
    AND prop.stream = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
WHERE
    main.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
    AND main.STREAM = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
ORDER BY
    uni.STEP_PLN_AGRMNT_NUM