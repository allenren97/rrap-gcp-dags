WITH
        main AS (
            SELECT
                basel_acct_id,
                prim_basel_cust_id AS BASEL_CUST_ID
            FROM
                ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT
            WHERE
                MTH_TM_ID = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}
        ),
        BASEL_PRD_CD as (
                    SELECT
                        * 
                    FROM 
                        features.BASEL_PRD_CD 
                    WHERE 
                        obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
        ),
        PIT_STATUS AS (
            SELECT
                *
            FROM
                features.PIT_STATUS_CROSS_DEFAULT_ORIG
            WHERE
                obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
        ),
        CONSM_SCORECRD_EXCLSN_F AS (
        SELECT 
            *
        FROM 
            features.CONSM_SCORECRD_EXCLSN_F
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
        TRANSACTOR_F AS (
            SELECT
                *
            FROM
                features.TRANSACTOR_F
            WHERE
                obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
        ),
        HELOC_F as (
            SELECT
                *
            FROM
                features.HELOC_F
            WHERE
                obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
        )
        SELECT
            main.*,
            BASEL_PRD_CD.BASEL_PRD_CD,
            PIT_STATUS.PIT_STATUS_CROSS_DEFAULT_ORIG,
            CONSM_SCORECRD_EXCLSN_F.CONSM_SCORECRD_EXCLSN_F,
            CONSM_PRD_TREATMNT_CD.CONSM_PRD_TREATMNT_CD,
            TRANSACTOR_F.TRANSACTOR_F,
            HELOC_F.HELOC_F
        FROM
            main
            LEFT JOIN BASEL_PRD_CD ON main.BASEL_ACCT_ID = BASEL_PRD_CD.BASEL_ACCT_ID
            LEFT JOIN PIT_STATUS AS PIT_STATUS ON main.BASEL_ACCT_ID = PIT_STATUS.BASEL_ACCT_ID
            LEFT JOIN CONSM_SCORECRD_EXCLSN_F AS CONSM_SCORECRD_EXCLSN_F ON main.BASEL_ACCT_ID = CONSM_SCORECRD_EXCLSN_F.BASEL_ACCT_ID
            LEFT JOIN CONSM_PRD_TREATMNT_CD AS CONSM_PRD_TREATMNT_CD ON main.BASEL_ACCT_ID = CONSM_PRD_TREATMNT_CD.BASEL_ACCT_ID
            LEFT JOIN TRANSACTOR_F AS TRANSACTOR_F ON main.BASEL_ACCT_ID = TRANSACTOR_F.BASEL_ACCT_ID
            LEFT JOIN HELOC_F AS HELOC_F ON main.BASEL_ACCT_ID = HELOC_F.BASEL_ACCT_ID
        WHERE
            TRIM(BASEL_PRD_CD.BASEL_PRD_CD) = 'LOC'
            AND TRIM(PIT_STATUS.PIT_STATUS_CROSS_DEFAULT_ORIG) IN ('CUR', 'DEF')
            AND CONSM_SCORECRD_EXCLSN_F.CONSM_SCORECRD_EXCLSN_F IN ('N', 'Y')
            AND CONSM_PRD_TREATMNT_CD.CONSM_PRD_TREATMNT_CD = 'A'
            AND TRANSACTOR_F.TRANSACTOR_F IN ('N', 'T')
            AND HELOC_F.HELOC_F = 'N'