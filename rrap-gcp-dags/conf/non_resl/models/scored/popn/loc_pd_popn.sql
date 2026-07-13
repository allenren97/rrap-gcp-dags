WITH
            rvl AS (
                SELECT
                    basel_acct_id,
                    CASE
                        WHEN prim_basel_cust_id <= 0 THEN NULL
                        ELSE prim_basel_cust_id
                    END AS BASEL_CUST_ID,
                    nullif(trim(step_pln_agrmnt_num), '') as step_pln_agrmnt_num
                FROM
                    ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT
                WHERE MTH_TM_ID = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}
            ),
            BASEL_PRD_CD as (
                select * from features.BASEL_PRD_CD where obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
            ),
            CONSM_SCORECRD_EXCLSN_F as (
                select * from features.CONSM_SCORECRD_EXCLSN_F where obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
            ),
            HELOC_F as (
                select * from features.HELOC_F where obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
            ),
            PIT_STATUS as (
                select * from features.PIT_STATUS_CROSS_DEFAULT_ORIG where obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
            ),
            CONSM_PRD_TREATMNT_CD  as (
                select * from features.CONSM_PRD_TREATMNT_CD where obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
            )
        SELECT
            main.*,
            BASEL_PRD_CD.BASEL_PRD_CD,
            CONSM_SCORECRD_EXCLSN_F.CONSM_SCORECRD_EXCLSN_F,
            PIT_STATUS.PIT_STATUS_CROSS_DEFAULT_ORIG,
            HELOC_F.HELOC_F,
            CONSM_PRD_TREATMNT_CD.CONSM_PRD_TREATMNT_CD
        FROM
            rvl AS main
            LEFT JOIN BASEL_PRD_CD ON main.BASEL_ACCT_ID = BASEL_PRD_CD.BASEL_ACCT_ID
            LEFT JOIN CONSM_SCORECRD_EXCLSN_F ON main.BASeL_ACCT_ID = CONSM_SCORECRD_EXCLSN_F.BASEL_ACCT_ID
            LEFT JOIN HELOC_F AS HELOC_F ON main.BASeL_ACCT_ID = HELOC_F.BASEL_ACCT_ID
            LEFT JOIN PIT_STATUS ON main.BASeL_ACCT_ID = PIT_STATUS.BASEL_ACCT_ID
            LEFT JOIN CONSM_PRD_TREATMNT_CD AS CONSM_PRD_TREATMNT_CD ON main.BASeL_ACCT_ID = CONSM_PRD_TREATMNT_CD.BASEL_ACCT_ID
        WHERE
            TRIM(BASEL_PRD_CD.BASEL_PRD_CD) = 'LOC'
            AND TRIM(CONSM_SCORECRD_EXCLSN_F.CONSM_SCORECRD_EXCLSN_F) = 'N'
            AND TRIM(HELOC_F.HELOC_F) = 'N'
            AND TRIM(PIT_STATUS.PIT_STATUS_CROSS_DEFAULT_ORIG) = 'CUR'
            AND TRIM(CONSM_PRD_TREATMNT_CD.CONSM_PRD_TREATMNT_CD) = 'A'