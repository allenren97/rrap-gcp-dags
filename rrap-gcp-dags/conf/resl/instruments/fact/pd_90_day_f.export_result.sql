SELECT
    main.BASEL_ACCT_ID,
    main.SRC_SYS_CD,
    CASE
        WHEN main.src_sys_cd = 'KS' THEN CASE
            WHEN TRIM(BASEL_PRD_CD) = 'CC'
            AND TRIM(HELOC_F) = 'N'
            AND TRIM(CONSM_PRD_TREATMNT_CD_IF) = 'A' THEN CASE
                WHEN (
                    TRIM(CONSM_SCORECRD_EXCLSN_F) = 'Y'
                    AND TRIM(PIT_STAT_CD) = 'CUR'
                )
                OR TRIM(PIT_STAT_CD) = 'DEF' THEN 'True'
                ELSE 'False'
            END
            WHEN TRIM(CONSM_PRD_TREATMNT_CD_IF) = 'A'
            AND (
                (
                    TRIM(BASEL_PRD_CD) = 'CC'
                    OR TRIM(HELOC_F) != 'N'
                )
                OR TRIM(BASEL_PRD_CD) != 'CC'
            ) THEN CASE
                WHEN PD_FINAL_RPTG_RTO >= 1 THEN 'True'
                ELSE 'False'
            END
        END
        WHEN main.src_sys_cd IN ('MOR', 'TNG-MOR', 'SPL') THEN CASE
            WHEN TRIM(CONSM_PRD_TREATMNT_CD_IF) = 'A'
            AND PD_FINAL_RPTG_RTO >= 1 THEN 'True'
            WHEN TRIM(CONSM_PRD_TREATMNT_CD_IF) = 'A'
            AND (
                PD_FINAL_RPTG_RTO < 1
                OR PD_FINAL_RPTG_RTO IS NULL
            ) THEN 'False'
        END
    END AS PD_90_DAY_F
FROM
    features.basel_acct_id AS main
    LEFT JOIN instruments.PD_FINAL_RPTG_RTO ON PD_FINAL_RPTG_RTO.basel_acct_id = main.basel_acct_id
    AND PD_FINAL_RPTG_RTO.obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
    AND PD_FINAL_RPTG_RTO.stream = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
    LEFT JOIN features.HELOC_F AS HELOC_F ON HELOC_F.basel_acct_id = main.basel_acct_id
    AND HELOC_F.obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
    LEFT JOIN features.BASEL_PRD_CD AS BASEL_PRD_CD ON BASEL_PRD_CD.basel_acct_id = main.basel_acct_id
    AND BASEL_PRD_CD.obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
    LEFT JOIN instruments.PIT_STAT_CD AS PIT_STAT_CD ON PIT_STAT_CD.basel_acct_id = main.basel_acct_id
    AND PIT_STAT_CD.obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
    AND PIT_STAT_CD.stream = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
    LEFT JOIN features.CONSM_PRD_TREATMNT_CD_IF AS CONSM_PRD_TREATMNT_CD ON CONSM_PRD_TREATMNT_CD.basel_acct_id = main.basel_acct_id
    AND CONSM_PRD_TREATMNT_CD.obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
    LEFT JOIN features.CONSM_SCORECRD_EXCLSN_F AS CONSM_SCORECRD_EXCLSN_F ON CONSM_SCORECRD_EXCLSN_F.basel_acct_id = main.basel_acct_id
    AND CONSM_SCORECRD_EXCLSN_F.obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
WHERE
    main.obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'