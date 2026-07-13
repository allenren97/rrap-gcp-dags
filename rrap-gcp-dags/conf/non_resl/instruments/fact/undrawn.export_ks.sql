WITH RPTG_PRD_LKP1 AS(
    SELECT
        TRIM(rptg.PRD_ID) AS PRD_ID,
        TRIM(rptg.BASEL_SUB_PRD_NM) AS BASEL_SUB_PRD_NM,
        TRIM(rptg.PRD_CD) AS PRD_CD,
        TRIM(rptg.SUB_PRD_CD) AS SUB_PRD_CD0,
        TRIM(rptg.REVISED_EXPSR_OV_125K_F) AS REVISED_EXPSR_OV_125K_F,
        TRIM(rptg.HELOC_F) AS HELOC_F,
        TRIM(rptg.BASEL_PRD_CD) AS BASEL_PRD_CD,
        TRIM(rptg.SRC_SYS_CD) AS SRC_SYS_CD,
    FROM reference.BASEL_RPTG_PRD_LKP rptg
    LEFT JOIN reference.BASEL_EGL_LKP_NZ egl ON
        LTRIM(RTRIM(rptg.PRD_ID)) = LTRIM(RTRIM(egl.PRD_CD))
    WHERE 
        TRIM(rptg.SRC_SYS_CD) = 'KS'
        AND '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="yyyymm") }}' BETWEEN egl.EFF_FROM_YR_MTH AND egl.EFF_TO_YR_MTH
    )
    SELECT
        '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' AS OBSN_DT,
        ks.BASEL_ACCT_ID,
        'KS' AS SRC_SYS_CD,
        CASE 
            WHEN TRIM(pd.PD_BAND) = '26'
            THEN 0
            ELSE afz.AF_ZERO_NET_UNDRAWN_AMT
        END AS UNDRAWN
    FROM ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT ks
    LEFT JOIN (SELECT * FROM features.HELOC_F WHERE OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}') heloc ON
        ks.BASEL_ACCT_ID = heloc.BASEL_ACCT_ID
    LEFT JOIN (SELECT * FROM features.BASEL_PRD_CD WHERE OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}') prd_cd ON
        ks.BASEL_ACCT_ID = prd_cd.BASEL_ACCT_ID
    LEFT JOIN (SELECT * FROM features.TOTAL_EXPSR_ABOVE_LMT_F WHERE OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}') expsr ON
        ks.BASEL_ACCT_ID = expsr.BASEL_ACCT_ID
    LEFT JOIN (SELECT * FROM features.AF_ZERO_NET_UNDRAWN_AMT WHERE OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}') afz ON
        ks.BASEL_ACCT_ID = afz.BASEL_ACCT_ID
    LEFT JOIN (SELECT * FROM instruments.PD_BAND WHERE OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' AND STREAM = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}') pd ON
        ks.BASEL_ACCT_ID = pd.BASEL_ACCT_ID
    LEFT JOIN RPTG_PRD_LKP1 pl1 ON
        ks.PRD_CD = pl1.PRD_CD
        AND TRIM(ks.SUB_PRD_CD) = pl1.SUB_PRD_CD0
        AND TRIM(expsr.TOTAL_EXPSR_ABOVE_LMT_F) = pl1.REVISED_EXPSR_OV_125K_F
        AND TRIM(heloc.HELOC_F) = pl1.HELOC_F
        AND TRIM(prd_cd.BASEL_PRD_CD) = pl1.BASEL_PRD_CD
    LEFT JOIN ingestion.BASEL_ACCT_PRFM_FACT fact ON
        ks.BASEL_ACCT_ID = fact.BASEL_ACCT_ID
        AND ks.MTH_TM_ID = fact.MTH_TM_ID
    WHERE 
        ks.MTH_TM_ID = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}