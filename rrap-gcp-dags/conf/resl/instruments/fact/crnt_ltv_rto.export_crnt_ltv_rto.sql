WITH tng_c AS (
    SELECT
        dim.BASEL_ACCT_ID, -- This is set to -1 in SAS but I think(?) it should be acceptable to use instead of account_id
        NULL AS STEP_PLN_AGRMNT_NUM,
        prpty.CRNT_PRPTY_VAL_AMT
    FROM ingestion.BASEL_ACCT_DIM dim
    LEFT JOIN ingestion.TNG_ACCT_MO tng
        ON dim.SRC_APP_CD = 'TNG-MOR'
        AND dim.SRC_SYS_DEL_F != 'Y'
        AND TRIM(tng.ACCOUNT_ID) = TRIM(dim.SRC_APP_ID)
    LEFT JOIN instruments.CRNT_PRPTY_VAL_AMT AS prpty
        ON dim.BASEL_ACCT_ID = prpty.BASEL_ACCT_ID
        AND prpty.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
        AND prpty.STREAM = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'

    LEFT JOIN reference.RPTG_PRD_LKP_MOR AS rptg
        ON UPPER(tng.insurer_desc) = UPPER(rptg.basel_mortgage_insurer_group_des) 
        AND (case when UPPER(tng.bulk_nsurer_desc)='BULKINSURED' then 'Y' else 'N' end) = UPPER(rptg.bulk_indicator)

    WHERE tng.MONTH_END_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
        AND (OPEN_DT >= '2016-11-01' OR LAST_RENEWAL_DT >= '2016-11-01')
        AND (CASE   
                WHEN tng.month_end_dt >= '2017-11-30'
                    THEN UPPER(rptg.SECURITY_TYPE) IN ('UNINSURED','INSURED')
                ELSE UPPER(rptg.SECURITY_TYPE) in ('UNINSURED')
            END)
),
mor_c AS (
    SELECT
        mor.BASEL_ACCT_ID,
        mor.STEP_PLN_AGRMNT_NUM,
        COALESCE(prpty.CRNT_PRPTY_VAL_AMT, ind.INDEX_TERANETV) AS CRNT_PRPTY_VAL_AMT
    FROM ingestion.MORT_MTH_SNAPSHOT mor
    
    LEFT JOIN instruments.INDEX_TERANETV_CMA ind
        ON mor.BASEL_ACCT_ID = ind.BASEL_ACCT_ID
        AND ind.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
        AND ind.STREAM = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
    --LEFT JOIN '~/ehellebust/LTV_VAR_CLUS_LTV_FINAL_CMA_INDEX_TERANETV.parquet' ind
    --    ON mor.MORT_NUM = ind.MORTGAGE_NO

    --LEFT JOIN '~/ehellebust/EDRTLRP1D.BASEL_STEP_LTV_DRVD_VARS_CMA_21076_WITH_BASEL_ACCT_ID.parquet' AS prpty
    LEFT JOIN instruments.CRNT_PRPTY_VAL_AMT AS prpty
        ON mor.BASEL_ACCT_ID = prpty.BASEL_ACCT_ID
        AND prpty.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
        AND prpty.STREAM = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
    WHERE mor.MTH_TM_ID = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}
        AND (mor.LAST_RNEW_DT >= '2016-11-01' OR mor.INTR_ADJ_DT >= '2016-11-01')
),
spl_c AS (
    SELECT
        spl.BASEL_ACCT_ID,
        spl.STEP_PLN_AGRMNT_NUM,
        prpty.CRNT_PRPTY_VAL_AMT
    FROM ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT AS spl
    --LEFT JOIN '~/ehellebust/EDRTLRP1D.BASEL_STEP_LTV_DRVD_VARS_CMA_21076_WITH_BASEL_ACCT_ID.parquet' AS prpty
    LEFT JOIN instruments.CRNT_PRPTY_VAL_AMT AS prpty
        ON spl.BASEL_ACCT_ID = prpty.BASEL_ACCT_ID
        AND prpty.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
        AND prpty.STREAM = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
    LEFT JOIN features.PRD_ID AS prd
        ON spl.BASEL_ACCT_ID = prd.BASEL_ACCT_ID
        AND prd.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
    WHERE spl.MTH_TM_ID = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}
        AND spl.NOTE_DT >= '2016-11-01'
        AND UPPER(prd.PRD_ID) IN ('S05', 'S08')
),
ks_c AS (
    SELECT
        ks.BASEL_ACCT_ID,
        ks.STEP_PLN_AGRMNT_NUM,
        prpty.CRNT_PRPTY_VAL_AMT
    FROM ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT ks
    --LEFT JOIN '~/ehellebust/EDRTLRP1D.BASEL_STEP_LTV_DRVD_VARS_CMA_21076_WITH_BASEL_ACCT_ID.parquet' AS prpty
    LEFT JOIN instruments.CRNT_PRPTY_VAL_AMT AS prpty
        ON ks.BASEL_ACCT_ID = prpty.BASEL_ACCT_ID
        AND prpty.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
        AND prpty.STREAM = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
    LEFT JOIN features.HELOC_F AS h
        ON ks.BASEL_ACCT_ID = h.BASEL_ACCT_ID
        AND h.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
    WHERE ks.MTH_TM_ID = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}
        AND ks.ACCT_OPND_DT >= '2016-11-01'
        AND h.HELOC_F = 'Y'
),
first_orders AS (
    SELECT 
        acct.BASEL_ACCT_ID,
        acct.STEP_PLN_AGRMNT_NUM,
        acct.CRNT_PRPTY_VAL_AMT,
        bal.MAX_ACCT_BAL_AMT AS MAX_ACCT_BAL_AMT,
        --edft.EXPSR_AT_DFT_RTO AS EXPSR_AT_DFT_RTO,
        edft.EAD_FINAL_RPTG_RTO AS EXPSR_AT_DFT_RTO,
        (bal.MAX_ACCT_BAL_AMT * edft.EAD_FINAL_RPTG_RTO) AS EAD_DOLLAR,
        senrty.ACCT_SENRTY_CD
    FROM (
        SELECT
            *
        FROM ks_c
        UNION ALL
        SELECT
            *
        FROM mor_c
        UNION ALL
        SELECT
            *
        FROM spl_c
        UNION ALL
        SELECT
            *
        FROM tng_c
    ) AS acct
    LEFT JOIN features.MAX_ACCT_BAL_AMT bal
        ON acct.BASEL_ACCT_ID = bal.BASEL_ACCT_ID
        AND bal.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
    --LEFT JOIN instruments.EXPSR_AT_DFT_RTO edft
    LEFT JOIN instruments.EAD_FINAL_RPTG_RTO edft
        ON acct.BASEL_ACCT_ID = edft.BASEL_ACCT_ID
        AND edft.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
        AND edft.STREAM = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
    LEFT JOIN features.ACCT_SENRTY_CD senrty
        ON acct.BASEL_ACCT_ID = senrty.BASEL_ACCT_ID
        AND senrty.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
),
step_only AS (
    SELECT
        *,
        SUM(EAD_DOLLAR) OVER (
            PARTITION BY STEP_PLN_AGRMNT_NUM, acct_senrty_cd
        ) AS SUM_EAD_DOLLAR
    FROM first_orders
    WHERE step_pln_agrmnt_num IS NOT NULL
)
SELECT
    BASEL_ACCT_ID,
    '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' AS OBSN_DT,
    '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}' AS STREAM,
    CASE
        WHEN step_pln_agrmnt_num IS NULL THEN (MAX_ACCT_BAL_AMT * EXPSR_AT_DFT_RTO) / NULLIF(COALESCE(CRNT_PRPTY_VAL_AMT, 0), 0)
        WHEN SUM_EAD_DOLLAR IS NOT NULL THEN SUM_EAD_DOLLAR / NULLIF(COALESCE(CRNT_PRPTY_VAL_AMT, 0), 0)
        ELSE 0
    END AS CRNT_LTV_RTO
FROM (
    SELECT *, NULL AS SUM_EAD_DOLLAR FROM first_orders WHERE STEP_PLN_AGRMNT_NUM IS NULL
    UNION ALL
    SELECT * FROM step_only
    )

