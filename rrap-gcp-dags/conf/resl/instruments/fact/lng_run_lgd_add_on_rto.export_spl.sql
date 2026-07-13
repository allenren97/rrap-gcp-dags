WITH vars AS(
    SELECT
        spl.BASEL_ACCT_ID,
        DLGD_F,
        PRPTY_VAL_CORR_PCTG,
        CRNT_LTV_RTO
    FROM ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT spl
    LEFT JOIN instruments.DLGD_F dlgd ON
        spl.BASEL_ACCT_ID = dlgd.BASEL_ACCT_ID
        AND dlgd.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
        AND TRIM(dlgd.STREAM) = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
    LEFT JOIN instruments.PRPTY_VAL_CORR_PCTG prpty ON
        spl.BASEL_ACCT_ID = prpty.BASEL_ACCT_ID
        AND prpty.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
        AND TRIM(prpty.STREAM) = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
    LEFT JOIN instruments.CRNT_LTV_RTO crnt ON
        spl.BASEL_ACCT_ID = crnt.BASEL_ACCT_ID
        AND crnt.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
        AND TRIM(crnt.STREAM) = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
    WHERE 
        spl.MTH_TM_ID = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}
),

dv_pop AS(
    SELECT DISTINCT
    spl.BASEL_ACCT_ID,
    CASE
    WHEN CRNT_LTV_RTO IS NOT NULL 
         AND CRNT_LTV_RTO <> 0 
         AND PRPTY_VAL_CORR_PCTG::DECIMAL(30,20) IS NOT NULL
    THEN
        CASE 
            WHEN INDEXED_CCLTV_RTO IS NOT NULL 
            THEN 
                CAST(ROUND(GREATEST((LEAST(CRNT_LTV_RTO, GREATEST(INDEXED_CCLTV_RTO - 0.8 * (1 - PRPTY_VAL_CORR_PCTG::DECIMAL(30,20)), 0)) - GREATEST(INDEXED_CCLTV_RTO - 0.8, 0)) /CRNT_LTV_RTO, 0), 8) AS DECIMAL(28,8))
            ELSE 
                CAST(ROUND((GREATEST(CRNT_LTV_RTO - 0.8 * (1 - PRPTY_VAL_CORR_PCTG::DECIMAL(30,20)), 0) - GREATEST(CRNT_LTV_RTO - 0.8, 0)) / CRNT_LTV_RTO, 8) AS DECIMAL(28,8))
        END
    ELSE NULL
END AS LNG_RUN_LGD_ADD_ON_RTO
FROM ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT spl
INNER JOIN ingestion.TM_DIM dim ON
    spl.MTH_TM_ID = dim.TM_ID
LEFT JOIN vars v ON
    spl.BASEL_ACCT_ID = v.BASEL_ACCT_ID
LEFT JOIN instruments.PIT_STAT_CD pit ON
    spl.BASEL_ACCT_ID = pit.BASEL_ACCT_ID
    AND pit.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
    AND TRIM(pit.STREAM) = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
LEFT JOIN instruments.INDEXED_CCLTV_RTO ind ON
    spl.BASEL_ACCT_ID = ind.BASEL_ACCT_ID
    AND ind.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
    AND TRIM(ind.STREAM) = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
LEFT JOIN features.ACCT_SENRTY_CD senrty ON
    spl.BASEL_ACCT_ID = senrty.BASEL_ACCT_ID
    AND senrty.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
LEFT JOIN features.PRD_ID prd ON
    spl.BASEL_ACCT_ID = prd.BASEL_ACCT_ID
    AND prd.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
LEFT JOIN features.NOTE_DT note ON
    spl.BASEL_ACCT_ID = note.BASEL_ACCT_ID
    AND note.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
LEFT JOIN reference.BASEL_SEG_RPTG_PARM parm ON
    dim.TM_LVL_END_DT BETWEEN parm.EFF_FROM_DT AND parm.EFF_TO_DT
LEFT JOIN reference.BASEL_SEG seg ON
    seg.BASEL_SEG_ID = parm.BASEL_SEG_ID
WHERE 
    spl.MTH_TM_ID = 21076
    AND senrty.ACCT_SENRTY_CD = 3
    AND UPPER(prd.PRD_ID) IN ('S05','S08') 
    AND note.NOTE_DT >= '2016-11-01' --Hardcoded dlgd_date value from SAS job
    AND seg.BASEL_SEG_ID NOT IN ('16136', '16135', '16162', '16155', '16128',
    '16156', '16163', '16164', '16134')
    AND TRIM(pit.PIT_STAT_CD) = 'CUR'
ORDER BY
    spl.MTH_TM_ID,
    spl.BASEL_ACCT_ID)

SELECT 
    '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' AS OBSN_DT,
    spl.BASEL_ACCT_ID,
    'SPL' AS SRC_SYS_CD,
    '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}' AS STREAM,
    pop.LNG_RUN_LGD_ADD_ON_RTO
FROM ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT spl
LEFT JOIN dv_pop pop ON
    spl.BASEL_ACCT_ID = pop.BASEL_ACCT_ID
WHERE spl.MTH_TM_ID = 21076