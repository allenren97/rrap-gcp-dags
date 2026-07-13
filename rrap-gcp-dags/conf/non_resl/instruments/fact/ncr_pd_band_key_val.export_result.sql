SELECT
    dim.NCR_PD_BAND_KEY_VAL,
    exp.BASEL_ACCT_ID,
    exp.OBSN_DT,
    exp.SRC_SYS_CD,
    '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}' AS STREAM
FROM (SELECT * FROM features.PD_BAND_EXPSR_CL_KEY_VAL WHERE OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}') exp
LEFT JOIN (SELECT * FROM instruments.PD_FLRD_RPTG_RTO WHERE OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' AND STREAM = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}') rto
    ON exp.BASEL_ACCT_ID = rto.BASEL_ACCT_ID
LEFT JOIN (SELECT * FROM features.CMHC_F WHERE OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}') cmhc
    ON exp.BASEL_ACCT_ID = cmhc.BASEL_ACCT_ID
LEFT JOIN (SELECT * FROM features.TRANSACTOR_FLAG_QRR WHERE OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}') tf
    ON exp.BASEL_ACCT_ID = tf.BASEL_ACCT_ID
LEFT JOIN reference.PD_BAND_DIM dim
    ON exp.PD_BAND_EXPSR_CL_KEY_VAL = dim.NCR_EXPSR_CL_KEY_VAL
    AND rto.PD_FLRD_RPTG_RTO BETWEEN dim.PD_MIN_VAL AND dim.PD_MAX_VAL
    AND COALESCE(UPPER(cmhc.CMHC_F),'Z') = COALESCE(UPPER(dim.CMHC_F),'Z')
    AND COALESCE(UPPER(tf.TRANSACTOR_FLAG_QRR),'Z') = COALESCE(UPPER(dim.TRANSACTOR_F),'Z')
    AND '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="yyyymm") }}' BETWEEN dim.EFF_FROM_YR_MTH AND dim.EFF_TO_YR_MTH
    AND dim.STREAM = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'