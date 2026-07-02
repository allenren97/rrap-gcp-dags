SELECT
    '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' AS OBSN_DT,
    '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}' AS STREAM,
    spl.BASEL_ACCT_ID,
    'SPL' AS SRC_SYS_CD,
    CASE
        WHEN e_mat.E_MAT_DT IS NULL THEN 
            date_diff(
                'Month',
                DATE '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}',
                last_day((note.NOTE_DT + am.AMORT))
            )
        WHEN e_mat.E_MAT_DT < DATE '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' THEN 
            date_diff(
                'Month',
                DATE '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}',
                rgl.LAST_RGL_PAY_DT
            )
        ELSE 
            date_diff(
                'Month',
                DATE '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}',
                e_mat.E_MAT_DT
            )
    END AS RESIDUAL_MAT
FROM ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT spl
LEFT JOIN features.E_MAT_DT e_mat ON
    spl.BASEL_ACCT_ID = e_mat.BASEL_ACCT_ID
    AND e_mat.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
LEFT JOIN features.LAST_RGL_PAY_DT rgl ON
    spl.BASEL_ACCT_ID = rgl.BASEL_ACCT_ID
    AND rgl.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
LEFT JOIN instruments.AMORT am ON
    spl.BASEL_ACCT_ID = am.BASEL_ACCT_ID
    AND am.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
    AND am.STREAM = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
LEFT JOIN features.NOTE_DT note ON
    spl.BASEL_ACCT_ID = note.BASEL_ACCT_ID
    AND note.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
WHERE spl.MTH_TM_ID = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}
