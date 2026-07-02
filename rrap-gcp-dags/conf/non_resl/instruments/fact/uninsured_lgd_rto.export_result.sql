WITH
    mapping AS (
        SELECT
            m.model_nm,
            parm.FINAL_RTO,
            seg.seg_num,
            seg.basel_seg_id
        FROM
            reference.BASEL_SEG_RPTG_PARM AS parm
            LEFT JOIN reference.BASEL_SEG AS seg ON parm.basel_seg_id = seg.basel_seg_id
            LEFT JOIN reference.BASEL_MODEL AS m ON m.basel_model_id = parm.basel_model_id
        WHERE
            parm.stream = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
            AND parm.EFF_TO_DT >= '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
            AND parm.EFF_FROM_DT <= '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
            AND m.src_sys_cd = 'MOR'
    ),
    seg_num AS (
        SELECT
            main.*,
            m.LGD_MODEL_NM
        FROM
            instruments.UNINSURED_LGD_SEG_NUM AS main
            LEFT JOIN instruments.LGD_MODEL_NM AS m ON m.basel_acct_id = main.basel_acct_id
            AND m.obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
            AND m.stream = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
        WHERE
            main.obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
            AND main.stream = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
    )
SELECT
    main.basel_acct_id,
    CASE
        WHEN main.src_sys_cd IN ('KS', 'SPL') THEN NULL
        WHEN main.src_sys_cd IN ('MOR')
        AND (
            BASEL_PRD_TP_CD LIKE '%GENW%'
            OR BASEL_PRD_TP_CD LIKE '%GUAR%'
        ) THEN FINAL_RTO
        WHEN main.src_sys_cd IN ('TNG-MOR') -- confirm the TNG logic, this is best guess
        AND BASEL_PRD_TP_CD LIKE '%GENW%'
        OR BASEL_PRD_TP_CD LIKE '%GUAR%' THEN LGD_FINAL_RPTG_RTO
    END AS UNINSURED_LGD_RTO
FROM
    features.basel_acct_id AS main
    LEFT JOIN seg_num ON main.basel_acct_id = seg_num.basel_acct_id
    LEFT JOIN mapping ON seg_num.UNINSURED_LGD_SEG_NUM = mapping.seg_num
    AND seg_num.lgd_model_nm = mapping.model_nm
    LEFT JOIN features.BASEL_PRD_TP_CD AS cd ON cd.basel_acct_id = main.basel_acct_id
    AND cd.obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
    LEFT JOIN instruments.LGD_FINAL_RPTG_RTO AS lgd ON lgd.basel_acct_id = main.basel_acct_id
    AND lgd.obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
    AND lgd.stream = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
WHERE
    main.obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'