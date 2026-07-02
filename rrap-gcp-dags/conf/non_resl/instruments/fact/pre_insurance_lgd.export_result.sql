WITH
    mapping AS (
        SELECT
            seg.basel_seg_id,
            parm.PRE_INSURANCE_LGD
        FROM
            reference.BASEL_SEG_RPTG_PARM AS parm
            LEFT JOIN reference.BASEL_SEG AS seg ON parm.basel_seg_id = seg.basel_seg_id
            LEFT JOIN reference.BASEL_MODEL AS MOD ON MOD.basel_model_id = parm.basel_model_id
        WHERE
            parm.stream = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
            AND parm.EFF_TO_DT >= '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
            AND parm.EFF_FROM_DT <= '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
            AND MOD.SRC_SYS_CD IN ('MOR', 'TNG-MOR')
    ),
    seg_num AS (
        SELECT
            main.*,
            seg.basel_seg_id
        FROM
            instruments.lgd_basel_seg_num AS main
            LEFT JOIN reference.BASEL_SEG AS seg ON main.lgd_basel_seg_num = seg.SEG_NUM
            AND main.model = seg.BASEL_MODEL_ID
        WHERE
            main.obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
            AND main.stream = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
    )
SELECT
    main.basel_acct_id,
    mapping.pre_insurance_lgd,
FROM
    seg_num AS main
    LEFT JOIN mapping ON main.basel_seg_id = mapping.basel_seg_id
WHERE
    main.obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'