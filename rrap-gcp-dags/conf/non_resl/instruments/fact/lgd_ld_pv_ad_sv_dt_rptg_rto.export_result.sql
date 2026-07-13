WITH
    mapping AS (
        SELECT
            seg.basel_seg_id,
            parm.LR_PV_AD_SV_DT_RTO
        FROM
            reference.BASEL_SEG_RPTG_PARM AS parm
            LEFT JOIN reference.BASEL_SEG AS seg ON parm.basel_seg_id = seg.basel_seg_id
            LEFT JOIN reference.BASEL_MODEL AS MOD ON MOD.basel_model_id = parm.basel_model_id
        WHERE
            parm.stream = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}'
            AND parm.EFF_TO_DT >= '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
            AND parm.EFF_FROM_DT <= '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    ),
    seg_num AS (
        SELECT
            main.*,
            seg.basel_seg_id
        FROM
            instruments.LGD_BASEL_SEG_NUM AS main
            LEFT JOIN reference.BASEL_SEG AS seg ON main.LGD_BASEL_SEG_NUM = seg.SEG_NUM
            AND main.model = seg.BASEL_MODEL_ID
        WHERE
            main.obsn_dt = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
            AND main.stream = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}'
    ),
    joined AS(
    SELECT
        main.basel_acct_id,
        mapping.LR_PV_AD_SV_DT_RTO,
    FROM
        seg_num AS main
        LEFT JOIN mapping ON main.basel_seg_id = mapping.basel_seg_id
    WHERE
        main.obsn_dt = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    )

    SELECT DISTINCT
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        acct.BASEL_ACCT_ID,
        acct.SRC_SYS_CD,
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}' AS STREAM,
        LR_PV_AD_SV_DT_RTO AS LGD_LD_PV_AD_SV_DT_RPTG_RTO
    FROM features.BASEL_ACCT_ID acct
    LEFT JOIN joined ON
        acct.BASEL_ACCT_ID = joined.BASEL_ACCT_ID
    WHERE
        acct.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'