WITH get_pop as (
    {% set upstream_asset = [
        "features.BASEL_ACCT_ID",
        "instruments.PRE_INSURANCE_LGD",
        "features.BASEL_PRD_TP_CD",
        "reference.BASEL_SEG_RPTG_PARM",
        "reference.BASEL_SEG",
        "reference.BASEL_MODEL",
        "instruments.UNINSURED_LGD_SEG_NUM",
        "instruments.LGD_BASEL_SEG_NUM",
        "instruments.LGD_MODEL_NM"
        ]%}

        SELECT
            acct.BASEL_ACCT_ID,
            acct.SRC_SYS_CD
        FROM {{upstream_asset[0]}} acct
        WHERE OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate")}}'
        ),
    
    mapping AS(
        SELECT
            seg.BASEL_SEG_ID,
            parm.PRE_INSURANCE_LGD,
            mod.SRC_SYS_CD
        FROM 
            {{upstream_asset[3]}} AS parm
            LEFT JOIN {{upstream_asset[4]}} AS seg ON parm.basel_seg_id = seg.basel_seg_id
            LEFT JOIN {{upstream_asset[5]}} AS MOD ON MOD.basel_model_id = parm.basel_model_id
        WHERE
            TRIM(parm.stream) = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream")}}'
            AND parm.EFF_TO_DT >= '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate")}}'
            AND parm.EFF_FROM_DT <= '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate")}}'
            AND TRIM(MOD.SRC_SYS_CD) IN ('MOR','TNG-MOR')
    ),

    seg_num AS (
        SELECT DISTINCT
            main.*,
            sys_cd.SRC_SYS_CD,
            seg.basel_seg_id
        FROM
            {{upstream_asset[6]}} AS main
        LEFT JOIN features.SRC_SYS_CD sys_cd ON
            main.BASEL_ACCT_ID = sys_cd.BASEL_ACCT_ID
            AND sys_cd.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate")}}'
        LEFT JOIN {{upstream_asset[7]}} lgd_seg ON
            main.BASEL_ACCT_ID = lgd_seg.BASEL_ACCT_ID
            AND lgd_seg.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate")}}'
            AND TRIM(lgd_seg.STREAM) = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream")}}'
        LEFT JOIN {{upstream_asset[8]}} nm ON
            main.BASEL_ACCT_ID = nm.BASEL_ACCT_ID
            AND nm.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate")}}'
            AND TRIM(nm.STREAM) = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream")}}'
        LEFT JOIN {{upstream_asset[4]}} AS seg ON 
            main.UNINSURED_LGD_SEG_NUM = seg.SEG_NUM
            AND TRIM(lgd_seg.model) = TRIM(seg.BASEL_MODEL_ID)
        WHERE
            main.obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate")}}'
            AND TRIM(main.STREAM) = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream")}}'
    )

    SELECT
        '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate")}}' as OBSN_DT,
        acct.BASEL_ACCT_ID,
        acct.SRC_SYS_CD,
        '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}' as STREAM,
        CASE
            WHEN TRIM(BASEL_PRD_TP_CD) NOT LIKE 'GENW%' AND TRIM(BASEL_PRD_TP_CD) NOT LIKE  'GUAR%' THEN NULL
            ELSE mapping.PRE_INSURANCE_LGD
        END AS PMI_LGD_INSURED_RPTG_RTO
    FROM get_pop acct
    LEFT JOIN seg_num AS main ON
        acct.BASEL_ACCT_ID = main.BASEL_ACCT_ID
    LEFT JOIN {{upstream_asset[2]}} prd_tp ON
        acct.BASEL_ACCT_ID = prd_tp.BASEL_ACCT_ID
        AND prd_tp.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
    LEFT JOIN mapping ON
        main.BASEL_SEG_ID = mapping.BASEL_SEG_ID
        