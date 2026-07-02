WITH get_pop as (
    {% set upstream_asset = [
        "features.BASEL_ACCT_ID",
        "features.ASST_CL_NUM",
        "features.CMHC_F",
        "features.COLLATERAL_TYPE",
        "features.LGD_S",
        "features.LGD_U",
        "instruments.FULLY_SECURED_F",
        "instruments.WEIGHT_SECURED",
        "instruments.WEIGHT_UNSECURED",
        "instruments.EXPOSURE_SECURED_MAXIMUM",
        "instruments.LGD_FINAL_RPTG_RTO"
        ]%}

        SELECT
            acct.BASEL_ACCT_ID,
            acct.SRC_SYS_CD
        FROM {{upstream_asset[0]}} acct
        WHERE OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate")}}'
        )

    SELECT
        '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate")}}' as OBSN_DT,
        acct.BASEL_ACCT_ID,
        acct.SRC_SYS_CD,
        '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}' as STREAM,
        CASE
            WHEN LGD_FINAL_RPTG_RTO IS NULL THEN NULL
            WHEN ASST_CL_NUM = 1 and TRIM(CMHC_F) = 'Y' and FULLY_SECURED_F IS NULL then 0.0
            WHEN ASST_CL_NUM = 1 and CMHC_F IS NULL and FULLY_SECURED_F IS NULL then 0.1
            WHEN ASST_CL_NUM = 3 and CMHC_F IS NULL and FULLY_SECURED_F IS NULL then 0.5
            WHEN ASST_CL_NUM = 2 and CMHC_F IS NULL and COLLATERAL_TYPE IS NULL then 0.3
            WHEN ASST_CL_NUM = 2 and CMHC_F IS NULL and COLLATERAL_TYPE IS NOT NULL THEN
                CASE WHEN EXPOSURE_SECURED_MAXIMUM = 0 THEN 0.3
                ELSE WEIGHT_UNSECURED * LGD_U+WEIGHT_SECURED * LGD_S END 
            ELSE NULL
        END AS LGD_FLR
    FROM get_pop acct
    LEFT JOIN {{upstream_asset[1]}} asst ON
        acct.BASEL_ACCT_ID = asst.BASEL_ACCT_ID
        AND asst.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
    LEFT JOIN {{upstream_asset[2]}} cmhc ON
        acct.BASEL_ACCT_ID = cmhc.BASEL_ACCT_ID
        AND cmhc.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
    LEFT JOIN {{upstream_asset[3]}} col ON
        acct.BASEL_ACCT_ID = col.BASEL_ACCT_ID
        AND col.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
    LEFT JOIN {{upstream_asset[4]}} lgd_s ON
        acct.BASEL_ACCT_ID = lgd_s.BASEL_ACCT_ID
        AND lgd_s.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
    LEFT JOIN {{upstream_asset[5]}} lgd_u ON
        acct.BASEL_ACCT_ID = lgd_u.BASEL_ACCT_ID
        AND lgd_u.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
    LEFT JOIN {{upstream_asset[6]}} full_sec ON
        acct.BASEL_ACCT_ID = full_sec.BASEL_ACCT_ID
        AND full_sec.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
        AND TRIM(full_sec.STREAM) = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
    LEFT JOIN {{upstream_asset[7]}} weight_sec ON
        acct.BASEL_ACCT_ID = weight_sec.BASEL_ACCT_ID
        AND weight_sec.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
        AND TRIM(full_sec.STREAM) = TRIM(weight_sec.STREAM)
    LEFT JOIN {{upstream_asset[8]}} weight_un ON
        acct.BASEL_ACCT_ID = weight_un.BASEL_ACCT_ID
        AND weight_un.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
        AND TRIM(full_sec.STREAM) = TRIM(weight_un.STREAM)
    LEFT JOIN {{upstream_asset[9]}} esm ON
        acct.BASEL_ACCT_ID = esm.BASEL_ACCT_ID
        AND esm.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
        AND TRIM(full_sec.STREAM) = TRIM(esm.STREAM)
    LEFT JOIN {{upstream_asset[10]}} lgd_rto ON
        acct.BASEL_ACCT_ID = lgd_rto.BASEL_ACCT_ID
        AND lgd_rto.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
        AND TRIM(full_sec.STREAM) = TRIM(lgd_rto.STREAM)