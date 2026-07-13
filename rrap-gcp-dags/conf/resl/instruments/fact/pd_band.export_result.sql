WITH
    BAND_KEY AS (
        SELECT
            main.basel_acct_id,
            COALESCE(
                l1.PD_BAND_EXPSR_CL_KEY_VAL,
                l2.PD_BAND_EXPSR_CL_KEY_VAL,
                l3.PD_BAND_EXPOSURE_CLASS_KEY_VALUE
            ) AS PD_BAND_EXPSR_CL_KEY_VAL
        FROM
            features.basel_acct_id AS main
            LEFT JOIN features.prd_id AS prd_id ON prd_id.basel_acct_id = main.basel_acct_id
            AND prd_id.obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
            LEFT JOIN reference.RPTG_PRD_LKP_KS l1 ON UPPER(l1.prd_id) = UPPER(prd_id.prd_id)
            AND (
                '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="yyyymm") }}' BETWEEN CAST(l1.EFF_FROM_YR_MTH AS INTEGER) AND CAST(l1.EFF_TO_YR_MTH AS INTEGER)
            )
            LEFT JOIN reference.RPTG_PRD_LKP_SPL l2 ON UPPER(l2.prd_id) = UPPER(prd_id.prd_id)
            AND (
                '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="yyyymm") }}' BETWEEN CAST(l2.EFF_FROM_YR_MTH AS INTEGER) AND CAST(l2.EFF_TO_YR_MTH AS INTEGER)
            )
            LEFT JOIN reference.RPTG_PRD_LKP_MOR l3 ON UPPER(l3.product_id) = UPPER(prd_id.prd_id)
            AND (
                '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="yyyymm") }}' BETWEEN CAST(l3.EFF_FROM_YR_MTH AS INTEGER) AND CAST(l3.EFF_TO_YR_MTH AS INTEGER)
            )
        WHERE
            main.obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
    )
SELECT
    main.basel_acct_id,
    dim.PD_BAND,
    main.src_sys_cd,
FROM
    features.basel_acct_id AS main
    LEFT JOIN features.CMHC_F AS CMHC_F ON main.basel_acct_id = CMHC_F.basel_acct_id
    AND CMHC_F.obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
    LEFT JOIN features.TRANSACTOR_FLAG_QRR AS TRANSACTOR_FLAG_QRR ON main.basel_acct_id = TRANSACTOR_FLAG_QRR.basel_acct_id
    AND TRANSACTOR_FLAG_QRR.obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
    LEFT JOIN instruments.PD_FLRD_RPTG_RTO AS PD_FLRD_RPTG_RTO ON main.basel_acct_id = PD_FLRD_RPTG_RTO.basel_acct_id
    AND PD_FLRD_RPTG_RTO.obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
    AND PD_FLRD_RPTG_RTO.stream = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
    LEFT JOIN BAND_KEY AS bk ON bk.basel_acct_id = main.basel_acct_id
    LEFT JOIN reference.PD_BAND_DIM AS dim ON COALESCE(dim.CMHC_F, 'Z') = COALESCE(CMHC_F.CMHC_F, 'Z')
    AND COALESCE(dim.TRANSACTOR_F, 'Z') = COALESCE(TRANSACTOR_FLAG_QRR.TRANSACTOR_FLAG_QRR, 'Z')
    AND bk.PD_BAND_EXPSR_CL_KEY_VAL = dim.NCR_EXPSR_CL_KEY_VAL
    AND '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="yyyymm") }}' BETWEEN dim.eff_from_yr_mth AND dim.eff_to_yr_mth
    AND PD_FLRD_RPTG_RTO.PD_FLRD_RPTG_RTO BETWEEN dim.PD_MIN_VAL AND dim.PD_MAX_VAL AND dim.stream = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
WHERE
    main.obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'