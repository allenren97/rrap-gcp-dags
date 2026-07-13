WITH get_pop as (
    {% set upstream_asset = [
        "features.BASEL_ACCT_ID",
        "instruments.LGD_FLR",
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
            ELSE GREATEST(LGD_FLR::DECIMAL(18,10), LGD_FINAL_RPTG_RTO::DECIMAL(18,10))
        END AS LGD_FLRD_RPTG_RTO
    FROM get_pop acct
    LEFT JOIN {{upstream_asset[1]}} flr ON
        acct.BASEL_ACCT_ID = flr.BASEL_ACCT_ID
        AND flr.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
        AND TRIM(flr.STREAM) = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
    LEFT JOIN {{upstream_asset[2]}} lgd_rto ON
        acct.BASEL_ACCT_ID = lgd_rto.BASEL_ACCT_ID
        AND lgd_rto.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
        AND TRIM(flr.STREAM) = TRIM(lgd_rto.STREAM)
        