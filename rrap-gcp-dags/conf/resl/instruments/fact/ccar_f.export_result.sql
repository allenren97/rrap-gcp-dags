WITH get_pop as (
    {% set upstream_asset = [
        "features.BASEL_ACCT_ID",
        "features.CONSM_PRD_TREATMNT_CD_IF",
        "features.SML_BUS_F",
        "features.TRNST_EXCLSN_F",
        "instruments.PIT_STAT_CD",
        "instruments.PD_BASEL_SEG_NUM",
        "instruments.LGD_BASEL_SEG_NUM"
        ]%}

        SELECT
            pop.BASEL_ACCT_ID,
            pop.SRC_SYS_CD,
            CASE WHEN 
                TRIM(consm.CONSM_PRD_TREATMNT_CD_IF)='A' 
                AND TRIM(sml.SML_BUS_F)='N' 
                AND TRIM(trnst.TRNST_EXCLSN_F)='N' 
                AND TRIM(pit.PIT_STATUS_CROSS_DEFAULT_ORIG) in ('CUR','DEF') 
                AND (PD_BASEL_SEG_NUM IS NOT NULL AND LGD_BASEL_SEG_NUM IS NOT NULL) 
            THEN 1 ELSE 0 
            END AS CCAR_F
        FROM {{upstream_asset[0]}} pop
        LEFT JOIN {{upstream_asset[1]}} consm ON
            pop.BASEL_ACCT_ID = consm.BASEL_ACCT_ID
            AND consm.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate")}}'
        LEFT JOIN {{upstream_asset[2]}} sml ON
            pop.BASEL_ACCT_ID = sml.BASEL_ACCT_ID
            AND sml.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate")}}'
        LEFT JOIN {{upstream_asset[3]}} trnst ON
            pop.BASEL_ACCT_ID = trnst.BASEL_ACCT_ID
            AND trnst.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate")}}'
        LEFT JOIN {{upstream_asset[4]}} pit ON
            pop.BASEL_ACCT_ID = pit.BASEL_ACCT_ID
            AND pit.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate")}}'
        LEFT JOIN {{upstream_asset[5]}} pd ON
            pop.BASEL_ACCT_ID = pd.BASEL_ACCT_ID
            AND pd.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate")}}'
            AND TRIM(pd.STREAM) = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
        LEFT JOIN {{upstream_asset[6]}} lgd ON
            pop.BASEL_ACCT_ID = lgd.BASEL_ACCT_ID
            AND lgd.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate")}}'
            AND TRIM(pd.STREAM) = TRIM(lgd.STREAM)
        WHERE 
            pop.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate")}}'
    )
    SELECT
        '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate")}}' as OBSN_DT,
        pop.BASEL_ACCT_ID,
        pop.SRC_SYS_CD,
        '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}' as STREAM,
        CCAR_F
    FROM get_pop as pop