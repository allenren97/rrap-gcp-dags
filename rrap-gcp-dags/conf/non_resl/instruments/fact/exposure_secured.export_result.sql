WITH get_pop as (
    {% set upstream_asset = [
        "ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT",
        "ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT",
        "ingestion.MORT_MTH_SNAPSHOT",
        "ingestion.TNG_ACCT_MO",
        "ingestion.BASEL_ACCT_DIM",
        "instruments.EXPOSURE_SECURED_MAXIMUM",
        "features.COLLATERAL_VALUE",
        "features.H_C"
        ]%}

        SELECT
            ks.BASEL_ACCT_ID,
            'KS' AS SRC_SYS_CD
        FROM {{upstream_asset[0]}} ks
        WHERE ks.MTH_TM_ID = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id")}}
        
        UNION

        SELECT
            spl.BASEL_ACCT_ID,
            'SPL' AS SRC_SYS_CD
        FROM {{upstream_asset[1]}} spl
        WHERE spl.MTH_TM_ID = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id")}}
        
        UNION

        SELECT
            mor.BASEL_ACCT_ID,
            'MOR' AS SRC_SYS_CD
        FROM {{upstream_asset[2]}} mor
        WHERE mor.MTH_TM_ID = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id")}}
        
        UNION
        
        SELECT
            dim.BASEL_ACCT_ID,
            'TNG-MOR' AS SRC_SYS_CD
        FROM {{upstream_asset[3]}} tng
        INNER JOIN {{upstream_asset[4]}} dim ON
            dim.SRC_APP_CD = 'TNG-MOR'
            AND dim.SRC_SYS_DEL_F != 'Y'
            AND TRIM(tng.ACCOUNT_ID) = TRIM(dim.SRC_APP_ID)
        WHERE tng.MONTH_END_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate")}}'
    )
    SELECT
        '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate")}}' as OBSN_DT,
        pop.BASEL_ACCT_ID,
        pop.SRC_SYS_CD,
        '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}' as STREAM,
        LEAST(EXPOSURE_SECURED_MAXIMUM, COALESCE(COLLATERAL_VALUE, 0) * (1 - H_C)) AS EXPOSURE_SECURED

    
    FROM get_pop as pop
    LEFT JOIN {{upstream_asset[5]}} as ex_max on 
        pop.BASEL_ACCT_ID = ex_max.BASEL_ACCT_ID 
        AND ex_max.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate")}}'
        AND TRIM(ex_max.STREAM) = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
    LEFT JOIN {{upstream_asset[6]}} as col_value on 
        pop.BASEL_ACCT_ID = col_value.BASEL_ACCT_ID 
        AND col_value.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate")}}'
    LEFT JOIN {{upstream_asset[7]}} as h_c on 
        pop.BASEL_ACCT_ID = h_c.BASEL_ACCT_ID 
        AND h_c.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate")}}'
    GROUP BY
        pop.BASEL_ACCT_ID,
        pop.SRC_SYS_CD,
        ex_max.EXPOSURE_SECURED_MAXIMUM,
        col_value.COLLATERAL_VALUE,
        h_c.H_C