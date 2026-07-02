WITH get_pop as (
    {% set upstream_asset = [
        "ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT",
        "ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT",
        "ingestion.MORT_MTH_SNAPSHOT",
        "ingestion.TNG_ACCT_MO",
        "ingestion.BASEL_ACCT_DIM",
        "instruments.EXPOSURE",
        "instruments.EXPOSURE_SECURED"
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
        ROUND((EXPOSURE - EXPOSURE_SECURED), 3)::DECIMAL AS EXPOSURE_UNSECURED
    FROM get_pop as pop
    LEFT JOIN {{upstream_asset[5]}} as ex on 
        pop.BASEL_ACCT_ID = ex.BASEL_ACCT_ID 
        AND ex.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate")}}'
        AND TRIM(ex.STREAM) = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
    LEFT JOIN {{upstream_asset[6]}} as ex_sec on 
        pop.BASEL_ACCT_ID = ex_sec.BASEL_ACCT_ID 
        AND ex_sec.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate")}}'
        AND TRIM(ex_sec.STREAM) = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
    GROUP BY
        pop.BASEL_ACCT_ID,
        pop.SRC_SYS_CD,
        ex.EXPOSURE,
        ex_sec.EXPOSURE_SECURED