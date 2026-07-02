WITH get_pop as (

    {% set upstream_asset = [
        "ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT",
        "ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT",
        "ingestion.TNG_ACCT_MO",
        "ingestion.BASEL_ACCT_DIM",
        "instruments.CRNT_PRPTY_VAL_AMT",
        "instruments.PREV_12_QTR_PRPTY_VAL_AMT",
        "ingestion.TM_DIM",
        "instruments.DLGD_F",
        "instruments.METRPL_AREA_NM",
        "features.METRPL_BREACH_F",
        "reference.METRPL_AREA_NM_LKP"
        ]%}

        SELECT
            BASEL_ACCT_ID,
            'KS' AS SRC_SYS_CD
        FROM {{upstream_asset[0]}}
        WHERE MTH_TM_ID = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}
        UNION

        SELECT
            BASEL_ACCT_ID,
            'TNG-MOR' AS SRC_SYS_CD
        FROM {{upstream_asset[3]}} dim
        LEFT JOIN {{upstream_asset[2]}} tng 
        ON dim.SRC_APP_CD = 'TNG-MOR'
            AND dim.SRC_SYS_DEL_F != 'Y'
            AND TRIM(tng.ACCOUNT_ID) = TRIM(dim.SRC_APP_ID)
        WHERE tng.MONTH_END_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
        UNION
        
        SELECT
            BASEL_ACCT_ID,
            'SPL' AS SRC_SYS_CD
        FROM {{upstream_asset[1]}}
        WHERE MTH_TM_ID = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}

    ),
    prpty_nm AS (
        SELECT
            BASEL_ACCT_ID,
            OBSN_DT,
            PROV_COMP AS PROV,
            METRPL_AREA_NM_CLEAN AS METRPL_AREA_NM
        FROM {{upstream_asset[8]}} m
        LEFT JOIN {{upstream_asset[10]}} lkp
        ON m.METRPL_AREA_NM = lkp.METRPL_AREA_NM
        and m.PROV = lkp.PROV
        WHERE OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
            AND STREAM = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
    ), prop_val as (
        SELECT
            p.*,
            {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }} as MTH_TM_ID,
            area.METRPL_AREA_NM,
            prev.PREV_12_QTR_PRPTY_VAL_AMT,
            fact.CRNT_PRPTY_VAL_AMT,
            CASE
                WHEN prev.PREV_12_QTR_PRPTY_VAL_AMT is NULL or fact.CRNT_PRPTY_VAL_AMT = 0 or fact.CRNT_PRPTY_VAL_AMT IS NULL THEN NULL
                ELSE (1 - prev.PREV_12_QTR_PRPTY_VAL_AMT / fact.CRNT_PRPTY_VAL_AMT)
            END AS v_PROPERTY_VAL_RATIO
        FROM 
            get_pop p
            LEFT JOIN prpty_nm area on area.BASEL_ACCT_ID = p.BASEL_ACCT_ID and area.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
            LEFT JOIN (SELECT DISTINCT * FROM {{upstream_asset[4]}}) fact  on p.BASEL_ACCT_ID = fact.BASEL_ACCT_ID and fact.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' and fact.stream = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
            LEFT JOIN {{upstream_asset[5]}} prev on p.BASEL_ACCT_ID = prev.BASEL_ACCT_ID and prev.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' and prev.STREAM = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
            
    )
    SELECT
        p.BASEL_ACCT_ID,
        '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' as OBSN_DT,
        STREAM,
        v_PROPERTY_VAL_RATIO,
        METRPL_BREACH_F,
        p.*,
        DLGD_F,
        CASE
            WHEN DLGD_F = 'N' THEN NULL
            WHEN p.SRC_SYS_CD in('KS','MOR')
            THEN CASE
                    WHEN METRPL_BREACH_F = 'Y' and v_PROPERTY_VAL_RATIO is not NULL
                        THEN GREATEST(ROUND(v_PROPERTY_VAL_RATIO, 4), 0.25)
                    WHEN (METRPL_BREACH_F <> 'Y' or METRPL_BREACH_F is NULL) and v_PROPERTY_VAL_RATIO is not NULL
                        THEN GREATEST(ROUND(v_PROPERTY_VAL_RATIO, 4), 0)
                    else NULL
                END
            ELSE CASE
                    WHEN METRPL_BREACH_F = 'Y'
                        THEN GREATEST(ROUND(v_PROPERTY_VAL_RATIO, 4), 0.25)
                    WHEN (METRPL_BREACH_F <> 'Y' or METRPL_BREACH_F is NULL)
                        THEN GREATEST(ROUND(v_PROPERTY_VAL_RATIO, 4), 0)
                    else NULL
                END
            END AS PRPTY_VAL_CORR_PCTG,
        SRC_SYS_CD
    FROM prop_val p
    LEFT JOIN {{upstream_asset[7]}} dlgd
        on p.BASEL_ACCT_ID = dlgd.BASEL_ACCT_ID
        and stream = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
        and OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
    LEFT JOIN ( 
        SELECT DISTINCT 
            METRPL_BREACH_F,
            upper(TRIM(METRPL_AREA_NM)) as METRPL_AREA_NM
        FROM {{upstream_asset[9]}}
        WHERE OBSN_DT = (
            SELECT TM_LVL_END_DT FROM {{upstream_asset[6]}} WHERE TM_ID = (
                SELECT max(TM_ID) 
                FROM {{upstream_asset[6]}} 
                WHERE FNCL_QTR_KEY in (
                    SELECT FNCL_QTR_KEY
                    FROM {{upstream_asset[6]}}
                    WHERE TM_LVL = 'Month' and TM_ID = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}
                )
            )
        )
    ) AS mb
    on UPPER(TRIM(p.METRPL_AREA_NM)) = mb.METRPL_AREA_NM

    