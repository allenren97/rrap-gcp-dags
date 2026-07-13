{% set upstream_asset = [
    "ingestion.MORT_MTH_SNAPSHOT",
    "instruments.INDEX_TERANETV_CMA",
    "instruments.PREV_12_QTR_PRPTY_VAL_AMT",
    "ingestion.TM_DIM",
    "instruments.DLGD_F",
    "instruments.METRPL_AREA_NM",
    "features.METRPL_BREACH_F",
    "reference.METRPL_AREA_NM_LKP"
    ]%}
WITH prpty_nm AS (
        SELECT
            BASEL_ACCT_ID,
            OBSN_DT,
            PROV_COMP AS PROV,
            METRPL_AREA_NM_CLEAN AS METRPL_AREA_NM
        FROM {{upstream_asset[5]}} m
        LEFT JOIN {{upstream_asset[7]}} lkp
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
            fact.INDEX_TERANETV,
            CASE
                WHEN prev.PREV_12_QTR_PRPTY_VAL_AMT is NULL or fact.INDEX_TERANETV = 0 or fact.INDEX_TERANETV IS NULL THEN NULL
                ELSE (1 - prev.PREV_12_QTR_PRPTY_VAL_AMT / fact.INDEX_TERANETV)
            END AS v_PROPERTY_VAL_RATIO
        FROM 
            {{upstream_asset[0]}} p
            LEFT JOIN prpty_nm area on area.BASEL_ACCT_ID = p.BASEL_ACCT_ID and area.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
            LEFT JOIN {{upstream_asset[1]}} fact on p.MORT_NUM = fact.MORT_NUM and fact.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' and fact.STREAM = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
            LEFT JOIN {{upstream_asset[2]}} prev on p.BASEL_ACCT_ID = prev.BASEL_ACCT_ID and prev.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' and prev.STREAM = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
        WHERE p.MTH_TM_ID = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}
    )
    SELECT
        p.BASEL_ACCT_ID,
        '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' as OBSN_DT,
        STREAM,
        v_PROPERTY_VAL_RATIO,
        METRPL_BREACH_F,
        p.*,
        CASE
            WHEN DLGD_F = 'N' THEN NULL

            WHEN METRPL_BREACH_F = 'Y' and v_PROPERTY_VAL_RATIO is not NULL
                THEN GREATEST(ROUND(v_PROPERTY_VAL_RATIO, 4), 0.25)
            WHEN (METRPL_BREACH_F <> 'Y' or METRPL_BREACH_F is NULL) and v_PROPERTY_VAL_RATIO is not NULL
                THEN GREATEST(ROUND(v_PROPERTY_VAL_RATIO, 4), 0)
            else NULL

        END AS PRPTY_VAL_CORR_PCTG,
        'MOR' as SRC_SYS_CD
    FROM prop_val p
    LEFT JOIN {{upstream_asset[4]}} dlgd
        on p.BASEL_ACCT_ID = dlgd.BASEL_ACCT_ID
        and stream = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
        and OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
    LEFT JOIN ( 
        SELECT DISTINCT 
            METRPL_BREACH_F,
            upper(TRIM(METRPL_AREA_NM)) as METRPL_AREA_NM
        FROM {{upstream_asset[6]}}
        WHERE OBSN_DT = (
            SELECT TM_LVL_END_DT FROM {{upstream_asset[3]}} WHERE TM_ID = (
                SELECT max(TM_ID) 
                FROM {{upstream_asset[3]}} 
                WHERE FNCL_QTR_KEY in (
                    SELECT FNCL_QTR_KEY
                    FROM {{upstream_asset[3]}}
                    WHERE TM_LVL = 'Month' and TM_ID = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}
                )
            )
        )
    ) AS mb
    on UPPER(TRIM(p.METRPL_AREA_NM)) = mb.METRPL_AREA_NM

    