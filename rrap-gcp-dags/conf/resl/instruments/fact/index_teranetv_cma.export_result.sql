WITH teranet_data AS(
    SELECT
        a.MTH_TM_ID,
        t.TM_LVL_END_DT AS MONTH_END_DT,
        a.LABEL_1,
        a.LABEL_2,
        a.INDEX,
        NULLIF(p.HOUSE_INDEX_RTO, 0) AS PROVNCL_INDEX,
    FROM ingestion.TERANET_HOUSE_PRC_INDEX_CMA a
    LEFT JOIN ingestion.PROVNCL_HOUSE_INDEX_SUM_CMA p ON
        a.MTH_TM_ID = p.MTH_TM_ID AND a.LABEL_1 = (CASE WHEN p.PROV_CD='CO' THEN 'COMPOSITE' ELSE p.PROV_CD end )
    LEFT JOIN ingestion.TM_DIM t ON
        a.mth_tm_id = t.tm_id and t.tm_lvl='Month'
),
    
ordered AS (
    SELECT *
    FROM teranet_data
    ORDER BY
        LABEL_1,
        LABEL_2,
        MTH_TM_ID DESC
),

teranet AS (
    SELECT
        *,
        LAST_VALUE(index IGNORE NULLS) OVER (
            PARTITION BY label_2
            ORDER BY mth_tm_id DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS newindex,

        LAST_VALUE(PROVNCL_INDEX IGNORE NULLS) OVER (
            PARTITION BY label_2
            ORDER BY mth_tm_id DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS newPROVNCL_INDEX
    FROM ordered
),

snapshot_c AS (
    SELECT
        coalesce(airb.mth_end_dt,cast(last_day(date_trunc('day', airb.mth_end_dt)) as DATE)) AS PROCESS_DATE,
        TM_ID,
        BASEL_ACCT_ID,
        airb.MORT_NUM,
        LEND_VALUE,
        CASE
            WHEN MADE_DT IS NULL OR MADE_DT > PROCESS_DATE THEN DATE(airb.INTR_ADJ_DT)
			ELSE DATE(MADE_DT)
        END AS MADE_DT,
        prov.PROVINCE_CD AS PROVINCE
    FROM ingestion.AIRB_MORT_MTH_SNAPSHOT airb
    LEFT JOIN reference.PROVINCE_REF prov ON
        TRY_CAST(airb.PROP_PROV AS INTEGER) = TRY_CAST(prov.PROVINCE_ID AS INTEGER)
    LEFT JOIN ingestion.BASEL_MORT_MTH_SNAPSHOT mor ON
        airb.MORT_NUM = mor.MORT_NUM
        AND airb.TM_ID = mor.MTH_TM_ID
    WHERE TM_ID = 21076
    AND date_trunc('month', mth_end_dt) >= '2009-01-01 00:00:00'
),

metrpl_cma AS(
    SELECT
        BASEL_ACCT_ID,
        METRPL_AREA_NM,
        CMA,
        SRC_SYS_CD,
        PROV,
        STREAM
    FROM instruments.METRPL_AREA_NM
    WHERE
        OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate")}}'
        AND STREAM = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream")}}'
        AND TRIM(SRC_SYS_CD) = 'MOR'
),

joined_metrpl AS (
    SELECT
        s.BASEL_ACCT_ID,
        m.CMA,
        s.TM_ID,
        s.MORT_NUM,
        DATE_TRUNC('month', s.MADE_DT) + INTERVAL '1 month' - INTERVAL '1 day' AS MADE_DT,
        s.PROCESS_DATE,
        s.LEND_VALUE AS LEND_VAL2,
        s.PROVINCE
    FROM metrpl_cma m
    LEFT JOIN snapshot_c s ON
        s.BASEL_ACCT_ID = m.BASEL_ACCT_ID
),

teranet_dedup AS (
    SELECT *
    FROM (
        SELECT 
            MONTH_END_DT,
            LABEL_1,
            LABEL_2,
            newindex,
            ROW_NUMBER() OVER (
                PARTITION BY MONTH_END_DT, LABEL_1, LABEL_2
                ORDER BY newindex DESC
            ) AS rn
        FROM teranet
    )
    WHERE rn = 1
),

composite_11 AS (
    SELECT *
    FROM (
        SELECT 
            MONTH_END_DT,
            newindex AS COMP11,
            ROW_NUMBER() OVER (
                PARTITION BY MONTH_END_DT 
                ORDER BY newindex DESC
            ) AS rn
        FROM teranet
        WHERE LABEL_1 = 'COMPOSITE'
          AND LABEL_2 = '11'
          AND newindex IS NOT NULL
    )
    WHERE rn = 1
),

final AS (
    SELECT
        '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate")}}' as OBSN_DT,
        '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}' as STREAM,
        a.BASEL_ACCT_ID,
        a.MORT_NUM,
        COALESCE(b.newindex, d.COMP11) AS current_HPI,
        COALESCE(b2.newindex, d2.COMP11) AS madedate_HPI,
        a.LEND_VAL2 * (current_HPI / madedate_HPI) AS INDEX_TERANETV
    FROM joined_metrpl a

    LEFT JOIN teranet_dedup b
        ON a.PROCESS_DATE = DATE(b.MONTH_END_DT + INTERVAL 1 MONTH)
        AND a.CMA = b.LABEL_2
        AND a.PROVINCE = b.LABEL_1

    LEFT JOIN teranet_dedup b2
        ON a.MADE_DT = b2.MONTH_END_DT
        AND a.CMA = b2.LABEL_2
        AND a.PROVINCE = b2.LABEL_1

    LEFT JOIN composite_11 d
        ON a.PROCESS_DATE = DATE(d.MONTH_END_DT + INTERVAL 1 MONTH)

    LEFT JOIN composite_11 d2
        ON a.MADE_DT = d2.MONTH_END_DT
)

SELECT
    '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate")}}' as OBSN_DT,
    '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}' as STREAM,
    BASEL_ACCT_ID,
    MORT_NUM,
    INDEX_TERANETV
FROM final