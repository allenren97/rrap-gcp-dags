{% set upstream_asset = [
        "ingestion.MORT_MTH_SNAPSHOT",
        "ingestion.AIRB_MORT_MTH_SNAPSHOT",
        "instruments.DLGD_F",
        "ingestion.TERANET_ADDR_LKP_CMA",
        "reference.PROVINCE_REF"
        ] %}

WITH snapshot_c AS (
    SELECT
        TM_ID,
        MORT_NUM,
        CAST(PROPERTY_ADDR_1 AS VARCHAR(112)) AS PROPERTY_ADDR_1,
        CAST(PROPERTY_ADDR_2 AS VARCHAR(112)) AS PROPERTY_ADDR_2,
        CAST(PROPERTY_ADDR_3 AS VARCHAR(112)) AS PROPERTY_ADDR_3,
        prov.PROVINCE_CD as PROV,
        trim(
                regexp_replace(
                translate(
                    lower(coalesce(PROPERTY_ADDR_1, '')),
                    'àáâäãåçèéêëìíîïñòóôöõùúûüýÿ',
                    'aaaaaaceeeeiiiinooooouuuuyy'
                ),
                '[^a-z0-9]+',
                ' ',
                'g'
                )
            ) AS PROPERTY_ADDR_11,
        trim(
                regexp_replace(
                translate(
                    lower(coalesce(PROPERTY_ADDR_2, '')),
                    'àáâäãåçèéêëìíîïñòóôöõùúûüýÿ',
                    'aaaaaaceeeeiiiinooooouuuuyy'
                ),
                '[^a-z0-9]+',
                ' ',
                'g'
                )
            ) AS PROPERTY_ADDR_22,
        trim(
                regexp_replace(
                translate(
                    lower(coalesce(PROPERTY_ADDR_3, '')),
                    'àáâäãåçèéêëìíîïñòóôöõùúûüýÿ',
                    'aaaaaaceeeeiiiinooooouuuuyy'
                ),
                '[^a-z0-9]+',
                ' ',
                'g'
                )
            ) AS PROPERTY_ADDR_33
    FROM {{upstream_asset[1]}}
    left join {{upstream_asset[4]}} prov
    ON prov.PROVINCE_ID = prop_prov
    WHERE TM_ID = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}
),
filtered AS (
        SELECT
                LOCTN_LABEL_1,
                LOCTN_LABEL_2,
                PRPTY_LOCTN_NM
        FROM {{upstream_asset[3]}}
        WHERE EFF_TO_YR_MTH >= '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="yyyymm") }}'
          AND EFF_FROM_YR_MTH <= '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="yyyymm") }}'
),
dedup AS (
        SELECT
                LOCTN_LABEL_1,
                LOCTN_LABEL_2,
                PRPTY_LOCTN_NM,
                ROW_NUMBER() OVER (
                        PARTITION BY PRPTY_LOCTN_NM, LOCTN_LABEL_1
                        ORDER BY PRPTY_LOCTN_NM, LOCTN_LABEL_1
                ) AS rn
        FROM filtered
),
lkp_temp AS (
        SELECT
                LOCTN_LABEL_1,
                LOCTN_LABEL_2,
                PRPTY_LOCTN_NM
        FROM dedup
        WHERE rn = 1
        ORDER BY PRPTY_LOCTN_NM, LOCTN_LABEL_1
),
PRPTY_LOCTN_NM2 AS (
        SELECT
                *,
                trim(
                regexp_replace(
                translate(
                    lower(coalesce(PRPTY_LOCTN_NM, '')),
                    'àáâäãåçèéêëìíîïñòóôöõùúûüýÿ',
                    'aaaaaaceeeeiiiinooooouuuuyy'
                ),
                '[^a-z0-9]+',
                ' ',
                'g'
                )
                ) AS PRPTY_LOCTN_NM2
        FROM dedup
),
substrings AS (
        SELECT
                p.*,
                CASE
                        WHEN LENGTH(PRPTY_LOCTN_NM2) = 3 THEN 1
                        ELSE 2
                END AS SORT
        FROM PRPTY_LOCTN_NM2 p
),
substrings_c AS (
        SELECT *
        FROM (
                SELECT
                        *,
                        ROW_NUMBER() OVER (
                                PARTITION BY LOCTN_LABEL_1, SORT, PRPTY_LOCTN_NM
                                ORDER BY LOCTN_LABEL_1, SORT, PRPTY_LOCTN_NM
                        ) AS rn
                FROM substrings
        )
        WHERE rn = 1
),
candidates AS (
    SELECT
        s.MORT_NUM,
        t.loctn_label_1 AS prov,
        t.loctn_label_2 AS cma,
        t.prpty_loctn_nm,
        t.prpty_loctn_nm2,
        t.sort,
        CASE
            WHEN instr(' ' || lower(coalesce(s.PROPERTY_ADDR_33, '')) || ' ',
                       ' ' || lower(t.prpty_loctn_nm2) || ' ') > 0 THEN 1
            WHEN instr(' ' || lower(coalesce(s.PROPERTY_ADDR_22, '')) || ' ',
                       ' ' || lower(t.prpty_loctn_nm2) || ' ') > 0 THEN 2
            WHEN instr(' ' || lower(coalesce(s.PROPERTY_ADDR_11, '')) || ' ',
                       ' ' || lower(t.prpty_loctn_nm2) || ' ') > 0 THEN 3
            WHEN instr(lower(coalesce(s.PROPERTY_ADDR_33, '')),
                       lower(t.prpty_loctn_nm2)) > 0 THEN 4
            ELSE 99
        END AS match_rank
    FROM snapshot_c s
    JOIN substrings_c t
                ON s.prov = t.loctn_label_1
),
best_match AS (
    SELECT
        MORT_NUM,
        cma,
        prov,
        prpty_loctn_nm,
        row_number() OVER (
            PARTITION BY MORT_NUM
            ORDER BY match_rank, sort, length(prpty_loctn_nm2) DESC, prpty_loctn_nm2, cma
        ) AS rn
    FROM candidates
    WHERE match_rank < 99
)
SELECT
    b.basel_acct_id,
    '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate")}}' as OBSN_DT,
    prov.prov,
    d.stream,
    'MOR' AS SRC_SYS_CD,
    coalesce(m.cma, '11') as cma,
    CASE
        WHEN d.DLGD_F = 'N' THEN NULL
        ELSE coalesce(m.cma, '11')
    END AS metrpl_area_nm
FROM {{upstream_asset[0]}} b
LEFT JOIN best_match m
    ON b.MORT_NUM = m.MORT_NUM
        AND m.rn = 1
LEFT JOIN {{upstream_asset[2]}} d
    ON b.BASEL_ACCT_ID = d.BASEL_ACCT_ID
        AND d.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
        AND d.stream = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
LEFT JOIN (SELECT DISTINCT MORT_NUM, prov FROM snapshot_c) prov ON prov.MORT_NUM = b.MORT_NUM
WHERE b.mth_tm_id = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}