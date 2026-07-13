{% set upstream_asset = [
        "ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT",
        "ingestion.BASEL_STEP_PLN_MTH_SNAPSHOT",
        "instruments.DLGD_F",
        "ingestion.TERANET_ADDR_LKP_CMA"
        ] %}

WITH snapshot_c AS (
    SELECT
        MTH_TM_ID,
        STEP_PLN_AGRMNT_NUM,
        CAST(PRPTY_DESC_1 AS VARCHAR(112)) AS PRPTY_DESC_1,
        CAST(PRPTY_DESC_2 AS VARCHAR(112)) AS PRPTY_DESC_2,
        CAST(PRPTY_DESC_3 AS VARCHAR(112)) AS PRPTY_DESC_3,

        CASE
            WHEN UPPER(PRPTY_PROV_CD) = 'MN' THEN 'MB'
            WHEN UPPER(PRPTY_PROV_CD) = 'NF' THEN 'NL'
            WHEN UPPER(PRPTY_PROV_CD) = 'PQ' THEN 'QC'
            ELSE UPPER(PRPTY_PROV_CD)
        END AS PROV,
        trim(
                regexp_replace(
                        regexp_replace(
                        translate(lower(PRPTY_DESC_1),
                        'àâäçèéêëîïôùûüÿ,''/-',
                        'aaaceeeeiiouuuy    '
                        ),
                        '[^a-z0-9 ]',
                        '',
                        'g'
                ),
                '\s+',
                ' ',
                        'g'
                )
            ) AS PRPTY_DESC_11,

        trim(
                regexp_replace(
                        regexp_replace(
                        translate(lower(PRPTY_DESC_2),
                        'àâäçèéêëîïôùûüÿ,''/-',
                        'aaaceeeeiiouuuy    '
                        ),
                        '[^a-z0-9 ]',
                        '',
                        'g'
                ),
                '\s+',
                ' ',
                        'g'
                )
            ) AS PRPTY_DESC_22,

        trim(
                regexp_replace(
                        regexp_replace(
                        translate(lower(PRPTY_DESC_3),
                        'àâäçèéêëîïôùûüÿ,''/-',
                        'aaaceeeeiiouuuy    '
                        ),
                        '[^a-z0-9 ]',
                        '',
                        'g'
                ),
                '\s+',
                ' ',
                        'g'
                )
            ) AS PRPTY_DESC_33

    FROM {{upstream_asset[1]}}
    WHERE MTH_TM_ID = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}
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
                        regexp_replace(
                        translate(lower(PRPTY_LOCTN_NM),
                        'àâäçèéêëîïôùûüÿ,''/-',
                        'aaaceeeeiiouuuy    '
                        ),
                        '[^a-z0-9 ]',
                        '',
                        'g'
                ),
                '\s+',
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
prov_c AS (
        SELECT
                LOCTN_LABEL_1,
                "TABLE"
        FROM (
                SELECT DISTINCT LOCTN_LABEL_1, 'Snapshot' AS "TABLE" FROM snapshot_c
                UNION ALL
                SELECT DISTINCT LOCTN_LABEL_1, 'Substrings' AS "TABLE" FROM substrings_c
        )
        GROUP BY LOCTN_LABEL_1, "TABLE"
        ORDER BY LOCTN_LABEL_1
),
candidates AS (
    SELECT
        s.STEP_PLN_AGRMNT_NUM,
        t.loctn_label_1 AS prov,
        t.loctn_label_2 AS cma,
        t.prpty_loctn_nm,
        t.prpty_loctn_nm2,
        t.sort,
        CASE
            WHEN instr(' ' || s.PRPTY_DESC_33 || ' ',
                       ' ' || t.prpty_loctn_nm2 || ' ') > 0 THEN 1
            WHEN instr(' ' || s.PRPTY_DESC_22 || ' ',
                       ' ' || t.prpty_loctn_nm2 || ' ') > 0 THEN 2
            WHEN instr(' ' || s.PRPTY_DESC_11 || ' ',
                       ' ' || t.prpty_loctn_nm2 || ' ') > 0 THEN 3
            WHEN instr(s.PRPTY_DESC_33, t.prpty_loctn_nm2) > 0 THEN 4
            ELSE 99
        END AS match_rank
    FROM snapshot_c s
    JOIN substrings_c t
                ON s.prov = t.loctn_label_1
),
best_match AS (
    SELECT
        STEP_PLN_AGRMNT_NUM,
        cma,
        prpty_loctn_nm,
        prov,
        row_number() OVER (
            PARTITION BY STEP_PLN_AGRMNT_NUM
            ORDER BY match_rank, sort, prpty_loctn_nm2
        ) AS rn
    FROM candidates
    WHERE match_rank < 99
)
SELECT
    b.basel_acct_id,
    '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate")}}' as OBSN_DT,
    prov.prov,
    d.stream,
    'SPL' AS SRC_SYS_CD,
    coalesce(m.cma, '11')  as cma,
    CASE
        WHEN d.DLGD_F = 'N' THEN NULL
        ELSE coalesce(m.cma, '11')
    END AS metrpl_area_nm
FROM {{upstream_asset[0]}} b
LEFT JOIN best_match m
    ON b.STEP_PLN_AGRMNT_NUM = m.STEP_PLN_AGRMNT_NUM
        AND m.rn = 1
LEFT JOIN {{upstream_asset[2]}} d
    ON b.BASEL_ACCT_ID = d.BASEL_ACCT_ID
        AND d.OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
        AND d.stream = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
LEFT JOIN (SELECT DISTINCT STEP_PLN_AGRMNT_NUM, PROV FROM snapshot_c) prov ON prov.STEP_PLN_AGRMNT_NUM = b.STEP_PLN_AGRMNT_NUM
WHERE b.mth_tm_id = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}