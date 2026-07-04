"""
Rewrite of J_RRII_KS10_2103 via pre-materialized features tables.

Builds emulated.BASEL_REVLVNG_CR_BASE_DRVD_VARS_FEATURE_GENERATED:
  export_result — join features.* + lightweight assembly logic
  duckdb_load   — insert parquet into DuckLake

Requires the stream features DAG to have run for the process month.
PIT, exposure, and most lookup-derived columns come from features; inline
logic covers HELOC_F, STEP_CD, final PIT selection, PIT_STATUS_PRE_STEP
override, CONSM_SCORECRD_EXCLSN_F (CHG uses final PIT), and TOTAL_EXPSR_ABOVE_LMT_F.
"""

UPSTREAM_ASSET = [
    "ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT",
    "ingestion.PIT_STATUS_PRE_STEP",
    "ingestion.TM_DIM",
    "reference.BLOCK_RECL_LKP",
    "reference.BLOCK_RECL_CLS_RSN_LKP",
    "reference.SRC_PRD_LKP",
    "reference.RPTG_PRD_LKP_THRSHLD",
    "features.BASEL_PRD_CD",
    "features.PIT_STAT_VER_2_CD90",
    "features.PIT_STAT_VER_2_CD180",
    "features.REVISED_EXPSR_AMT",
    "features.SML_BUS_F",
    "features.TRNST_EXCLSN_F",
    "features.RS_F",
    "features.ACCRL_STAT_F",
    "features.LTV_TP_CD",
    "features.BNKRPY_F",
    "features.CONSM_PRD_TREATMNT_CD",
]

DOWNSTREAM_ASSET = "emulated.BASEL_REVLVNG_CR_BASE_DRVD_VARS_FEATURE_GENERATED"

_TASK_GROUP = "filteredtable__BASEL_REVLVNG_CR_BASE_DRVD_VARS_FEATURE_GENERATED"

DEPENDENCIES = {
    "duckdb_delete": ["export_result"],
    "export_result": ["duckdb_load"],
}


def duckdb_delete(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    DELETE FROM {DOWNSTREAM_ASSET}
    WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
      AND STREAM = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}'
    """,
):
    pass


def export_result(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    WITH
        ymt AS (
            SELECT STRFTIME(TM_LVL_END_DT, '%Y%m') AS yrmth
            FROM ingestion.TM_DIM
            WHERE TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
        ),
        lk_cc1 AS (
            SELECT CSEF_CONDITION_1, BLOCK_RECL_CD
            FROM (
                SELECT
                    TRIM(CONSM_SCORECRD_EXCLSN_F) AS CSEF_CONDITION_1,
                    TRIM(BLOCK_RECL_CD) AS BLOCK_RECL_CD
                FROM reference.BLOCK_RECL_LKP, ymt
                WHERE ymt.yrmth BETWEEN EFF_FROM_YR_MTH AND EFF_TO_YR_MTH
                  AND TRIM(BLOCK_RECL_CD) <> ''
            )
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY BLOCK_RECL_CD ORDER BY CSEF_CONDITION_1
            ) = 1
        ),
        lk_cc2 AS (
            SELECT CSEF_CONDITION_2, CLS_RSN_CD, BLOCK_RECL_CD
            FROM (
                SELECT
                    TRIM(CONSM_SCORECRD_EXCLSN_F) AS CSEF_CONDITION_2,
                    TRIM(CLS_RSN_CD) AS CLS_RSN_CD,
                    TRIM(BLOCK_RECL_CD) AS BLOCK_RECL_CD
                FROM reference.BLOCK_RECL_CLS_RSN_LKP, ymt
                WHERE ymt.yrmth BETWEEN EFF_FROM_YR_MTH AND EFF_TO_YR_MTH
                  AND (TRIM(CLS_RSN_CD) <> '' OR TRIM(BLOCK_RECL_CD) <> '')
            )
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY BLOCK_RECL_CD, CLS_RSN_CD ORDER BY CSEF_CONDITION_2
            ) = 1
        ),
        lk_cc3 AS (
            SELECT CSEF_CONDITION_3, SRC_PRD_CD, SRC_SUB_PRD_CD
            FROM (
                SELECT
                    TRIM(CONSM_SCORECRD_EXCLSN_F) AS CSEF_CONDITION_3,
                    TRIM(SRC_PRD_CD) AS SRC_PRD_CD,
                    TRIM(SRC_SUB_PRD_CD) AS SRC_SUB_PRD_CD
                FROM reference.SRC_PRD_LKP, ymt
                WHERE TRIM(PRD_SYS_CD) = 'KS'
                  AND ymt.yrmth BETWEEN EFF_FROM_YR_MTH AND EFF_TO_YR_MTH
            )
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY SRC_PRD_CD, SRC_SUB_PRD_CD ORDER BY CSEF_CONDITION_3
            ) = 1
        ),
        base AS (
            SELECT
                a.MTH_TM_ID,
                a.BASEL_ACCT_ID,
                a.PRIM_BASEL_CUST_ID AS BASEL_CUST_ID,
                a.ACCT_NUM,
                a.STEP_PLN_SNAPSHOT_ID,
                TRIM(a.PRD_CD) AS PRD_CD,
                TRIM(a.SUB_PRD_CD) AS SUB_PRD_CD,
                TRIM(a.BLOCK_RECL_CD) AS BLOCK_RECL_CD,
                TRIM(a.ACCT_CLS_RSN_CD) AS ACCT_CLS_RSN_CD,
                a.CR_LMT_AMT,
                a.TOT_NEW_BAL_AMT,
                TRIM(a.SCRD_TP_CD) AS SCRD_TP_CD,
                TRIM(a.CRNT_BILL_CD) AS CRNT_BILL_CD,
                SUBSTR(TRIM(a.CRNT_BILL_CD), 1, 1) AS BILL_CD_CHAR_1,
                SUBSTR(TRIM(a.CRNT_BILL_CD), 1, 2) AS BILL_CD_CHAR_2,
                CASE
                    WHEN a.SUB_PRD_CD = 'RS'
                      OR a.STEP_PLN_SNAPSHOT_ID NOT IN (-1, -2)
                    THEN 'Y'
                    ELSE 'N'
                END AS HELOC_F
            FROM ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT a
            WHERE a.MTH_TM_ID =
                {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
        ),
        joined AS (
            SELECT
                b.*,
                prd.BASEL_PRD_CD,
                prd.BASEL_PRD_DESC,
                pit90.PIT_STAT_VER_2_CD90,
                pit180.PIT_STAT_VER_2_CD180,
                expsr.REVISED_EXPSR_AMT,
                sml.SML_BUS_F,
                trnst.TRNST_EXCLSN_F,
                rs.RS_F,
                accrl.ACCRL_STAT_F,
                ltv.LTV_TP_CD,
                bnkrpy.BNKRPY_F,
                trt.CONSM_PRD_TREATMNT_CD,
                cc1.CSEF_CONDITION_1,
                cc2.CSEF_CONDITION_2,
                cc3.CSEF_CONDITION_3 AS CONSM_SCORECRD_EXCLSN_F3,
                CASE
                    WHEN b.STEP_PLN_SNAPSHOT_ID NOT IN (-1, -2) THEN 'Y'
                    WHEN b.STEP_PLN_SNAPSHOT_ID IN (-1, -2) AND b.PRD_CD IN ('SCL', 'VIC') THEN
                        CASE
                            WHEN b.SCRD_TP_CD = 'U' THEN 'U'
                            WHEN b.BILL_CD_CHAR_1 = 'U' THEN 'U'
                            WHEN b.BILL_CD_CHAR_2 IN ('11', 'SB', 'SN', 'SP', 'SR', 'ST') THEN 'R'
                            ELSE 'O'
                        END
                    WHEN b.STEP_PLN_SNAPSHOT_ID IN (-1, -2) AND b.PRD_CD NOT IN ('SCL', 'VIC') THEN 'N'
                END AS STEP_CD
            FROM base b
            LEFT JOIN features.BASEL_PRD_CD prd
                ON b.BASEL_ACCT_ID = prd.BASEL_ACCT_ID
               AND prd.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
            LEFT JOIN features.PIT_STAT_VER_2_CD90 pit90
                ON b.BASEL_ACCT_ID = pit90.BASEL_ACCT_ID
               AND pit90.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
            LEFT JOIN features.PIT_STAT_VER_2_CD180 pit180
                ON b.BASEL_ACCT_ID = pit180.BASEL_ACCT_ID
               AND pit180.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
            LEFT JOIN features.REVISED_EXPSR_AMT expsr
                ON b.BASEL_ACCT_ID = expsr.BASEL_ACCT_ID
               AND expsr.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
            LEFT JOIN features.SML_BUS_F sml
                ON b.BASEL_ACCT_ID = sml.BASEL_ACCT_ID
               AND sml.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
            LEFT JOIN features.TRNST_EXCLSN_F trnst
                ON b.BASEL_ACCT_ID = trnst.BASEL_ACCT_ID
               AND trnst.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
            LEFT JOIN features.RS_F rs
                ON b.BASEL_ACCT_ID = rs.BASEL_ACCT_ID
               AND rs.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
            LEFT JOIN features.ACCRL_STAT_F accrl
                ON b.BASEL_ACCT_ID = accrl.BASEL_ACCT_ID
               AND accrl.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
            LEFT JOIN features.LTV_TP_CD ltv
                ON b.BASEL_ACCT_ID = ltv.BASEL_ACCT_ID
               AND ltv.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
            LEFT JOIN features.BNKRPY_F bnkrpy
                ON b.BASEL_ACCT_ID = bnkrpy.BASEL_ACCT_ID
               AND bnkrpy.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
            LEFT JOIN features.CONSM_PRD_TREATMNT_CD trt
                ON b.BASEL_ACCT_ID = trt.BASEL_ACCT_ID
               AND trt.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
            LEFT JOIN lk_cc1 cc1
                ON b.BLOCK_RECL_CD = cc1.BLOCK_RECL_CD
            LEFT JOIN lk_cc2 cc2
                ON b.BLOCK_RECL_CD = cc2.BLOCK_RECL_CD
               AND b.ACCT_CLS_RSN_CD = cc2.CLS_RSN_CD
            LEFT JOIN lk_cc3 cc3
                ON b.PRD_CD = cc3.SRC_PRD_CD
               AND b.SUB_PRD_CD = cc3.SRC_SUB_PRD_CD
        ),
        derived AS (
            SELECT
                *,
                CASE
                    WHEN BASEL_PRD_CD = 'CC' AND HELOC_F = 'N'
                    THEN PIT_STAT_VER_2_CD180
                    ELSE PIT_STAT_VER_2_CD90
                END AS PIT_STAT_VER_2_CD_PRE,
                CASE
                    WHEN CSEF_CONDITION_1 = 'Y' THEN 'Y'
                    WHEN CSEF_CONDITION_2 = 'Y' THEN 'Y'
                    WHEN CONSM_SCORECRD_EXCLSN_F3 = 'Y' THEN 'Y'
                    WHEN TOT_NEW_BAL_AMT <= 0 AND CR_LMT_AMT <= 0 THEN 'Y'
                    WHEN TOT_NEW_BAL_AMT <= 0
                         AND (SUBSTR(BLOCK_RECL_CD, 1, 1) = 'V' OR BLOCK_RECL_CD = 'FX')
                    THEN 'Y'
                    WHEN (
                        CASE
                            WHEN BASEL_PRD_CD = 'CC' AND HELOC_F = 'N'
                            THEN PIT_STAT_VER_2_CD180
                            ELSE PIT_STAT_VER_2_CD90
                        END
                    ) = 'CHG'
                    THEN 'Y'
                    ELSE 'N'
                END AS CONSM_SCORECRD_EXCLSN_F
            FROM joined
        ),
        with_pit_override AS (
            SELECT
                d.* EXCLUDE (PIT_STAT_VER_2_CD_PRE),
                CASE
                    WHEN pit.CROSS_DFLT_PIT_OVERRIDE_F = 'Y'
                    THEN pit.CROSS_DFLT_PIT_STATUS
                    ELSE d.PIT_STAT_VER_2_CD_PRE
                END AS PIT_STAT_VER_2_CD
            FROM derived d
            LEFT JOIN ingestion.PIT_STATUS_PRE_STEP pit
                ON d.BASEL_ACCT_ID = pit.BASEL_ACCT_ID
               AND pit.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
               AND pit.SRC_SYS_CD = 'KS'
               AND pit.CROSS_DFLT_PIT_OVERRIDE_F = 'Y'
        ),
        thrshld AS (
            SELECT a.THRSHLD
            FROM reference.RPTG_PRD_LKP_THRSHLD a
            INNER JOIN ingestion.TM_DIM b
                ON b.TM_LVL = 'Month'
               AND b.TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
               AND b.TM_LVL_END_DT BETWEEN a.EFF_FROM_YR_MTH AND a.EFF_TO_YR_MTH
            LIMIT 1
        ),
        tot_expsr AS (
            SELECT
                BASEL_CUST_ID,
                BASEL_PRD_CD,
                SUM(REVISED_EXPSR_AMT) AS TOT_EXPSR
            FROM with_pit_override
            WHERE BASEL_CUST_ID > 0
              AND CONSM_PRD_TREATMNT_CD = 'A'
              AND HELOC_F = 'N'
              AND PIT_STAT_VER_2_CD IN ('CUR', 'DEF')
              AND SML_BUS_F = 'N'
              AND TRNST_EXCLSN_F = 'N'
              AND BASEL_PRD_CD IN ('CC', 'LOC', 'SL A')
            GROUP BY BASEL_CUST_ID, BASEL_PRD_CD
        ),
        with_expsr_flag AS (
            SELECT
                a.*,
                CASE
                    WHEN a.CONSM_PRD_TREATMNT_CD = 'A'
                         AND a.HELOC_F = 'N'
                         AND a.PIT_STAT_VER_2_CD IN ('CUR', 'DEF')
                         AND a.SML_BUS_F = 'N'
                         AND a.TRNST_EXCLSN_F = 'N'
                         AND te.TOT_EXPSR IS NULL
                    THEN ''
                    WHEN a.CONSM_PRD_TREATMNT_CD = 'A'
                         AND a.HELOC_F = 'N'
                         AND a.PIT_STAT_VER_2_CD IN ('CUR', 'DEF')
                         AND a.SML_BUS_F = 'N'
                         AND a.TRNST_EXCLSN_F = 'N'
                         AND te.TOT_EXPSR > (SELECT THRSHLD FROM thrshld)
                    THEN 'Y'
                    WHEN a.CONSM_PRD_TREATMNT_CD = 'A'
                         AND a.HELOC_F = 'N'
                         AND a.PIT_STAT_VER_2_CD IN ('CUR', 'DEF')
                         AND a.SML_BUS_F = 'N'
                         AND a.TRNST_EXCLSN_F = 'N'
                    THEN 'N'
                    ELSE NULL
                END AS TOTAL_EXPSR_ABOVE_LMT_F_BASE
            FROM with_pit_override a
            LEFT JOIN tot_expsr te
                ON a.BASEL_CUST_ID = te.BASEL_CUST_ID
               AND a.BASEL_PRD_CD = te.BASEL_PRD_CD
               AND a.CONSM_PRD_TREATMNT_CD = 'A'
               AND a.HELOC_F = 'N'
               AND a.PIT_STAT_VER_2_CD IN ('CUR', 'DEF')
               AND a.SML_BUS_F = 'N'
               AND a.TRNST_EXCLSN_F = 'N'
        )
    SELECT
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}' AS STREAM,
        MTH_TM_ID,
        BASEL_ACCT_ID,
        BASEL_CUST_ID,
        ACCT_NUM,
        BASEL_PRD_CD,
        BASEL_PRD_DESC,
        CONSM_SCORECRD_EXCLSN_F,
        CONSM_PRD_TREATMNT_CD,
        HELOC_F,
        PIT_STAT_VER_2_CD,
        REVISED_EXPSR_AMT,
        RS_F,
        SML_BUS_F,
        STEP_CD,
        TRNST_EXCLSN_F,
        ACCRL_STAT_F,
        LTV_TP_CD,
        BNKRPY_F,
        PIT_STAT_VER_2_CD90,
        PIT_STAT_VER_2_CD180,
        CASE
            WHEN BASEL_CUST_ID = -1
                 AND CONSM_PRD_TREATMNT_CD = 'A'
                 AND HELOC_F = 'N'
                 AND PIT_STAT_VER_2_CD IN ('CUR', 'DEF')
                 AND SML_BUS_F = 'N'
                 AND TRNST_EXCLSN_F = 'N'
                 AND BASEL_PRD_CD IN ('CC', 'LOC', 'SL A')
            THEN CASE
                WHEN REVISED_EXPSR_AMT > (SELECT THRSHLD FROM thrshld) THEN 'Y'
                ELSE 'N'
            END
            WHEN CONSM_PRD_TREATMNT_CD = 'A'
                 AND (HELOC_F = 'Y' OR BASEL_PRD_CD IN ('SL', 'SL B'))
                 AND PIT_STAT_VER_2_CD IN ('CUR', 'DEF')
                 AND SML_BUS_F = 'N'
                 AND TRNST_EXCLSN_F = 'N'
            THEN 'N'
            ELSE TOTAL_EXPSR_ABOVE_LMT_F_BASE
        END AS TOTAL_EXPSR_ABOVE_LMT_F
    FROM with_expsr_flag
    """,
):
    pass


def duckdb_load(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} BY NAME
    SELECT *
    FROM read_parquet(
        '{{{{ task_instance.xcom_pull(task_ids="{_TASK_GROUP}.export_result", key="parquet") }}}}'
    )
    """,
):
    pass
