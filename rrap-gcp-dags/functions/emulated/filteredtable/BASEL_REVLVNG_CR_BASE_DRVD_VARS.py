"""
Rewrite of J_RRII_KS10_2103_BASEL_REVLVNG_CR_BASE_DRVD_VARS.sas only.

Builds emulated.BASEL_REVLVNG_CR_BASE_DRVD_VARS in a single DuckDB pipeline.
PIT cross-default overrides are applied via a read of ingestion.PIT_STATUS_PRE_STEP
(as in the SAS job); that table is populated upstream (not by this module).
"""

UPSTREAM_ASSET = [
    "ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT",
    "ingestion.PIT_STATUS_PRE_STEP",
    "ingestion.TM_DIM",
    "reference.BLOCK_RECL_LKP",
    "reference.BLOCK_RECL_CLS_RSN_LKP",
    "reference.CHRG_OFF_LKP",
    "reference.SRC_PRD_LKP",
    "reference.SRC_PRD_STDNT_LOAN_LKP",
    "reference.TRNST_EXCLSN_LKP",
    "reference.RPTG_PRD_LKP_THRSHLD",
]

DOWNSTREAM_ASSET = "emulated.BASEL_REVLVNG_CR_BASE_DRVD_VARS"

DEPENDENCIES = {
    "duckdb_delete": ["duckdb_load"],
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


def duckdb_load(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} BY NAME
    WITH
        ymt AS (
            SELECT STRFTIME(TM_LVL_END_DT, '%Y%m') AS yrmth
            FROM ingestion.TM_DIM
            WHERE TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
        ),
        snap AS (
            SELECT
                a.MTH_TM_ID,
                a.BASEL_ACCT_ID,
                a.PRIM_BASEL_CUST_ID,
                a.PRIM_BASEL_CUST_ID AS BASEL_CUST_ID,
                a.STEP_PLN_SNAPSHOT_ID,
                a.ACCT_NUM,
                a.PRD_CD,
                a.SUB_PRD_CD,
                a.BLOCK_RECL_CD,
                a.TOT_NEW_BAL_AMT,
                a.CR_LMT_AMT,
                a.ACCT_CLS_RSN_CD,
                a.CHRG_OFF_CD,
                a.BNS_DLQNT_DAY,
                a.TOT_UNPAID_FNCL_CHRG_AMT,
                a.CRNT_BILL_CD,
                a.SCRD_TP_CD,
                a.SWITCH_XREF,
                a.SCRTY_TP_CD,
                a.TRNST_NUM,
                c.EXCLUDED_TRNST_NUM,
                CASE
                    WHEN a.SUB_PRD_CD = 'RS'
                      OR a.STEP_PLN_SNAPSHOT_ID NOT IN (-1, -2)
                    THEN 'Y'
                    ELSE 'N'
                END AS HELOC_F,
                CASE
                    WHEN lk_recl.BLOCK_RECL_CD IS NULL THEN '1'
                    ELSE '0'
                END AS v_PT_STAT_BLCK_RECL_CD_LKP_CUR,
                CASE
                    WHEN a.CR_LMT_AMT > a.TOT_NEW_BAL_AMT THEN a.CR_LMT_AMT
                    ELSE a.TOT_NEW_BAL_AMT
                END AS REVISED_EXPSR_AMT
            FROM ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT a
            CROSS JOIN ymt
            LEFT JOIN reference.TRNST_EXCLSN_LKP c
                ON a.TRNST_NUM = c.EXCLUDED_TRNST_NUM
            LEFT JOIN (
                SELECT BLOCK_RECL_CD
                FROM reference.BLOCK_RECL_LKP, ymt
                WHERE BNKRPY_F = 'Y'
                  AND ymt.yrmth BETWEEN EFF_FROM_YR_MTH AND EFF_TO_YR_MTH
            ) lk_recl
                ON a.BLOCK_RECL_CD = lk_recl.BLOCK_RECL_CD
            WHERE a.MTH_TM_ID =
                {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
        ),
        prev_snap AS (
            SELECT
                a.BASEL_ACCT_ID,
                a.PRIM_BASEL_CUST_ID AS BASEL_CUST_ID,
                a.TOT_NEW_BAL_AMT AS PREV_TOT_NEW_BAL_AMT,
                a.CHRG_OFF_CD AS PREV_CHRG_OFF_CD,
                a.BLOCK_RECL_CD AS PREV_BLOCK_RECL_CD,
                CASE
                    WHEN a.TOT_NEW_BAL_AMT > 0 AND lk_recl.BLOCK_RECL_CD IS NULL THEN '1'
                    ELSE '0'
                END AS v_PT_STAT_BLCK_RECL_CD_LKP_PRV,
                CASE
                    WHEN lk_chrg.CHRG_OFF_CD IS NULL THEN '1'
                    ELSE '0'
                END AS v_PT_STAT_CHRG_OFF_LKP_PREV2,
                a.TOT_UNPAID_FNCL_CHRG_AMT AS PREV_TOT_UNPAID_FNCL_CHRG_AMT
            FROM ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT a
            CROSS JOIN ymt
            LEFT JOIN (
                SELECT BLOCK_RECL_CD
                FROM reference.BLOCK_RECL_LKP, ymt
                WHERE BNKRPY_F = 'Y'
                  AND ymt.yrmth BETWEEN EFF_FROM_YR_MTH AND EFF_TO_YR_MTH
            ) lk_recl
                ON a.BLOCK_RECL_CD = lk_recl.BLOCK_RECL_CD
            LEFT JOIN (
                SELECT CHRG_OFF_CD
                FROM reference.CHRG_OFF_LKP, ymt
                WHERE CHRG_OFF_STAT_F = 'Y'
                  AND ymt.yrmth BETWEEN EFF_FROM_YR_MTH AND EFF_TO_YR_MTH
            ) lk_chrg
                ON a.CHRG_OFF_CD = lk_chrg.CHRG_OFF_CD
            WHERE a.MTH_TM_ID =
                {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}} - 40
        ),
        snap_with_pit AS (
            SELECT
                s.*,
                ps.PREV_TOT_NEW_BAL_AMT,
                ps.PREV_CHRG_OFF_CD,
                ps.PREV_BLOCK_RECL_CD,
                ps.PREV_TOT_UNPAID_FNCL_CHRG_AMT,
                ps.v_PT_STAT_BLCK_RECL_CD_LKP_PRV,
                ps.v_PT_STAT_CHRG_OFF_LKP_PREV2,
                CASE
                    WHEN s.HELOC_F = 'N' THEN
                        CASE
                            WHEN TRIM(s.CHRG_OFF_CD) = '1' THEN 'CHG'
                            WHEN s.BNS_DLQNT_DAY < 120
                                 AND NOT (s.TOT_NEW_BAL_AMT > 0 AND TRIM(s.CHRG_OFF_CD) IN ('N', 'Q'))
                                 AND s.v_PT_STAT_BLCK_RECL_CD_LKP_CUR = '1'
                            THEN 'CUR'
                            WHEN s.TOT_NEW_BAL_AMT > 0
                                 AND s.TOT_NEW_BAL_AMT = s.TOT_UNPAID_FNCL_CHRG_AMT
                                 AND ps.v_PT_STAT_CHRG_OFF_LKP_PREV2 = '1'
                                 AND NOT (ps.PREV_TOT_NEW_BAL_AMT > 0 AND TRIM(ps.PREV_CHRG_OFF_CD) IN ('N', 'Q'))
                                 AND (ps.PREV_TOT_NEW_BAL_AMT > 0 AND ps.v_PT_STAT_BLCK_RECL_CD_LKP_PRV = '1')
                            THEN 'CUR'
                            WHEN s.TOT_NEW_BAL_AMT = 0
                                 AND ps.PREV_TOT_NEW_BAL_AMT = ps.PREV_TOT_UNPAID_FNCL_CHRG_AMT
                                 AND ps.v_PT_STAT_CHRG_OFF_LKP_PREV2 = '1'
                                 AND NOT (ps.PREV_TOT_NEW_BAL_AMT > 0 AND TRIM(ps.PREV_CHRG_OFF_CD) IN ('N', 'Q'))
                                 AND (ps.PREV_TOT_NEW_BAL_AMT > 0 AND ps.v_PT_STAT_BLCK_RECL_CD_LKP_PRV = '1')
                            THEN 'CUR'
                            ELSE 'DEF'
                        END
                    ELSE
                        CASE
                            WHEN TRIM(s.CHRG_OFF_CD) = '1' THEN 'CHG'
                            WHEN s.BNS_DLQNT_DAY < 120
                                 AND NOT (s.TOT_NEW_BAL_AMT > 0 AND TRIM(s.CHRG_OFF_CD) IN ('N', 'Q'))
                            THEN 'CUR'
                            WHEN s.TOT_NEW_BAL_AMT > 0
                                 AND s.TOT_NEW_BAL_AMT = s.TOT_UNPAID_FNCL_CHRG_AMT
                                 AND TRIM(s.CHRG_OFF_CD) <> '1'
                                 AND NOT (ps.PREV_TOT_NEW_BAL_AMT > 0 AND TRIM(ps.PREV_CHRG_OFF_CD) IN ('N', 'Q'))
                                 AND ps.PREV_TOT_NEW_BAL_AMT > 0
                            THEN 'CUR'
                            WHEN s.TOT_NEW_BAL_AMT = 0
                                 AND ps.PREV_TOT_NEW_BAL_AMT = ps.PREV_TOT_UNPAID_FNCL_CHRG_AMT
                                 AND TRIM(ps.PREV_CHRG_OFF_CD) <> '1'
                                 AND NOT (ps.PREV_TOT_NEW_BAL_AMT > 0 AND TRIM(ps.PREV_CHRG_OFF_CD) IN ('N', 'Q'))
                                 AND ps.PREV_TOT_NEW_BAL_AMT > 0
                            THEN 'CUR'
                            ELSE 'DEF'
                        END
                END AS OLD_PIT_STAT_VER_2_CD,
                CASE
                    WHEN s.HELOC_F = 'N' THEN
                        CASE
                            WHEN TRIM(s.CHRG_OFF_CD) = '1' THEN 'CHG'
                            WHEN s.BNS_DLQNT_DAY < 210
                                 AND NOT (s.TOT_NEW_BAL_AMT > 0 AND TRIM(s.CHRG_OFF_CD) IN ('N', 'Q'))
                                 AND s.v_PT_STAT_BLCK_RECL_CD_LKP_CUR = '1'
                            THEN 'CUR'
                            WHEN s.TOT_NEW_BAL_AMT > 0
                                 AND s.TOT_NEW_BAL_AMT = s.TOT_UNPAID_FNCL_CHRG_AMT
                                 AND ps.v_PT_STAT_CHRG_OFF_LKP_PREV2 = '1'
                                 AND NOT (ps.PREV_TOT_NEW_BAL_AMT > 0 AND TRIM(ps.PREV_CHRG_OFF_CD) IN ('N', 'Q'))
                                 AND (ps.PREV_TOT_NEW_BAL_AMT > 0 AND ps.v_PT_STAT_BLCK_RECL_CD_LKP_PRV = '1')
                            THEN 'CUR'
                            WHEN s.TOT_NEW_BAL_AMT = 0
                                 AND ps.PREV_TOT_NEW_BAL_AMT = ps.PREV_TOT_UNPAID_FNCL_CHRG_AMT
                                 AND ps.v_PT_STAT_CHRG_OFF_LKP_PREV2 = '1'
                                 AND NOT (ps.PREV_TOT_NEW_BAL_AMT > 0 AND TRIM(ps.PREV_CHRG_OFF_CD) IN ('N', 'Q'))
                                 AND (ps.PREV_TOT_NEW_BAL_AMT > 0 AND ps.v_PT_STAT_BLCK_RECL_CD_LKP_PRV = '1')
                            THEN 'CUR'
                            ELSE 'DEF'
                        END
                    ELSE
                        CASE
                            WHEN TRIM(s.CHRG_OFF_CD) = '1' THEN 'CHG'
                            WHEN s.BNS_DLQNT_DAY < 210
                                 AND NOT (s.TOT_NEW_BAL_AMT > 0 AND TRIM(s.CHRG_OFF_CD) IN ('N', 'Q'))
                            THEN 'CUR'
                            WHEN s.TOT_NEW_BAL_AMT > 0
                                 AND s.TOT_NEW_BAL_AMT = s.TOT_UNPAID_FNCL_CHRG_AMT
                                 AND TRIM(s.CHRG_OFF_CD) <> '1'
                                 AND NOT (ps.PREV_TOT_NEW_BAL_AMT > 0 AND TRIM(ps.PREV_CHRG_OFF_CD) IN ('N', 'Q'))
                                 AND ps.PREV_TOT_NEW_BAL_AMT > 0
                            THEN 'CUR'
                            WHEN s.TOT_NEW_BAL_AMT = 0
                                 AND ps.PREV_TOT_NEW_BAL_AMT = ps.PREV_TOT_UNPAID_FNCL_CHRG_AMT
                                 AND TRIM(ps.PREV_CHRG_OFF_CD) <> '1'
                                 AND NOT (ps.PREV_TOT_NEW_BAL_AMT > 0 AND TRIM(ps.PREV_CHRG_OFF_CD) IN ('N', 'Q'))
                                 AND ps.PREV_TOT_NEW_BAL_AMT > 0
                            THEN 'CUR'
                            ELSE 'DEF'
                        END
                END AS PIT_STAT_VER_2_CD_INTERIM
            FROM snap s
            LEFT JOIN prev_snap ps
                ON s.BASEL_ACCT_ID = ps.BASEL_ACCT_ID
               AND s.BASEL_CUST_ID = ps.BASEL_CUST_ID
        ),
        snap_cd AS (
            SELECT DISTINCT
                a.MTH_TM_ID,
                a.BASEL_ACCT_ID,
                a.BASEL_CUST_ID,
                a.PRIM_BASEL_CUST_ID,
                a.STEP_PLN_SNAPSHOT_ID,
                a.ACCT_NUM,
                a.PRD_CD,
                a.SUB_PRD_CD,
                a.BLOCK_RECL_CD,
                a.TOT_NEW_BAL_AMT,
                a.CR_LMT_AMT,
                a.ACCT_CLS_RSN_CD,
                a.CHRG_OFF_CD,
                a.BNS_DLQNT_DAY,
                a.TOT_UNPAID_FNCL_CHRG_AMT,
                a.CRNT_BILL_CD,
                a.SCRD_TP_CD,
                a.SWITCH_XREF,
                a.SCRTY_TP_CD,
                a.TRNST_NUM,
                a.EXCLUDED_TRNST_NUM,
                a.HELOC_F,
                a.PREV_TOT_NEW_BAL_AMT,
                a.PREV_BLOCK_RECL_CD,
                a.PREV_TOT_UNPAID_FNCL_CHRG_AMT,
                a.PIT_STAT_VER_2_CD_INTERIM,
                a.OLD_PIT_STAT_VER_2_CD,
                a.REVISED_EXPSR_AMT,
                bf.BNKRPY_F,
                sp.CONSM_SCORECRD_EXCLSN_F AS CONSM_SCORECRD_EXCLSN_F3,
                cc1.CSEF_CONDITION_1,
                cc2.CSEF_CONDITION_2,
                asf.ACCRL_STAT_F,
                CASE
                    WHEN sp.SRC_PRD_CD = 'SSL' THEN std.BASEL_PRD_DESC
                    ELSE sp.BASEL_PRD_DESC
                END AS BASEL_PRD_DESC,
                sp.LTV_TP_CD,
                sp.SML_BUS_F,
                sp.CONSM_PRD_TREATMNT_CD,
                CASE
                    WHEN sp.SRC_PRD_CD = 'SSL' THEN std.BASEL_PRD_CD
                    ELSE sp.BASEL_PRD_CD
                END AS BASEL_PRD_CD,
                sp.SRC_SUB_PRD_CD,
                SUBSTR(TRIM(a.CRNT_BILL_CD), 3, 1) AS BILL_CD_CHAR1,
                std.BILL_CD_CHAR
            FROM snap_with_pit a
            CROSS JOIN ymt
            LEFT JOIN (
                SELECT DISTINCT
                    TRIM(BNKRPY_F) AS BNKRPY_F,
                    TRIM(BLOCK_RECL_CD) AS BLOCK_RECL_CD
                FROM reference.BLOCK_RECL_LKP, ymt
                WHERE ymt.yrmth BETWEEN EFF_FROM_YR_MTH AND EFF_TO_YR_MTH
            ) bf
                ON TRIM(a.BLOCK_RECL_CD) = bf.BLOCK_RECL_CD
            LEFT JOIN (
                SELECT DISTINCT
                    TRIM(CONSM_SCORECRD_EXCLSN_F) AS CONSM_SCORECRD_EXCLSN_F,
                    TRIM(BLOCK_RECL_CD) AS BLOCK_RECL_CD
                FROM reference.BLOCK_RECL_CLS_RSN_LKP, ymt
                WHERE ymt.yrmth BETWEEN EFF_FROM_YR_MTH AND EFF_TO_YR_MTH
            ) cs
                ON TRIM(a.BLOCK_RECL_CD) = cs.BLOCK_RECL_CD
            LEFT JOIN (
                SELECT DISTINCT
                    TRIM(ACCRL_STAT_F) AS ACCRL_STAT_F,
                    TRIM(CHRG_OFF_CD) AS CHRG_OFF_CD
                FROM reference.CHRG_OFF_LKP, ymt
                WHERE ymt.yrmth BETWEEN EFF_FROM_YR_MTH AND EFF_TO_YR_MTH
                  AND TRIM(CHRG_OFF_STAT_F) = 'Y'
                  AND TRIM(CHRG_OFF_CD) IN (
                      SELECT TRIM(CHRG_OFF_CD)
                      FROM reference.CHRG_OFF_LKP, ymt y2
                      WHERE TRIM(ACCRL_STAT_F) IN ('N', 'Q')
                        AND y2.yrmth BETWEEN EFF_FROM_YR_MTH AND EFF_TO_YR_MTH
                  )
            ) asf
                ON TRIM(a.CHRG_OFF_CD) = asf.CHRG_OFF_CD
            LEFT JOIN (
                SELECT DISTINCT
                    TRIM(BASEL_PRD_CD) AS BASEL_PRD_CD,
                    TRIM(BASEL_PRD_DESC) AS BASEL_PRD_DESC,
                    TRIM(LTV_TP_CD) AS LTV_TP_CD,
                    TRIM(SML_BUS_F) AS SML_BUS_F,
                    TRIM(CONSM_SCORECRD_EXCLSN_F) AS CONSM_SCORECRD_EXCLSN_F,
                    TRIM(CONSM_PRD_TREATMNT_CD) AS CONSM_PRD_TREATMNT_CD,
                    TRIM(SRC_PRD_CD) AS SRC_PRD_CD,
                    TRIM(SRC_SUB_PRD_CD) AS SRC_SUB_PRD_CD
                FROM reference.SRC_PRD_LKP, ymt
                WHERE TRIM(PRD_SYS_CD) = 'KS'
                  AND ymt.yrmth BETWEEN EFF_FROM_YR_MTH AND EFF_TO_YR_MTH
            ) sp
                ON TRIM(a.PRD_CD) = sp.SRC_PRD_CD
               AND TRIM(a.SUB_PRD_CD) = sp.SRC_SUB_PRD_CD
            LEFT JOIN (
                SELECT DISTINCT
                    TRIM(BASEL_PRD_CD) AS BASEL_PRD_CD,
                    TRIM(BASEL_PRD_DESC) AS BASEL_PRD_DESC,
                    TRIM(SRC_PRD_CD) AS SRC_PRD_CD,
                    TRIM(SRC_SUB_PRD_CD) AS SRC_SUB_PRD_CD,
                    TRIM(BILL_CD_CHAR) AS BILL_CD_CHAR
                FROM reference.SRC_PRD_STDNT_LOAN_LKP, ymt
                WHERE ymt.yrmth BETWEEN EFF_FROM_YR_MTH AND EFF_TO_YR_MTH
                  AND TRIM(PRD_SYS_CD) = 'KS'
            ) std
                ON TRIM(a.PRD_CD) = std.SRC_PRD_CD
               AND TRIM(a.SUB_PRD_CD) = std.SRC_SUB_PRD_CD
               AND SUBSTR(TRIM(a.CRNT_BILL_CD), 3, 1) = std.BILL_CD_CHAR
            LEFT JOIN (
                SELECT DISTINCT
                    TRIM(CONSM_SCORECRD_EXCLSN_F) AS CSEF_CONDITION_1,
                    TRIM(BLOCK_RECL_CD) AS BLOCK_RECL_CD
                FROM reference.BLOCK_RECL_LKP, ymt
                WHERE ymt.yrmth BETWEEN EFF_FROM_YR_MTH AND EFF_TO_YR_MTH
                  AND TRIM(BLOCK_RECL_CD) <> ''
            ) cc1
                ON TRIM(a.BLOCK_RECL_CD) = cc1.BLOCK_RECL_CD
            LEFT JOIN (
                SELECT DISTINCT
                    TRIM(CONSM_SCORECRD_EXCLSN_F) AS CSEF_CONDITION_2,
                    TRIM(CLS_RSN_CD) AS CLS_RSN_CD,
                    TRIM(BLOCK_RECL_CD) AS BLOCK_RECL_CD
                FROM reference.BLOCK_RECL_CLS_RSN_LKP, ymt
                WHERE ymt.yrmth BETWEEN EFF_FROM_YR_MTH AND EFF_TO_YR_MTH
                  AND (TRIM(CLS_RSN_CD) <> '' OR TRIM(BLOCK_RECL_CD) <> '')
            ) cc2
                ON TRIM(a.BLOCK_RECL_CD) = cc2.BLOCK_RECL_CD
               AND TRIM(a.ACCT_CLS_RSN_CD) = cc2.CLS_RSN_CD
        ),
        derived AS (
            SELECT
                *,
                OLD_PIT_STAT_VER_2_CD AS PIT_STAT_VER_2_CD90,
                PIT_STAT_VER_2_CD_INTERIM AS PIT_STAT_VER_2_CD180,
                CASE
                    WHEN TRIM(BASEL_PRD_CD) = 'CC' AND HELOC_F = 'N'
                    THEN PIT_STAT_VER_2_CD_INTERIM
                    ELSE OLD_PIT_STAT_VER_2_CD
                END AS PIT_STAT_VER_2_CD,
                CASE
                    WHEN COALESCE(TRIM(EXCLUDED_TRNST_NUM), '') = '' THEN 'N'
                    ELSE 'Y'
                END AS TRNST_EXCLSN_F,
                CASE
                    WHEN STEP_PLN_SNAPSHOT_ID NOT IN (-1, -2) THEN 'Y'
                    WHEN STEP_PLN_SNAPSHOT_ID IN (-1, -2) AND TRIM(PRD_CD) IN ('SCL', 'VIC') THEN
                        CASE
                            WHEN TRIM(SCRD_TP_CD) = 'U' THEN 'U'
                            WHEN SUBSTR(TRIM(CRNT_BILL_CD), 1, 1) = 'U' THEN 'U'
                            WHEN SUBSTR(TRIM(CRNT_BILL_CD), 1, 2) IN ('11', 'SB', 'SN', 'SP', 'SR', 'ST') THEN 'R'
                            ELSE 'O'
                        END
                    WHEN STEP_PLN_SNAPSHOT_ID IN (-1, -2) AND TRIM(PRD_CD) NOT IN ('SCL', 'VIC') THEN 'N'
                END AS STEP_CD,
                CASE WHEN TRIM(SUB_PRD_CD) = 'RS' THEN 'Y' ELSE 'N' END AS RS_F,
                CASE
                    WHEN TRIM(CSEF_CONDITION_1) = 'Y' THEN 'Y'
                    WHEN TRIM(CSEF_CONDITION_2) = 'Y' THEN 'Y'
                    WHEN TRIM(CONSM_SCORECRD_EXCLSN_F3) = 'Y' THEN 'Y'
                    WHEN TOT_NEW_BAL_AMT <= 0 AND CR_LMT_AMT <= 0 THEN 'Y'
                    WHEN TOT_NEW_BAL_AMT <= 0
                         AND (SUBSTR(TRIM(BLOCK_RECL_CD), 1, 1) = 'V' OR TRIM(BLOCK_RECL_CD) = 'FX')
                    THEN 'Y'
                    WHEN (
                        CASE
                            WHEN TRIM(BASEL_PRD_CD) = 'CC' AND HELOC_F = 'N'
                            THEN PIT_STAT_VER_2_CD_INTERIM
                            ELSE OLD_PIT_STAT_VER_2_CD
                        END
                    ) = 'CHG'
                    THEN 'Y'
                    ELSE 'N'
                END AS CONSM_SCORECRD_EXCLSN_F,
                CASE
                    WHEN CR_LMT_AMT <= 0 AND TOT_NEW_BAL_AMT <= 0 THEN 'Z'
                    WHEN TOT_NEW_BAL_AMT <= 0
                         AND (SUBSTR(TRIM(BLOCK_RECL_CD), 1, 1) = 'V' OR TRIM(BLOCK_RECL_CD) = 'FX')
                    THEN 'Z'
                    ELSE CONSM_PRD_TREATMNT_CD
                END AS CONSM_PRD_TREATMNT_CD_FINAL
            FROM snap_cd
        ),
        with_pit_override AS (
            SELECT
                d.* EXCLUDE (PIT_STAT_VER_2_CD, CONSM_PRD_TREATMNT_CD),
                d.CONSM_PRD_TREATMNT_CD_FINAL AS CONSM_PRD_TREATMNT_CD,
                CASE
                    WHEN pit.CROSS_DFLT_PIT_OVERRIDE_F = 'Y'
                    THEN pit.CROSS_DFLT_PIT_STATUS
                    ELSE d.PIT_STAT_VER_2_CD
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
              AND TRIM(BASEL_PRD_CD) IN ('CC', 'LOC', 'SL A')
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
                 AND TRIM(BASEL_PRD_CD) IN ('CC', 'LOC', 'SL A')
            THEN CASE
                WHEN REVISED_EXPSR_AMT > (SELECT THRSHLD FROM thrshld) THEN 'Y'
                ELSE 'N'
            END
            WHEN CONSM_PRD_TREATMNT_CD = 'A'
                 AND (HELOC_F = 'Y' OR TRIM(BASEL_PRD_CD) IN ('SL', 'SL B'))
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
