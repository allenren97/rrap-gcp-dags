"""
Rewrite of J_RRAP_TL10_2104_BASEL_PSNL_LOAN_ACCT_DRVD_VARS_2.sas only.

Builds emulated.BASEL_PSNL_LOAN_ACCT_DRVD_VARS_2 in a single DuckDB pipeline
(SPL PIT v2, exclusions, treatment, OS_BAL_AMT_V2) for the current process month.
"""

UPSTREAM_ASSET = [
    "ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT",
    "emulated.BASEL_PSNL_LOAN_ACCT_DRVD_VARS",
    "ingestion.BASEL_PSNL_LN_SUBV_MST_SNAPSHT_NEW",
    "ingestion.PIT_STATUS_PRE_STEP",
    "ingestion.TM_DIM",
    "reference.PSNL_LOAN_SCRTY_CD_PRPS_CD_LKP",
    "reference.PSNL_LOAN_SCRTY_CD_LKP",
]

DOWNSTREAM_ASSET = "emulated.BASEL_PSNL_LOAN_ACCT_DRVD_VARS_2"

DEPENDENCIES = {
    "duckdb_delete": ["duckdb_load"],
}


def duckdb_delete(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    DELETE FROM {DOWNSTREAM_ASSET}
    WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """,
):
    pass


def duckdb_load(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} BY NAME
    WITH
        mth_tm_id AS (
            SELECT {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}} AS val
        ),
        tm_month AS (
            SELECT TM_ID, TM_LVL_END_DT, STRFTIME(TM_LVL_END_DT, '%Y%m') AS perf_ym
            FROM ingestion.TM_DIM
            WHERE TM_ID = (SELECT val FROM mth_tm_id)
        ),
        subv_accts AS (
            SELECT DISTINCT BASEL_ACCT_ID
            FROM ingestion.BASEL_PSNL_LN_SUBV_MST_SNAPSHT_NEW
        ),
        base_inputs AS (
            SELECT
                m.MTH_TM_ID,
                m.BASEL_ACCT_ID,
                m.PRIM_BASEL_CUST_ID AS BASEL_CUST_ID,
                tm.TM_LVL_END_DT AS PROCESS_DT,
                tm.perf_ym,
                TRIM(m.CRNT_BR_LOCTN_TRNST) AS CAB,
                TRIM(m.LOAN_NUM) AS LOAN_NUM,
                TRY_CAST(TRIM(m.COMM_LOAN_CD) AS INTEGER) AS COMMERCIAL_LOAN_FLAG,
                d.PIT_STAT_VER_1_CD AS SELECT_STATUS_NEW,
                m.TOT_CRNT_BAL_AMT AS PRINCIPAL_BAL,
                m.ADD_ON_BAL_AMT AS ADDON_BAL,
                m.ACCR_INTR AS ACCR_INT,
                m.DAY_ODUE,
                m.RECD_STAT_CD,
                m.CHRG_OFF_DT,
                m.STEP_PLN_SNAPSHOT_ID,
                TRY_CAST(TRIM(m.PRPS_CD) AS INTEGER) AS NUM_PURPOSE_CODE,
                TRY_CAST(TRIM(m.SCRTY_CD) AS INTEGER) AS NUM_SCRTY_CD,
                pit.CROSS_DFLT_PIT_STATUS AS PIT_STATUS,
                CASE
                    WHEN s.BASEL_ACCT_ID IS NOT NULL THEN 'S10'
                    WHEN lkp1.PRD_ID IS NOT NULL THEN lkp1.PRD_ID
                    WHEN lkp2.PRD_ID IS NOT NULL THEN lkp2.PRD_ID
                    ELSE '-1'
                END AS PRD_ID,
                CASE
                    WHEN s.BASEL_ACCT_ID IS NOT NULL THEN 'INDIRECT'
                    WHEN lkp1.TP IS NOT NULL THEN lkp1.TP
                    WHEN lkp2.TP IS NOT NULL THEN lkp2.TP
                END AS PRD_TP,
                CASE
                    WHEN s.BASEL_ACCT_ID IS NOT NULL THEN 'Auto'
                    WHEN lkp1.PRD IS NOT NULL THEN lkp1.PRD
                    WHEN lkp2.PRD IS NOT NULL THEN lkp2.PRD
                END AS PRD,
                CASE
                    WHEN s.BASEL_ACCT_ID IS NOT NULL THEN 'Rate Subvented'
                    WHEN lkp1.SUB_PRD IS NOT NULL THEN lkp1.SUB_PRD
                    WHEN lkp2.SUB_PRD IS NOT NULL THEN lkp2.SUB_PRD
                END AS SUB_PRODUCT
            FROM ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT m
            CROSS JOIN tm_month tm
            LEFT JOIN emulated.BASEL_PSNL_LOAN_ACCT_DRVD_VARS d
                ON m.BASEL_ACCT_ID = d.BASEL_ACCT_ID
               AND m.MTH_TM_ID = d.MTH_TM_ID
            LEFT JOIN ingestion.PIT_STATUS_PRE_STEP pit
                ON m.MTH_TM_ID = pit.MTH_TM_ID
               AND m.BASEL_ACCT_ID = pit.BASEL_ACCT_ID
               AND pit.SRC_SYS_CD = 'SPL'
            LEFT JOIN subv_accts s ON m.BASEL_ACCT_ID = s.BASEL_ACCT_ID
            LEFT JOIN reference.PSNL_LOAN_SCRTY_CD_PRPS_CD_LKP lkp1
                ON TRIM(m.PRPS_CD) = lkp1.PRPS_CD
               AND TRIM(m.SCRTY_CD) = lkp1.SCRTY_CD
            LEFT JOIN reference.PSNL_LOAN_SCRTY_CD_LKP lkp2
                ON TRIM(m.SCRTY_CD) = lkp2.SCRTY_CD
            WHERE m.MTH_TM_ID = (SELECT val FROM mth_tm_id)
              AND m.RECD_STAT_CD IN ('4', '5', '6', '7', '8')
        ),
        product_enriched AS (
            SELECT
                *,
                CASE
                    WHEN UPPER(TRIM(PRD_TP)) = 'DIRECT' AND STEP_PLN_SNAPSHOT_ID <> -1
                    THEN 'S08'
                    ELSE PRD_ID
                END AS PRD_ID_FINAL,
                CASE
                    WHEN UPPER(TRIM(PRD_TP)) = 'DIRECT' AND STEP_PLN_SNAPSHOT_ID <> -1
                    THEN 'SPL under STEP'
                    ELSE PRD
                END AS PRD_FINAL,
                CASE
                    WHEN UPPER(TRIM(PRD_TP)) = 'DIRECT' AND STEP_PLN_SNAPSHOT_ID <> -1
                    THEN 'SPL under STEP'
                    ELSE SUB_PRODUCT
                END AS SUB_PRODUCT_FINAL
            FROM base_inputs
        ),
        comm_may2011 AS (
            SELECT DISTINCT
                TRIM(m.LOAN_NUM) AS LOAN_NUM,
                TRIM(m.CRNT_BR_LOCTN_TRNST) AS CAB,
                TRY_CAST(TRIM(m.COMM_LOAN_CD) AS INTEGER) AS COMMERCIAL_FLAG_V2
            FROM ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT m
            INNER JOIN ingestion.TM_DIM tm ON m.MTH_TM_ID = tm.TM_ID
            WHERE STRFTIME(tm.TM_LVL_END_DT, '%Y%m') = '201105'
        ),
        with_comm AS (
            SELECT
                p.*,
                COALESCE(
                    CASE
                        WHEN p.perf_ym IN ('201101', '201102', '201103', '201104')
                        THEN cf.COMMERCIAL_FLAG_V2
                        ELSE p.COMMERCIAL_LOAN_FLAG
                    END,
                    p.COMMERCIAL_LOAN_FLAG
                ) AS COMMERCIAL_FLAGV2
            FROM product_enriched p
            LEFT JOIN comm_may2011 cf
                ON p.LOAN_NUM = cf.LOAN_NUM
               AND p.CAB = cf.CAB
        ),
        prev_month AS (
            SELECT
                BASEL_ACCT_ID,
                PIT_STATUS_V2 AS PIT_STATUS_LAG,
                CONS_DFT_MTH_CNT AS CONS_MTHS_DEFAULT_LAG
            FROM {DOWNSTREAM_ASSET}
            WHERE MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="prev_mth_tm_id") }}}}
        ),
        with_cons_dft AS (
            SELECT
                w.*,
                CASE
                    WHEN (
                        UPPER(TRIM(pm.PIT_STATUS_LAG)) = 'CUR'
                        OR pm.PIT_STATUS_LAG IS NULL
                    )
                    AND UPPER(TRIM(w.PIT_STATUS)) IN ('DEF', 'CHG')
                    THEN 1
                    WHEN UPPER(TRIM(pm.PIT_STATUS_LAG)) IN ('DEF', 'CHG')
                         AND UPPER(TRIM(w.PIT_STATUS)) IN ('DEF', 'CHG')
                    THEN COALESCE(pm.CONS_MTHS_DEFAULT_LAG, 0) + 1
                    ELSE 0
                END AS CONS_DFT_MTH_CNT
            FROM with_comm w
            LEFT JOIN prev_month pm ON w.BASEL_ACCT_ID = pm.BASEL_ACCT_ID
        ),
        with_sub_port AS (
            SELECT
                *,
                CASE
                    WHEN PRD_ID_FINAL IN ('S01','S02','S03','S04','S05','S06','S07','S08')
                    THEN 'DIRECT'
                    WHEN PRD_ID_FINAL IN ('S09','S10','S11','S12','S13','S14','S15')
                    THEN 'INDIRECT'
                END AS SUB_PORTFL,
                PRINCIPAL_BAL + ADDON_BAL + ACCR_INT AS OS_BAL_AMT
            FROM with_cons_dft
        ),
        def_accts AS (
            SELECT DISTINCT LOAN_NUM, CAB
            FROM with_sub_port
            WHERE UPPER(TRIM(PIT_STATUS)) IN ('DEF', 'CHG')
        ),
        hist_snap AS (
            SELECT
                TRIM(m.LOAN_NUM) AS LOAN_NUM,
                TRIM(m.CRNT_BR_LOCTN_TRNST) AS CAB,
                m.BASEL_ACCT_ID,
                m.ACCR_INTR,
                tm.TM_LVL_END_DT
            FROM ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT m
            INNER JOIN ingestion.TM_DIM tm ON m.MTH_TM_ID = tm.TM_ID
            INNER JOIN def_accts d
                ON TRIM(m.LOAN_NUM) = d.LOAN_NUM
               AND TRIM(m.CRNT_BR_LOCTN_TRNST) = d.CAB
            WHERE m.MTH_TM_ID <= (SELECT val FROM mth_tm_id)
              AND m.RECD_STAT_CD IN ('4', '5', '6', '7', '8')
        ),
        hist_pit AS (
            SELECT
                BASEL_ACCT_ID,
                MTH_TM_ID,
                CROSS_DFLT_PIT_STATUS AS PIT_STATUS
            FROM ingestion.PIT_STATUS_PRE_STEP
            WHERE SRC_SYS_CD = 'SPL'
              AND MTH_TM_ID <= (SELECT val FROM mth_tm_id)
        ),
        hist_joined AS (
            SELECT
                h.LOAN_NUM,
                h.CAB,
                h.TM_LVL_END_DT,
                p.PIT_STATUS,
                h.ACCR_INTR
            FROM hist_snap h
            LEFT JOIN hist_pit p
                ON h.BASEL_ACCT_ID = p.BASEL_ACCT_ID
               AND h.TM_LVL_END_DT = (
                   SELECT TM_LVL_END_DT FROM ingestion.TM_DIM WHERE TM_ID = p.MTH_TM_ID
               )
            UNION ALL
            SELECT LOAN_NUM, CAB, PROCESS_DT, PIT_STATUS, ACCR_INT
            FROM with_sub_port
        ),
        ranked_hist AS (
            SELECT
                *,
                LAG(PIT_STATUS) OVER (
                    PARTITION BY LOAN_NUM, CAB ORDER BY TM_LVL_END_DT
                ) AS LAG_PIT,
                LAG(ACCR_INTR) OVER (
                    PARTITION BY LOAN_NUM, CAB ORDER BY TM_LVL_END_DT
                ) AS LAG_ACCR_INTR,
                ROW_NUMBER() OVER (
                    PARTITION BY LOAN_NUM, CAB ORDER BY TM_LVL_END_DT DESC
                ) AS RN_DESC,
                MIN(TM_LVL_END_DT) OVER (PARTITION BY LOAN_NUM, CAB) AS EARLIEST_DT
            FROM hist_joined
        ),
        int_at_default AS (
            SELECT
                LOAN_NUM,
                CAB,
                CASE
                    WHEN TRIM(LAG_PIT) = 'DEF' AND TRIM(PIT_STATUS) = 'CUR'
                    THEN LAG_ACCR_INTR
                    WHEN TM_LVL_END_DT = EARLIEST_DT
                         AND UPPER(TRIM(PIT_STATUS)) = 'DEF'
                    THEN ACCR_INTR
                END AS INT_AT_DEFAULT
            FROM ranked_hist
            WHERE (
                TRIM(LAG_PIT) = 'DEF' AND TRIM(PIT_STATUS) = 'CUR'
            ) OR (
                TM_LVL_END_DT = EARLIEST_DT AND UPPER(TRIM(PIT_STATUS)) = 'DEF'
            )
        ),
        int_at_default_recent AS (
            SELECT LOAN_NUM, CAB, MAX(INT_AT_DEFAULT) AS INT_AT_DEFAULT
            FROM int_at_default
            WHERE INT_AT_DEFAULT IS NOT NULL
            GROUP BY LOAN_NUM, CAB
        ),
        with_os_bal_v2 AS (
            SELECT
                w.*,
                CASE
                    WHEN UPPER(TRIM(w.PIT_STATUS)) IN ('DEF', 'CHG')
                    THEN w.PRINCIPAL_BAL + w.ADDON_BAL + COALESCE(i.INT_AT_DEFAULT, w.ACCR_INT)
                    WHEN UPPER(TRIM(w.PIT_STATUS)) IN ('CUR', 'CLO')
                    THEN w.PRINCIPAL_BAL + w.ADDON_BAL + w.ACCR_INT
                END AS OS_BAL_AMT_V2
            FROM with_sub_port w
            LEFT JOIN int_at_default_recent i
                ON w.LOAN_NUM = i.LOAN_NUM
               AND w.CAB = i.CAB
        ),
        with_excl AS (
            SELECT
                *,
                CASE
                    WHEN (
                        UPPER(TRIM(PIT_STATUS)) = 'CUR'
                        AND (OS_BAL_AMT < 100 OR PRINCIPAL_BAL <= 0)
                    )
                    OR (
                        UPPER(TRIM(PIT_STATUS)) = 'DEF'
                        AND (PRINCIPAL_BAL <= 0 OR OS_BAL_AMT < 1)
                    )
                    THEN 1 ELSE 0
                END AS IND_BAL_EXCL,
                CASE WHEN PRD_ID_FINAL = 'S11' THEN 1 ELSE 0 END AS IND_HL_EXCL,
                CASE WHEN PRD_ID_FINAL = 'S14' THEN 1 ELSE 0 END AS IND_OTHIND_EXCL,
                CASE WHEN COMMERCIAL_FLAGV2 IN (1, 2) THEN 1 ELSE 0 END AS IND_COM_EXCL,
                CASE
                    WHEN TRY_CAST(CAB AS INTEGER) IN (18192, 99432) THEN 1
                    ELSE 0
                END AS IND_CAB_EXCL,
                CASE WHEN CONS_DFT_MTH_CNT > 24 THEN 1 ELSE 0 END AS IND_24MOS_EXCL
            FROM with_os_bal_v2
        )
    SELECT
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        BASEL_ACCT_ID,
        MTH_TM_ID,
        PIT_STATUS AS PIT_STATUS_V2,
        PROCESS_DT,
        LOAN_NUM,
        CAB,
        SUB_PORTFL,
        COMMERCIAL_FLAGV2 AS COMM_F_V2,
        IND_BAL_EXCL,
        IND_HL_EXCL,
        IND_CAB_EXCL,
        IND_24MOS_EXCL,
        CASE
            WHEN IND_BAL_EXCL = 1 OR IND_HL_EXCL = 1 OR IND_OTHIND_EXCL = 1
                 OR IND_COM_EXCL = 1 OR IND_CAB_EXCL = 1 OR IND_24MOS_EXCL = 1
            THEN 'Y'
            ELSE 'N'
        END AS MODEL_EXCL_F,
        CASE
            WHEN IND_COM_EXCL = 1 OR IND_CAB_EXCL = 1 THEN 'Z'
            ELSE 'A'
        END AS TREATMNT_F,
        BASEL_CUST_ID,
        PRD_TP,
        PRD_FINAL AS PRD,
        CONS_DFT_MTH_CNT,
        SUB_PRODUCT_FINAL AS SUB_PRODUCT,
        PRD_ID_FINAL AS PRD_ID,
        CURRENT_TIMESTAMP AS INSRT_PROCESS_TMSTMP,
        CURRENT_TIMESTAMP AS UPDT_PROCESS_TMSTMP,
        OS_BAL_AMT_V2
    FROM with_excl
    """,
):
    pass
