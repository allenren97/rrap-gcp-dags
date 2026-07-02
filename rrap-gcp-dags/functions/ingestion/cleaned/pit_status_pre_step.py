"""
Rewrite of J_RRII_KS10_0000_PIT_STATUS_PRE_STEP_CROSS_DFLT.sas

Loads ingestion.PIT_STATUS_PRE_STEP for KS, SPL, and MOR and applies Step
cross-default overrides consumed by J_RRII_KS10_2103_BASEL_REVLVNG_CR_BASE_DRVD_VARS.
"""

UPSTREAM_ASSET = [
    "ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT",
    "ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT",
    "ingestion.AIRB_MORT_MTH_SNAPSHOT",
    "ingestion.BASEL_MORT_MTH_SNAPSHOT",
    "ingestion.TM_DIM",
    "reference.BLOCK_RECL_LKP",
    "reference.CHRG_OFF_LKP",
    "reference.SRC_PRD_LKP",
]

DOWNSTREAM_ASSET = "ingestion.PIT_STATUS_PRE_STEP"

DEPENDENCIES = {
    "duckdb_delete": ["duckdb_load_ks", "duckdb_load_spl", "duckdb_load_mor"],
    "duckdb_load_ks": ["duckdb_update_cross_default"],
    "duckdb_load_spl": ["duckdb_update_cross_default"],
    "duckdb_load_mor": ["duckdb_update_cross_default"],
}


def duckdb_delete(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    DELETE FROM {DOWNSTREAM_ASSET}
    WHERE MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    """,
):
    pass


def duckdb_load_ks(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} (
        MTH_TM_ID,
        SRC_SYS_CD,
        BASEL_ACCT_ID,
        STEP_PLN_SNAPSHOT_ID,
        STEP_PLN_AGRMNT_NUM,
        PIT_STATUS_PRE_STEP,
        STEP_DFLT_F,
        INSRT_PROCESS_TMSTMP,
        UPDT_PROCESS_TMSTMP
    )
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
                a.STEP_PLN_SNAPSHOT_ID,
                a.STEP_PLN_AGRMNT_NUM,
                a.PRD_CD,
                a.SUB_PRD_CD,
                a.BLOCK_RECL_CD,
                a.TOT_NEW_BAL_AMT,
                a.CHRG_OFF_CD,
                a.BNS_DLQNT_DAY,
                a.TOT_UNPAID_FNCL_CHRG_AMT,
                a.PRIM_BASEL_CUST_ID AS BASEL_CUST_ID,
                CASE
                    WHEN a.SUB_PRD_CD = 'RS'
                      OR a.STEP_PLN_SNAPSHOT_ID NOT IN (-1, -2)
                    THEN 'Y'
                    ELSE 'N'
                END AS HELOC_F,
                CASE
                    WHEN lk_recl.BLOCK_RECL_CD IS NULL THEN '1'
                    ELSE '0'
                END AS v_PT_STAT_BLCK_RECL_CD_LKP_CUR
            FROM ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT a
            CROSS JOIN ymt
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
        )
    SELECT
        s.MTH_TM_ID,
        'KS' AS SRC_SYS_CD,
        s.BASEL_ACCT_ID,
        s.STEP_PLN_SNAPSHOT_ID,
        s.STEP_PLN_AGRMNT_NUM,
        CASE
            WHEN sp.BASEL_PRD_CD = 'CC' AND s.HELOC_F = 'N' THEN
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
                END
        END AS PIT_STATUS_PRE_STEP,
        CASE
            WHEN TRIM(s.CHRG_OFF_CD) = '1' AND TRIM(s.BLOCK_RECL_CD) LIKE 'D%' THEN 'W'
            WHEN s.STEP_PLN_SNAPSHOT_ID > 0 THEN 'N'
        END AS STEP_DFLT_F,
        CURRENT_TIMESTAMP AS INSRT_PROCESS_TMSTMP,
        CURRENT_TIMESTAMP AS UPDT_PROCESS_TMSTMP
    FROM snap s
    LEFT JOIN prev_snap ps
        ON s.BASEL_ACCT_ID = ps.BASEL_ACCT_ID
       AND s.BASEL_CUST_ID = ps.BASEL_CUST_ID
    LEFT JOIN (
        SELECT DISTINCT
            TRIM(BASEL_PRD_CD) AS BASEL_PRD_CD,
            TRIM(SRC_PRD_CD) AS SRC_PRD_CD,
            TRIM(SRC_SUB_PRD_CD) AS SRC_SUB_PRD_CD
        FROM reference.SRC_PRD_LKP, ymt
        WHERE TRIM(PRD_SYS_CD) = 'KS'
          AND ymt.yrmth BETWEEN EFF_FROM_YR_MTH AND EFF_TO_YR_MTH
    ) sp
        ON s.PRD_CD = sp.SRC_PRD_CD
       AND s.SUB_PRD_CD = sp.SRC_SUB_PRD_CD
    """,
):
    pass


def duckdb_load_spl(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} (
        MTH_TM_ID,
        SRC_SYS_CD,
        BASEL_ACCT_ID,
        STEP_PLN_SNAPSHOT_ID,
        STEP_PLN_AGRMNT_NUM,
        PIT_STATUS_PRE_STEP,
        STEP_DFLT_F,
        INSRT_PROCESS_TMSTMP,
        UPDT_PROCESS_TMSTMP
    )
    SELECT
        MTH_TM_ID,
        'SPL' AS SRC_SYS_CD,
        BASEL_ACCT_ID,
        STEP_PLN_SNAPSHOT_ID,
        STEP_PLN_AGRMNT_NUM,
        CASE
            WHEN RECD_STAT_CD IN (9, 0) OR RECD_STAT_CD IS NULL THEN 'CLO'
            WHEN RECD_STAT_CD IN (6, 7, 8) THEN 'CHG'
            WHEN CHRG_OFF_DT IS NOT NULL THEN 'CHG'
            WHEN DAY_ODUE >= 90 OR RECD_STAT_CD = 5 THEN 'DEF'
            WHEN DAY_ODUE < 90 AND RECD_STAT_CD = 4 THEN 'CUR'
        END AS PIT_STATUS_PRE_STEP,
        CASE
            WHEN RECD_STAT_CD = 8 THEN 'W'
            WHEN STEP_PLN_SNAPSHOT_ID > 0 THEN 'N'
        END AS STEP_DFLT_F,
        CURRENT_TIMESTAMP AS INSRT_PROCESS_TMSTMP,
        CURRENT_TIMESTAMP AS UPDT_PROCESS_TMSTMP
    FROM ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT
    WHERE MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
      AND RECD_STAT_CD IN (4, 5, 6, 7, 8)
    """,
):
    pass


def duckdb_load_mor(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} (
        MTH_TM_ID,
        SRC_SYS_CD,
        BASEL_ACCT_ID,
        MORT_NUM,
        MORT_PROCESS_DATE,
        STEP_PLN_SNAPSHOT_ID,
        STEP_PLN_AGRMNT_NUM,
        PIT_STATUS_PRE_STEP,
        STEP_DFLT_F,
        INSRT_PROCESS_TMSTMP,
        UPDT_PROCESS_TMSTMP
    )
    SELECT
        a.TM_ID AS MTH_TM_ID,
        'MOR' AS SRC_SYS_CD,
        b.BASEL_ACCT_ID,
        a.MORT_NUM,
        a.MTH_END_DT AS MORT_PROCESS_DATE,
        b.STEP_PLN_SNAPSHOT_ID,
        b.STEP_PLN_AGRMNT_NUM,
        CASE
            WHEN UPPER(a.COMM_TP) = 'RESIDENTIAL'
                 AND a.PD_OFF_DT IS NULL
                 AND (
                     (a.DLQNT_DAY < 90 AND a.DLQNT_MTH < 4)
                     AND UPPER(COALESCE(a.FRCLSR_F, '')) <> 'Y'
                     AND a.CRNT_BAL <> 0
                     AND UPPER(COALESCE(a.LRA_STAT, '')) <> 'Y'
                 )
                 OR a.CRNT_BAL < 0
            THEN 'CUR'
            WHEN (
                UPPER(a.COMM_TP) = 'RESIDENTIAL'
                AND a.PD_OFF_DT IS NULL
                AND (
                    (a.DLQNT_DAY >= 90 OR a.DLQNT_MTH >= 4)
                    OR UPPER(a.FRCLSR_F) = 'Y'
                    OR UPPER(a.LRA_STAT) = 'Y'
                )
                AND a.CRNT_BAL > 0
            )
            OR (
                UPPER(a.COMM_TP) = 'RESIDENTIAL'
                AND UPPER(a.FRCLSR_F) = 'Y'
                AND UPPER(a.PD_OFF_F) = 'Y'
                AND GREATEST(a.CRNT_BAL, COALESCE(-a.TOT_SUSP_BAL, 0)) > 0
            )
            THEN 'DEF'
        END AS PIT_STATUS_PRE_STEP,
        CASE
            WHEN b.STEP_PLN_SNAPSHOT_ID > 0 THEN 'N'
        END AS STEP_DFLT_F,
        CURRENT_TIMESTAMP AS INSRT_PROCESS_TMSTMP,
        CURRENT_TIMESTAMP AS UPDT_PROCESS_TMSTMP
    FROM ingestion.AIRB_MORT_MTH_SNAPSHOT a
    LEFT JOIN ingestion.BASEL_MORT_MTH_SNAPSHOT b
        ON a.MORT_NUM = b.MORT_NUM
       AND a.TM_ID = b.MTH_TM_ID
    WHERE a.TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    """,
):
    pass


def duckdb_update_cross_default(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    UPDATE {DOWNSTREAM_ASSET}
    SET CROSS_DFLT_PIT_STATUS = PIT_STATUS_PRE_STEP
    WHERE MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
      AND PIT_STATUS_PRE_STEP IS NOT NULL;

    UPDATE {DOWNSTREAM_ASSET}
    SET
        STEP_DFLT_F = 'Y',
        CROSS_DFLT_PIT_OVERRIDE_F = CASE
            WHEN PIT_STATUS_PRE_STEP = 'CUR' THEN 'Y'
            ELSE 'N'
        END,
        CROSS_DFLT_PIT_STATUS = CASE
            WHEN PIT_STATUS_PRE_STEP = 'CUR' THEN 'DEF'
            ELSE PIT_STATUS_PRE_STEP
        END,
        UPDT_PROCESS_TMSTMP = CURRENT_TIMESTAMP
    WHERE MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
      AND COALESCE(STEP_DFLT_F, '') <> 'W'
      AND STEP_PLN_SNAPSHOT_ID IN (
          SELECT DISTINCT STEP_PLN_SNAPSHOT_ID
          FROM {DOWNSTREAM_ASSET}
          WHERE MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
            AND PIT_STATUS_PRE_STEP IN ('CHG', 'DEF')
            AND STEP_PLN_SNAPSHOT_ID > 0
            AND COALESCE(STEP_DFLT_F, '') <> 'W'
      );
    """,
):
    pass
