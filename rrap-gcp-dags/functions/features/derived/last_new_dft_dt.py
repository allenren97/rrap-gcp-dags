from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)


UPSTREAM_ASSET = [
    "ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT",
    "ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT",
    "ingestion.TM_DIM",
    "features.PIT_STATUS_CROSS_DEFAULT_ORIG",
    "features.TREATMENT_F",
    "features.SUB_PORT_F",
    "features.BASEL_PRD_CD",
    "features.HELOC_F",
    "features.ACCRL_STAT_F",
    "features.CONSM_PRD_TREATMNT_CD",
    "features.SML_BUS_F",
    "features.TRNST_EXCLSN_F",
]
DOWNSTREAM_ASSET = "features.LAST_NEW_DFT_DT"
DEPENDENCIES = {
    "duckdb_delete": ["export_spl", "export_ks"],
    "export_spl": ["duckdb_load"],
    "export_ks": ["duckdb_load"],
}


def duckdb_delete(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    delete from {DOWNSTREAM_ASSET} where obsn_dt = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """,
):
    pass


def export_spl(
    duckdb_conn_id="duckdb-conn",
    resource_tier="HIGH",
    pool_slots=96,
    sql=f"""
    WITH
        mth_tm_id AS (
            SELECT {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}} AS val
        ),
        process_tm AS (
            SELECT TM_ID, TM_LVL_END_DT
            FROM ingestion.TM_DIM
            WHERE TM_ID = (SELECT val FROM mth_tm_id)
        ),
        lgdd_obs AS (
            SELECT
                p.TM_ID AS PROCESS_MTH_TM_ID,
                p.TM_LVL_END_DT AS PROCESS_DT,
                LAST_DAY((p.TM_LVL_END_DT - INTERVAL 24 MONTH)::DATE) AS OBS_END_DT,
                LAST_DAY((p.TM_LVL_END_DT - INTERVAL 48 MONTH)::DATE) AS OBS_START_DT
            FROM process_tm p
        ),
        lgdd_obs_tm AS (
            SELECT
                o.PROCESS_MTH_TM_ID,
                o.PROCESS_DT,
                end_tm.TM_ID AS OBS_END_TM_ID,
                end_tm.TM_LVL_END_DT AS OBS_END_DT,
                start_tm.TM_ID AS OBS_START_TM_ID,
                start_tm.TM_LVL_END_DT AS OBS_START_DT
            FROM lgdd_obs o
            INNER JOIN ingestion.TM_DIM end_tm
                ON TRIM(end_tm.TM_LVL) = 'Month'
               AND end_tm.TM_LVL_END_DT = o.OBS_END_DT
            INNER JOIN ingestion.TM_DIM start_tm
                ON TRIM(start_tm.TM_LVL) = 'Month'
               AND start_tm.TM_LVL_END_DT = o.OBS_START_DT
        ),
        lgdnd_obs AS (
            SELECT
                p.TM_ID AS PROCESS_MTH_TM_ID,
                p.TM_LVL_END_DT AS PROCESS_DT,
                LAST_DAY(p.TM_LVL_END_DT::DATE) AS OBS_END_DT,
                LAST_DAY((p.TM_LVL_END_DT - INTERVAL 12 MONTH)::DATE) AS OBS_START_DT
            FROM process_tm p
        ),
        lgdnd_obs_tm AS (
            SELECT
                o.PROCESS_MTH_TM_ID,
                o.PROCESS_DT,
                end_tm.TM_ID AS OBS_END_TM_ID,
                end_tm.TM_LVL_END_DT AS OBS_END_DT,
                start_tm.TM_ID AS OBS_START_TM_ID,
                start_tm.TM_LVL_END_DT AS OBS_START_DT
            FROM lgdnd_obs o
            INNER JOIN ingestion.TM_DIM end_tm
                ON TRIM(end_tm.TM_LVL) = 'Month'
               AND end_tm.TM_LVL_END_DT = o.OBS_END_DT
            INNER JOIN ingestion.TM_DIM start_tm
                ON TRIM(start_tm.TM_LVL) = 'Month'
               AND start_tm.TM_LVL_END_DT = o.OBS_START_DT
        ),
        spl_acct_feats AS MATERIALIZED (
            SELECT
                snp.MTH_TM_ID,
                snp.BASEL_ACCT_ID,
                tm.TM_LVL_END_DT AS PROCESS_DT,
                TRIM(snp.CRNT_BR_LOCTN_TRNST) AS CAB,
                TRIM(snp.LOAN_NUM) AS LOAN_NUM,
                pit.PIT_STATUS_CROSS_DEFAULT_ORIG AS PIT_STATUS_V2,
                trt.TREATMENT_F AS TREATMNT_F,
                sub.SUB_PORT_F AS SUB_PORTFL
            FROM ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT snp
            INNER JOIN ingestion.TM_DIM tm
                ON snp.MTH_TM_ID = tm.TM_ID
               AND TRIM(tm.TM_LVL) = 'Month'
            CROSS JOIN lgdd_obs_tm lgdd
            CROSS JOIN lgdnd_obs_tm lgdnd
            LEFT JOIN (
                SELECT BASEL_ACCT_ID, OBSN_DT, PIT_STATUS_CROSS_DEFAULT_ORIG
                FROM features.PIT_STATUS_CROSS_DEFAULT_ORIG
                WHERE SRC_SYS_CD = 'SPL'
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY BASEL_ACCT_ID, OBSN_DT
                    ORDER BY PIT_STATUS_CROSS_DEFAULT_ORIG DESC NULLS LAST
                ) = 1
            ) pit
                ON snp.BASEL_ACCT_ID = pit.BASEL_ACCT_ID
               AND pit.OBSN_DT = tm.TM_LVL_END_DT
            LEFT JOIN (
                SELECT BASEL_ACCT_ID, OBSN_DT, TREATMENT_F
                FROM features.TREATMENT_F
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY BASEL_ACCT_ID, OBSN_DT
                    ORDER BY TREATMENT_F DESC NULLS LAST
                ) = 1
            ) trt
                ON snp.BASEL_ACCT_ID = trt.BASEL_ACCT_ID
               AND trt.OBSN_DT = tm.TM_LVL_END_DT
            LEFT JOIN (
                SELECT BASEL_ACCT_ID, OBSN_DT, SUB_PORT_F
                FROM features.SUB_PORT_F
                WHERE SRC_SYS_CD = 'SPL'
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY BASEL_ACCT_ID, OBSN_DT
                    ORDER BY SUB_PORT_F DESC NULLS LAST
                ) = 1
            ) sub
                ON snp.BASEL_ACCT_ID = sub.BASEL_ACCT_ID
               AND sub.OBSN_DT = tm.TM_LVL_END_DT
            WHERE snp.RECD_STAT_CD IN ('4', '5', '6', '7', '8')
              AND snp.MTH_TM_ID BETWEEN lgdd.OBS_START_TM_ID AND lgdnd.OBS_END_TM_ID
        ),
        max_non_def AS (
            SELECT
                d.BASEL_ACCT_ID,
                MAX(d.MTH_TM_ID) AS MAX_NON_DEF_TM_ID,
                MAX(d.PROCESS_DT) AS MAX_NON_DEF_DT
            FROM spl_acct_feats d
            CROSS JOIN lgdd_obs_tm o
            WHERE UPPER(TRIM(d.PIT_STATUS_V2)) = 'CUR'
              AND d.MTH_TM_ID BETWEEN o.OBS_START_TM_ID AND o.OBS_END_TM_ID
            GROUP BY d.BASEL_ACCT_ID
        ),
        def_after_cur AS (
            SELECT
                d.BASEL_ACCT_ID,
                MIN(d.MTH_TM_ID) AS LAST_NEW_DFT_MTH_TM_ID,
                MIN(d.PROCESS_DT) AS LAST_NEW_DFT_DT
            FROM spl_acct_feats d
            INNER JOIN max_non_def m ON d.BASEL_ACCT_ID = m.BASEL_ACCT_ID
            CROSS JOIN lgdd_obs_tm o
            WHERE UPPER(TRIM(d.PIT_STATUS_V2)) = 'DEF'
              AND d.MTH_TM_ID > m.MAX_NON_DEF_TM_ID
              AND d.MTH_TM_ID <= o.OBS_END_TM_ID
            GROUP BY d.BASEL_ACCT_ID
        ),
        pop_in_window AS (
            SELECT DISTINCT d.BASEL_ACCT_ID
            FROM spl_acct_feats d
            CROSS JOIN lgdd_obs_tm o
            WHERE d.MTH_TM_ID BETWEEN o.OBS_START_TM_ID AND o.OBS_END_TM_ID
        ),
        never_cur AS (
            SELECT p.BASEL_ACCT_ID
            FROM pop_in_window p
            WHERE p.BASEL_ACCT_ID NOT IN (SELECT BASEL_ACCT_ID FROM max_non_def)
        ),
        def_never_cur AS (
            SELECT
                d.BASEL_ACCT_ID,
                MIN(d.MTH_TM_ID) AS LAST_NEW_DFT_MTH_TM_ID,
                MIN(d.PROCESS_DT) AS LAST_NEW_DFT_DT
            FROM spl_acct_feats d
            INNER JOIN never_cur n ON d.BASEL_ACCT_ID = n.BASEL_ACCT_ID
            CROSS JOIN lgdd_obs_tm o
            WHERE UPPER(TRIM(d.PIT_STATUS_V2)) = 'DEF'
              AND d.MTH_TM_ID BETWEEN o.OBS_START_TM_ID AND o.OBS_END_TM_ID
            GROUP BY d.BASEL_ACCT_ID
        ),
        last_new_def AS (
            SELECT * FROM def_after_cur
            UNION ALL
            SELECT * FROM def_never_cur
        ),
        os_bal_at_def AS (
            SELECT
                l.BASEL_ACCT_ID,
                l.LAST_NEW_DFT_MTH_TM_ID,
                l.LAST_NEW_DFT_DT,
                ROUND(s.TOT_CRNT_BAL_AMT + s.ADD_ON_BAL_AMT + s.ACCR_INTR, 3) AS LAST_NEW_DFT_BAL_AMT
            FROM last_new_def l
            INNER JOIN ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT s
                ON l.BASEL_ACCT_ID = s.BASEL_ACCT_ID
               AND l.LAST_NEW_DFT_MTH_TM_ID = s.MTH_TM_ID
        ),
        lgdd_rows AS (
            SELECT
                a.BASEL_ACCT_ID,
                o.OBS_END_TM_ID AS OBSVTN_MTH_TM_ID,
                pf.LAST_NEW_DFT_DT,
                pf.LAST_NEW_DFT_BAL_AMT,
                '' AS MODEL_DFT_F
            FROM lgdd_obs_tm o
            INNER JOIN ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT a
                ON a.MTH_TM_ID = o.OBS_END_TM_ID
               AND a.RECD_STAT_CD IN ('4', '5', '6', '7', '8')
            INNER JOIN spl_acct_feats c
                ON a.BASEL_ACCT_ID = c.BASEL_ACCT_ID
               AND a.MTH_TM_ID = c.MTH_TM_ID
               AND UPPER(TRIM(c.PIT_STATUS_V2)) = 'DEF'
               AND c.TREATMNT_F = 'A'
            INNER JOIN os_bal_at_def pf ON a.BASEL_ACCT_ID = pf.BASEL_ACCT_ID
        ),
        filter_account_raw AS (
            SELECT
                b.BASEL_ACCT_ID,
                b.MTH_TM_ID,
                ROUND(a.TOT_CRNT_BAL_AMT + a.ADD_ON_BAL_AMT + a.ACCR_INTR, 3) AS OS_BAL_AMT_V2,
                a.TOT_CRNT_BAL_AMT,
                ROW_NUMBER() OVER (
                    PARTITION BY b.BASEL_ACCT_ID
                    ORDER BY b.MTH_TM_ID
                ) AS RN
            FROM ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT a
            INNER JOIN spl_acct_feats b
                ON a.MTH_TM_ID = b.MTH_TM_ID
               AND a.BASEL_ACCT_ID = b.BASEL_ACCT_ID
            CROSS JOIN lgdnd_obs_tm o
            WHERE b.MTH_TM_ID BETWEEN o.OBS_START_TM_ID AND o.OBS_END_TM_ID
              AND UPPER(TRIM(b.PIT_STATUS_V2)) IN ('DEF', 'CHG')
        ),
        filter_account AS (
            SELECT BASEL_ACCT_ID
            FROM filter_account_raw
            WHERE RN = 1
              AND OS_BAL_AMT_V2 >= 1
              AND TOT_CRNT_BAL_AMT > 0
        ),
        lgdnd_last_def AS (
            SELECT
                d.BASEL_ACCT_ID,
                MIN(d.MTH_TM_ID) AS LAST_NEW_DFT_MTH_TM_ID,
                MIN(d.PROCESS_DT) AS LAST_NEW_DFT_DT
            FROM spl_acct_feats d
            CROSS JOIN lgdnd_obs_tm o
            WHERE UPPER(TRIM(d.PIT_STATUS_V2)) IN ('DEF', 'CHG')
              AND d.MTH_TM_ID BETWEEN o.OBS_START_TM_ID AND o.OBS_END_TM_ID
            GROUP BY d.BASEL_ACCT_ID
        ),
        lgdnd_os_bal AS (
            SELECT
                l.BASEL_ACCT_ID,
                l.LAST_NEW_DFT_MTH_TM_ID,
                l.LAST_NEW_DFT_DT,
                ROUND(s.TOT_CRNT_BAL_AMT + s.ADD_ON_BAL_AMT + s.ACCR_INTR, 3) AS LAST_NEW_DFT_BAL_AMT
            FROM lgdnd_last_def l
            INNER JOIN ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT s
                ON l.BASEL_ACCT_ID = s.BASEL_ACCT_ID
               AND l.LAST_NEW_DFT_MTH_TM_ID = s.MTH_TM_ID
        ),
        lgdnd_rows AS (
            SELECT
                a.BASEL_ACCT_ID,
                o.OBS_START_TM_ID AS OBSVTN_MTH_TM_ID,
                pf.LAST_NEW_DFT_DT,
                pf.LAST_NEW_DFT_BAL_AMT,
                CASE WHEN pf.LAST_NEW_DFT_DT IS NULL THEN 'N' ELSE 'Y' END AS MODEL_DFT_F
            FROM lgdnd_obs_tm o
            INNER JOIN ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT a
                ON a.MTH_TM_ID = o.OBS_START_TM_ID
               AND a.RECD_STAT_CD IN ('4', '5', '6', '7', '8')
            INNER JOIN spl_acct_feats c
                ON a.BASEL_ACCT_ID = c.BASEL_ACCT_ID
               AND a.MTH_TM_ID = c.MTH_TM_ID
               AND UPPER(TRIM(c.PIT_STATUS_V2)) = 'CUR'
               AND c.TREATMNT_F = 'A'
            INNER JOIN filter_account fa ON a.BASEL_ACCT_ID = fa.BASEL_ACCT_ID
            LEFT JOIN lgdnd_os_bal pf ON a.BASEL_ACCT_ID = pf.BASEL_ACCT_ID
        ),
        combined AS (
            SELECT * FROM lgdd_rows
            UNION ALL
            SELECT * FROM lgdnd_rows
        )
    SELECT
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        BASEL_ACCT_ID,
        OBSVTN_MTH_TM_ID,
        LAST_NEW_DFT_DT,
        'SPL' AS SRC_SYS_CD
    FROM combined
    """,
):
    pass


def export_ks(
    duckdb_conn_id="duckdb-conn",
    resource_tier="HIGH",
    pool_slots=96,
    sql=f"""
    WITH
        mth_tm_id AS (
            SELECT {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}} AS val
        ),
        dataprep_full AS MATERIALIZED (
            SELECT
                snp.BASEL_ACCT_ID,
                'KS' AS SRC_SYS_CD,
                snp.MTH_TM_ID,
                snp.TOT_NEW_BAL_AMT AS OS_BAL_AMT,
                snp.TOT_UNPAID_FNCL_CHRG_AMT,
                accrl.ACCRL_STAT_F,
                pit.PIT_STATUS_CROSS_DEFAULT_ORIG AS PIT_STATUS_CD,
                prd.BASEL_PRD_CD,
                CAST(NULL AS DOUBLE) AS TOT_CRNT_BAL_AMT,
                trt.CONSM_PRD_TREATMNT_CD,
                sml.SML_BUS_F,
                trnst.TRNST_EXCLSN_F,
                heloc.HELOC_F
            FROM ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT snp
            INNER JOIN ingestion.TM_DIM tm
                ON snp.MTH_TM_ID = tm.TM_ID
               AND TRIM(tm.TM_LVL) = 'Month'
            CROSS JOIN mth_tm_id
            LEFT JOIN (
                SELECT BASEL_ACCT_ID, OBSN_DT, BASEL_PRD_CD
                FROM features.BASEL_PRD_CD
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY BASEL_ACCT_ID, OBSN_DT ORDER BY BASEL_PRD_CD DESC NULLS LAST
                ) = 1
            ) prd
                ON snp.BASEL_ACCT_ID = prd.BASEL_ACCT_ID
               AND prd.OBSN_DT = tm.TM_LVL_END_DT
            LEFT JOIN (
                SELECT BASEL_ACCT_ID, OBSN_DT, PIT_STATUS_CROSS_DEFAULT_ORIG
                FROM features.PIT_STATUS_CROSS_DEFAULT_ORIG
                WHERE SRC_SYS_CD = 'KS'
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY BASEL_ACCT_ID, OBSN_DT ORDER BY PIT_STATUS_CROSS_DEFAULT_ORIG DESC NULLS LAST
                ) = 1
            ) pit
                ON snp.BASEL_ACCT_ID = pit.BASEL_ACCT_ID
               AND pit.OBSN_DT = tm.TM_LVL_END_DT
            LEFT JOIN (
                SELECT BASEL_ACCT_ID, OBSN_DT, HELOC_F
                FROM features.HELOC_F
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY BASEL_ACCT_ID, OBSN_DT ORDER BY HELOC_F DESC NULLS LAST
                ) = 1
            ) heloc
                ON snp.BASEL_ACCT_ID = heloc.BASEL_ACCT_ID
               AND heloc.OBSN_DT = tm.TM_LVL_END_DT
            LEFT JOIN (
                SELECT BASEL_ACCT_ID, OBSN_DT, ACCRL_STAT_F
                FROM features.ACCRL_STAT_F
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY BASEL_ACCT_ID, OBSN_DT ORDER BY ACCRL_STAT_F DESC NULLS LAST
                ) = 1
            ) accrl
                ON snp.BASEL_ACCT_ID = accrl.BASEL_ACCT_ID
               AND accrl.OBSN_DT = tm.TM_LVL_END_DT
            LEFT JOIN (
                SELECT BASEL_ACCT_ID, OBSN_DT, CONSM_PRD_TREATMNT_CD
                FROM features.CONSM_PRD_TREATMNT_CD
                WHERE SRC_SYS_CD = 'KS'
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY BASEL_ACCT_ID, OBSN_DT ORDER BY CONSM_PRD_TREATMNT_CD DESC NULLS LAST
                ) = 1
            ) trt
                ON snp.BASEL_ACCT_ID = trt.BASEL_ACCT_ID
               AND trt.OBSN_DT = tm.TM_LVL_END_DT
            LEFT JOIN (
                SELECT BASEL_ACCT_ID, OBSN_DT, SML_BUS_F
                FROM features.SML_BUS_F
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY BASEL_ACCT_ID, OBSN_DT ORDER BY SML_BUS_F DESC NULLS LAST
                ) = 1
            ) sml
                ON snp.BASEL_ACCT_ID = sml.BASEL_ACCT_ID
               AND sml.OBSN_DT = tm.TM_LVL_END_DT
            LEFT JOIN (
                SELECT BASEL_ACCT_ID, OBSN_DT, TRNST_EXCLSN_F
                FROM features.TRNST_EXCLSN_F
                WHERE SRC_SYS_CD = 'KS'
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY BASEL_ACCT_ID, OBSN_DT ORDER BY TRNST_EXCLSN_F DESC NULLS LAST
                ) = 1
            ) trnst
                ON snp.BASEL_ACCT_ID = trnst.BASEL_ACCT_ID
               AND trnst.OBSN_DT = tm.TM_LVL_END_DT
            WHERE snp.MTH_TM_ID >= mth_tm_id.val - 48 * 40
              AND snp.MTH_TM_ID <= mth_tm_id.val
        ),
        lagged_pdead AS (
            SELECT
                *,
                LAG(OS_BAL_AMT) OVER (PARTITION BY BASEL_ACCT_ID ORDER BY MTH_TM_ID) AS LAG_OS_BAL_AMT,
                LAG(TOT_UNPAID_FNCL_CHRG_AMT) OVER (PARTITION BY BASEL_ACCT_ID ORDER BY MTH_TM_ID) AS LAG_TOT_UNPAID_FNCL_CHRG_AMT,
                LAG(PIT_STATUS_CD) OVER (PARTITION BY BASEL_ACCT_ID ORDER BY MTH_TM_ID) AS LAG_PIT_STATUS_CD
            FROM dataprep_full
            CROSS JOIN mth_tm_id
            WHERE MTH_TM_ID >= mth_tm_id.val - 12 * 40
              AND MTH_TM_ID <= mth_tm_id.val
        ),
        new_defaults_pdead AS (
            SELECT BASEL_ACCT_ID, MTH_TM_ID
            FROM lagged_pdead
            WHERE CASE
                WHEN SRC_SYS_CD IN ('SPL')
                 AND PIT_STATUS_CD IN ('DEF', 'CHG')
                 AND OS_BAL_AMT >= 1
                 AND TOT_CRNT_BAL_AMT > 0
                 AND COALESCE(LAG_PIT_STATUS_CD, 'CUR') NOT IN ('DEF', 'CHG')
                THEN 1
                WHEN SRC_SYS_CD IN ('TNG-MOR', 'MOR')
                 AND PIT_STATUS_CD IN ('DEF')
                 AND LAG_PIT_STATUS_CD = 'CUR'
                THEN 1
                WHEN SRC_SYS_CD IN ('KS')
                 AND BASEL_PRD_CD = 'CC'
                 AND HELOC_F = 'N'
                 AND PIT_STATUS_CD IN ('DEF', 'CHG')
                 AND LAG_PIT_STATUS_CD = 'CUR'
                 AND OS_BAL_AMT > 0
                 AND LAG_TOT_UNPAID_FNCL_CHRG_AMT <> LAG_OS_BAL_AMT
                THEN 1
                WHEN SRC_SYS_CD IN ('KS')
                 AND BASEL_PRD_CD = 'CC'
                 AND HELOC_F = 'N'
                 AND PIT_STATUS_CD IN ('DEF', 'CHG')
                 AND LAG_PIT_STATUS_CD = 'CUR'
                 AND OS_BAL_AMT > 0
                 AND LAG_TOT_UNPAID_FNCL_CHRG_AMT = LAG_OS_BAL_AMT
                 AND LAG_TOT_UNPAID_FNCL_CHRG_AMT <= 0
                THEN 1
                WHEN SRC_SYS_CD IN ('KS')
                 AND BASEL_PRD_CD = 'CC'
                 AND HELOC_F = 'N'
                 AND PIT_STATUS_CD IN ('DEF', 'CHG')
                 AND LAG_PIT_STATUS_CD = 'CUR'
                 AND OS_BAL_AMT > 0
                 AND LAG_TOT_UNPAID_FNCL_CHRG_AMT = LAG_OS_BAL_AMT
                 AND LAG_TOT_UNPAID_FNCL_CHRG_AMT > 0
                 AND TOT_UNPAID_FNCL_CHRG_AMT <> OS_BAL_AMT
                 AND PIT_STATUS_CD <> 'CHG'
                 AND OS_BAL_AMT >= 5
                THEN 1
                WHEN SRC_SYS_CD IN ('KS')
                 AND BASEL_PRD_CD = 'CC'
                 AND HELOC_F = 'N'
                 AND PIT_STATUS_CD IN ('DEF', 'CHG')
                 AND LAG_PIT_STATUS_CD = 'CUR'
                THEN 0
                WHEN SRC_SYS_CD IN ('KS')
                 AND BASEL_PRD_CD = 'CC'
                 AND HELOC_F = 'Y'
                 AND PIT_STATUS_CD IN ('DEF', 'CHG')
                 AND LAG_PIT_STATUS_CD = 'CUR'
                 AND LAG_TOT_UNPAID_FNCL_CHRG_AMT <> LAG_OS_BAL_AMT
                 AND OS_BAL_AMT > 0
                THEN 1
                WHEN SRC_SYS_CD IN ('KS')
                 AND BASEL_PRD_CD <> 'CC'
                 AND PIT_STATUS_CD IN ('DEF', 'CHG')
                 AND LAG_PIT_STATUS_CD = 'CUR'
                 AND LAG_TOT_UNPAID_FNCL_CHRG_AMT <> LAG_OS_BAL_AMT
                 AND OS_BAL_AMT > 0
                THEN 1
                ELSE 0
            END = 1
        ),
        defaults_pdead AS (
            SELECT BASEL_ACCT_ID, MAX(MTH_TM_ID) AS LAST_NEW_DEFAULT_DATE
            FROM new_defaults_pdead
            GROUP BY BASEL_ACCT_ID
        ),
        pdead AS (
            SELECT
                d.BASEL_ACCT_ID,
                1 AS MODEL_DFT_F,
                d.LAST_NEW_DEFAULT_DATE,
                CASE
                    WHEN snp_def.SRC_SYS_CD IN ('KS')
                     AND (snp_def.PIT_STATUS_CD = 'CHG' OR snp_def.ACCRL_STAT_F = 'N')
                     AND snp_def.OS_BAL_AMT = 0
                    THEN GREATEST(COALESCE(snp_def_ks_lag.OS_BAL_AMT, 0), 0)
                    ELSE GREATEST(COALESCE(snp_def.OS_BAL_AMT, 0), 0)
                END AS LAST_NEW_DEFAULT_OS_BAL_AMT
            FROM defaults_pdead d
            LEFT JOIN dataprep_full snp_def
                ON snp_def.BASEL_ACCT_ID = d.BASEL_ACCT_ID
               AND snp_def.MTH_TM_ID = d.LAST_NEW_DEFAULT_DATE
            LEFT JOIN dataprep_full snp_def_ks_lag
                ON snp_def_ks_lag.BASEL_ACCT_ID = d.BASEL_ACCT_ID
               AND snp_def_ks_lag.MTH_TM_ID = d.LAST_NEW_DEFAULT_DATE - 40
        ),
        lagged_lgd AS (
            SELECT
                *,
                LAG(OS_BAL_AMT) OVER (PARTITION BY BASEL_ACCT_ID ORDER BY MTH_TM_ID) AS LAG_OS_BAL_AMT,
                LAG(TOT_UNPAID_FNCL_CHRG_AMT) OVER (PARTITION BY BASEL_ACCT_ID ORDER BY MTH_TM_ID) AS LAG_TOT_UNPAID_FNCL_CHRG_AMT,
                LAG(PIT_STATUS_CD) OVER (PARTITION BY BASEL_ACCT_ID ORDER BY MTH_TM_ID) AS LAG_PIT_STATUS_CD
            FROM dataprep_full
            CROSS JOIN mth_tm_id
            WHERE MTH_TM_ID >= mth_tm_id.val - 48 * 40
              AND MTH_TM_ID <= mth_tm_id.val - 24 * 40
        ),
        new_defaults_lgd AS (
            SELECT BASEL_ACCT_ID, MTH_TM_ID
            FROM lagged_lgd
            WHERE CASE
                WHEN SRC_SYS_CD IN ('SPL')
                 AND PIT_STATUS_CD IN ('DEF', 'CHG')
                 AND OS_BAL_AMT >= 1
                 AND TOT_CRNT_BAL_AMT > 0
                 AND COALESCE(LAG_PIT_STATUS_CD, 'CUR') NOT IN ('DEF', 'CHG')
                THEN 1
                WHEN SRC_SYS_CD IN ('TNG-MOR', 'MOR')
                 AND PIT_STATUS_CD IN ('DEF')
                 AND LAG_PIT_STATUS_CD = 'CUR'
                THEN 1
                WHEN SRC_SYS_CD IN ('KS')
                 AND BASEL_PRD_CD = 'CC'
                 AND HELOC_F = 'N'
                 AND PIT_STATUS_CD IN ('DEF', 'CHG')
                 AND LAG_PIT_STATUS_CD = 'CUR'
                 AND OS_BAL_AMT > 0
                 AND LAG_TOT_UNPAID_FNCL_CHRG_AMT <> LAG_OS_BAL_AMT
                THEN 1
                WHEN SRC_SYS_CD IN ('KS')
                 AND BASEL_PRD_CD = 'CC'
                 AND HELOC_F = 'N'
                 AND PIT_STATUS_CD IN ('DEF', 'CHG')
                 AND LAG_PIT_STATUS_CD = 'CUR'
                 AND OS_BAL_AMT > 0
                 AND LAG_TOT_UNPAID_FNCL_CHRG_AMT = LAG_OS_BAL_AMT
                 AND LAG_TOT_UNPAID_FNCL_CHRG_AMT <= 0
                THEN 1
                WHEN SRC_SYS_CD IN ('KS')
                 AND BASEL_PRD_CD = 'CC'
                 AND HELOC_F = 'N'
                 AND PIT_STATUS_CD IN ('DEF', 'CHG')
                 AND LAG_PIT_STATUS_CD = 'CUR'
                 AND OS_BAL_AMT > 0
                 AND LAG_TOT_UNPAID_FNCL_CHRG_AMT = LAG_OS_BAL_AMT
                 AND LAG_TOT_UNPAID_FNCL_CHRG_AMT > 0
                 AND TOT_UNPAID_FNCL_CHRG_AMT <> OS_BAL_AMT
                 AND PIT_STATUS_CD <> 'CHG'
                 AND OS_BAL_AMT >= 5
                THEN 1
                WHEN SRC_SYS_CD IN ('KS')
                 AND BASEL_PRD_CD = 'CC'
                 AND HELOC_F = 'N'
                 AND PIT_STATUS_CD IN ('DEF', 'CHG')
                 AND LAG_PIT_STATUS_CD = 'CUR'
                THEN 0
                WHEN SRC_SYS_CD IN ('KS')
                 AND BASEL_PRD_CD = 'CC'
                 AND HELOC_F = 'Y'
                 AND PIT_STATUS_CD IN ('DEF', 'CHG')
                 AND LAG_PIT_STATUS_CD = 'CUR'
                 AND LAG_TOT_UNPAID_FNCL_CHRG_AMT <> LAG_OS_BAL_AMT
                 AND OS_BAL_AMT > 0
                THEN 1
                WHEN SRC_SYS_CD IN ('KS')
                 AND BASEL_PRD_CD <> 'CC'
                 AND PIT_STATUS_CD IN ('DEF', 'CHG')
                 AND LAG_PIT_STATUS_CD = 'CUR'
                 AND LAG_TOT_UNPAID_FNCL_CHRG_AMT <> LAG_OS_BAL_AMT
                 AND OS_BAL_AMT > 0
                THEN 1
                ELSE 0
            END = 1
        ),
        defaults_lgd AS (
            SELECT BASEL_ACCT_ID, MAX(MTH_TM_ID) AS LAST_NEW_DEFAULT_DATE
            FROM new_defaults_lgd
            GROUP BY BASEL_ACCT_ID
        ),
        lgd AS (
            SELECT
                d.BASEL_ACCT_ID,
                1 AS MODEL_DFT_F,
                d.LAST_NEW_DEFAULT_DATE,
                CASE
                    WHEN snp_def.SRC_SYS_CD IN ('KS')
                     AND (snp_def.PIT_STATUS_CD = 'CHG' OR snp_def.ACCRL_STAT_F = 'N')
                     AND snp_def.OS_BAL_AMT = 0
                    THEN GREATEST(COALESCE(snp_def_ks_lag.OS_BAL_AMT, 0), 0)
                    ELSE GREATEST(COALESCE(snp_def.OS_BAL_AMT, 0), 0)
                END AS LAST_NEW_DEFAULT_OS_BAL_AMT
            FROM defaults_lgd d
            LEFT JOIN dataprep_full snp_def
                ON snp_def.BASEL_ACCT_ID = d.BASEL_ACCT_ID
               AND snp_def.MTH_TM_ID = d.LAST_NEW_DEFAULT_DATE
            LEFT JOIN dataprep_full snp_def_ks_lag
                ON snp_def_ks_lag.BASEL_ACCT_ID = d.BASEL_ACCT_ID
               AND snp_def_ks_lag.MTH_TM_ID = d.LAST_NEW_DEFAULT_DATE - 40
        ),
        pdead_rows AS (
            SELECT
                snp.MTH_TM_ID AS OBSVTN_MTH_TM_ID,
                b.BASEL_ACCT_ID,
                b.LAST_NEW_DEFAULT_OS_BAL_AMT AS LAST_NEW_DFT_BAL_AMT,
                t.TM_LVL_END_DT AS LAST_NEW_DFT_DT,
                CASE WHEN b.MODEL_DFT_F = 1 THEN 'Y' ELSE 'N' END AS MODEL_DFT_F
            FROM ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT snp
            CROSS JOIN mth_tm_id
            INNER JOIN ingestion.TM_DIM obs_tm
                ON snp.MTH_TM_ID = obs_tm.TM_ID
               AND TRIM(obs_tm.TM_LVL) = 'Month'
            INNER JOIN pdead b
                ON snp.BASEL_ACCT_ID = b.BASEL_ACCT_ID
            INNER JOIN ingestion.TM_DIM t
                ON b.LAST_NEW_DEFAULT_DATE = t.TM_ID
               AND TRIM(t.TM_LVL) = 'Month'
            INNER JOIN (
                SELECT BASEL_ACCT_ID, OBSN_DT, PIT_STATUS_CROSS_DEFAULT_ORIG
                FROM features.PIT_STATUS_CROSS_DEFAULT_ORIG
                WHERE SRC_SYS_CD = 'KS'
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY BASEL_ACCT_ID, OBSN_DT ORDER BY PIT_STATUS_CROSS_DEFAULT_ORIG DESC NULLS LAST
                ) = 1
            ) pit
                ON snp.BASEL_ACCT_ID = pit.BASEL_ACCT_ID
               AND pit.OBSN_DT = obs_tm.TM_LVL_END_DT
            INNER JOIN (
                SELECT BASEL_ACCT_ID, OBSN_DT, SML_BUS_F
                FROM features.SML_BUS_F
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY BASEL_ACCT_ID, OBSN_DT ORDER BY SML_BUS_F DESC NULLS LAST
                ) = 1
            ) sml
                ON snp.BASEL_ACCT_ID = sml.BASEL_ACCT_ID
               AND sml.OBSN_DT = obs_tm.TM_LVL_END_DT
            INNER JOIN (
                SELECT BASEL_ACCT_ID, OBSN_DT, CONSM_PRD_TREATMNT_CD
                FROM features.CONSM_PRD_TREATMNT_CD
                WHERE SRC_SYS_CD = 'KS'
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY BASEL_ACCT_ID, OBSN_DT ORDER BY CONSM_PRD_TREATMNT_CD DESC NULLS LAST
                ) = 1
            ) trt
                ON snp.BASEL_ACCT_ID = trt.BASEL_ACCT_ID
               AND trt.OBSN_DT = obs_tm.TM_LVL_END_DT
            WHERE snp.MTH_TM_ID = mth_tm_id.val - 12 * 40
              AND pit.PIT_STATUS_CROSS_DEFAULT_ORIG = 'CUR'
              AND sml.SML_BUS_F = 'N'
              AND trt.CONSM_PRD_TREATMNT_CD = 'A'
        ),
        lgd_rows AS (
            SELECT
                snp.MTH_TM_ID AS OBSVTN_MTH_TM_ID,
                b.BASEL_ACCT_ID,
                b.LAST_NEW_DEFAULT_OS_BAL_AMT AS LAST_NEW_DFT_BAL_AMT,
                t.TM_LVL_END_DT AS LAST_NEW_DFT_DT,
                CASE WHEN b.MODEL_DFT_F = 1 THEN 'Y' ELSE 'N' END AS MODEL_DFT_F
            FROM ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT snp
            CROSS JOIN mth_tm_id
            INNER JOIN ingestion.TM_DIM obs_tm
                ON snp.MTH_TM_ID = obs_tm.TM_ID
               AND TRIM(obs_tm.TM_LVL) = 'Month'
            INNER JOIN lgd b
                ON snp.BASEL_ACCT_ID = b.BASEL_ACCT_ID
            INNER JOIN ingestion.TM_DIM t
                ON b.LAST_NEW_DEFAULT_DATE = t.TM_ID
               AND TRIM(t.TM_LVL) = 'Month'
            INNER JOIN (
                SELECT BASEL_ACCT_ID, OBSN_DT, PIT_STATUS_CROSS_DEFAULT_ORIG
                FROM features.PIT_STATUS_CROSS_DEFAULT_ORIG
                WHERE SRC_SYS_CD = 'KS'
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY BASEL_ACCT_ID, OBSN_DT ORDER BY PIT_STATUS_CROSS_DEFAULT_ORIG DESC NULLS LAST
                ) = 1
            ) pit
                ON snp.BASEL_ACCT_ID = pit.BASEL_ACCT_ID
               AND pit.OBSN_DT = obs_tm.TM_LVL_END_DT
            INNER JOIN (
                SELECT BASEL_ACCT_ID, OBSN_DT, SML_BUS_F
                FROM features.SML_BUS_F
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY BASEL_ACCT_ID, OBSN_DT ORDER BY SML_BUS_F DESC NULLS LAST
                ) = 1
            ) sml
                ON snp.BASEL_ACCT_ID = sml.BASEL_ACCT_ID
               AND sml.OBSN_DT = obs_tm.TM_LVL_END_DT
            INNER JOIN (
                SELECT BASEL_ACCT_ID, OBSN_DT, CONSM_PRD_TREATMNT_CD
                FROM features.CONSM_PRD_TREATMNT_CD
                WHERE SRC_SYS_CD = 'KS'
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY BASEL_ACCT_ID, OBSN_DT ORDER BY CONSM_PRD_TREATMNT_CD DESC NULLS LAST
                ) = 1
            ) trt
                ON snp.BASEL_ACCT_ID = trt.BASEL_ACCT_ID
               AND trt.OBSN_DT = obs_tm.TM_LVL_END_DT
            WHERE snp.MTH_TM_ID = mth_tm_id.val - 24 * 40
              AND pit.PIT_STATUS_CROSS_DEFAULT_ORIG <> 'CUR'
              AND sml.SML_BUS_F = 'N'
              AND trt.CONSM_PRD_TREATMNT_CD = 'A'
        ),
        combined_ks AS (
            SELECT * FROM pdead_rows
            UNION ALL
            SELECT * FROM lgd_rows
        )
    SELECT
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        BASEL_ACCT_ID,
        OBSVTN_MTH_TM_ID,
        LAST_NEW_DFT_DT,
        'KS' AS SRC_SYS_CD
    FROM combined_ks
    """,
):
    pass


def duckdb_load(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} by name
    FROM (
        select * from read_parquet([
            '{{{{ task_instance.xcom_pull(task_ids="derived__last_new_dft_dt.export_spl", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__last_new_dft_dt.export_ks", key="parquet") }}}}'],
        union_by_name = true)
    )
    """,
):
    pass
