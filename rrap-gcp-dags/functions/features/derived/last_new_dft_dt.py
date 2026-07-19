from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

# Last-new-default DATE at the observation point.
#
# New business logic (SAS default detection deprecated): the last new default is
# the CUR->DEF onset, taken directly from features.MONTH_DEF:
#     SPL onset  <=>  MONTH_DEF = 1   (inclusive count; 1 = first default month)
#     KS  onset  <=>  MONTH_DEF = 0   (exclusive count; 0 = first default month, non-default is NULL)
# last_new_dft_dt = the most recent onset month within each population's observation window.
#
# Observation points (OBSVTN_MTH_TM_ID) and windows are unchanged:
#   SPL LGDD   obs = rundate-24, new default in [rundate-48, rundate-24], DEF at obs
#   SPL LGDND  obs = rundate-12, new default in [rundate-12, rundate],    CUR at obs
#   KS  PDEAD  obs = rundate-12, new default in [rundate-12, rundate],    CUR at obs
#   KS  LGD    obs = rundate-24, new default in [rundate-48, rundate-24], <>CUR at obs
UPSTREAM_ASSET = [
    "features.MONTH_DEF",
    "features.PIT_STATUS_CROSS_DEFAULT_ORIG",
    "features.TREATMENT_F",
    "features.OS_BAL_AMT",
    "features.SML_BUS_F",
    "features.CONSM_PRD_TREATMNT_CD",
    "ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT",
    "ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT",
    "ingestion.TM_DIM",
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
        -- observation-window boundaries as TM_DIM rows (TM_ID + month-end date)
        lgdd_obs_tm AS (
            SELECT
                end_tm.TM_ID AS OBS_END_TM_ID,   end_tm.TM_LVL_END_DT AS OBS_END_DT,
                start_tm.TM_ID AS OBS_START_TM_ID, start_tm.TM_LVL_END_DT AS OBS_START_DT
            FROM process_tm p
            INNER JOIN ingestion.TM_DIM end_tm
                ON TRIM(end_tm.TM_LVL) = 'Month'
               AND end_tm.TM_LVL_END_DT = LAST_DAY((p.TM_LVL_END_DT - INTERVAL 24 MONTH)::DATE)
            INNER JOIN ingestion.TM_DIM start_tm
                ON TRIM(start_tm.TM_LVL) = 'Month'
               AND start_tm.TM_LVL_END_DT = LAST_DAY((p.TM_LVL_END_DT - INTERVAL 48 MONTH)::DATE)
        ),
        lgdnd_obs_tm AS (
            SELECT
                end_tm.TM_ID AS OBS_END_TM_ID,   end_tm.TM_LVL_END_DT AS OBS_END_DT,
                start_tm.TM_ID AS OBS_START_TM_ID, start_tm.TM_LVL_END_DT AS OBS_START_DT
            FROM process_tm p
            INNER JOIN ingestion.TM_DIM end_tm
                ON TRIM(end_tm.TM_LVL) = 'Month'
               AND end_tm.TM_LVL_END_DT = LAST_DAY(p.TM_LVL_END_DT::DATE)
            INNER JOIN ingestion.TM_DIM start_tm
                ON TRIM(start_tm.TM_LVL) = 'Month'
               AND start_tm.TM_LVL_END_DT = LAST_DAY((p.TM_LVL_END_DT - INTERVAL 12 MONTH)::DATE)
        ),
        -- CUR->DEF onset months from MONTH_DEF (SPL onset <=> MONTH_DEF = 1)
        spl_onset AS (
            SELECT md.BASEL_ACCT_ID, md.OBSN_DT AS ONSET_DT, tm.TM_ID AS ONSET_TM_ID
            FROM features.MONTH_DEF md
            INNER JOIN ingestion.TM_DIM tm
                ON TRIM(tm.TM_LVL) = 'Month' AND tm.TM_LVL_END_DT = md.OBSN_DT
            WHERE md.SRC_SYS_CD = 'SPL' AND md.MONTH_DEF = 1
        ),
        -- point-in-time status / treatment for eligibility
        pit AS (
            SELECT BASEL_ACCT_ID, OBSN_DT, PIT_STATUS_CROSS_DEFAULT_ORIG AS PIT_STATUS
            FROM features.PIT_STATUS_CROSS_DEFAULT_ORIG
            WHERE SRC_SYS_CD = 'SPL'
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY BASEL_ACCT_ID, OBSN_DT ORDER BY PIT_STATUS_CROSS_DEFAULT_ORIG DESC NULLS LAST
            ) = 1
        ),
        trt AS (
            SELECT BASEL_ACCT_ID, OBSN_DT, TREATMENT_F
            FROM features.TREATMENT_F
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY BASEL_ACCT_ID, OBSN_DT ORDER BY TREATMENT_F DESC NULLS LAST
            ) = 1
        ),
        -- balance at the onset month: the SPL observation SAS uses the plain
        -- 3-part sum (TOT_CRNT_BAL_AMT + ADD_ON_BAL_AMT + ACCR_INTR) = features.OS_BAL_AMT,
        -- NOT the int-at-default OS_BAL_AMT_V2 (that belongs to DRVD_VARS_2).
        spl_bal AS (
            SELECT BASEL_ACCT_ID, OBSN_DT, OS_BAL_AMT
            FROM features.OS_BAL_AMT
            WHERE SRC_SYS_CD = 'SPL'
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY BASEL_ACCT_ID, OBSN_DT ORDER BY OS_BAL_AMT DESC NULLS LAST
            ) = 1
        ),
        -- LGDD: DEF at obs_end, treatment 'A'; last new default in [OBS_START, OBS_END]
        lgdd_new_def AS (
            SELECT o.BASEL_ACCT_ID, MAX(o.ONSET_TM_ID) AS ONSET_TM_ID, MAX(o.ONSET_DT) AS LAST_NEW_DFT_DT
            FROM spl_onset o
            CROSS JOIN lgdd_obs_tm w
            WHERE o.ONSET_TM_ID BETWEEN w.OBS_START_TM_ID AND w.OBS_END_TM_ID
            GROUP BY o.BASEL_ACCT_ID
        ),
        lgdd_rows AS (
            SELECT
                snp.BASEL_ACCT_ID,
                w.OBS_END_TM_ID AS OBSVTN_MTH_TM_ID,
                d.LAST_NEW_DFT_DT,
                ob.OS_BAL_AMT AS LAST_NEW_DFT_BAL_AMT,
                '' AS MODEL_DFT_F
            FROM lgdd_obs_tm w
            INNER JOIN ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT snp
                ON snp.MTH_TM_ID = w.OBS_END_TM_ID
            INNER JOIN pit  ON pit.BASEL_ACCT_ID = snp.BASEL_ACCT_ID AND pit.OBSN_DT = w.OBS_END_DT
                            AND UPPER(TRIM(pit.PIT_STATUS)) = 'DEF'
            INNER JOIN trt  ON trt.BASEL_ACCT_ID = snp.BASEL_ACCT_ID AND trt.OBSN_DT = w.OBS_END_DT
                            AND trt.TREATMENT_F = 'A'
            INNER JOIN lgdd_new_def d ON d.BASEL_ACCT_ID = snp.BASEL_ACCT_ID
            LEFT JOIN spl_bal ob
                ON ob.BASEL_ACCT_ID = d.BASEL_ACCT_ID AND ob.OBSN_DT = d.LAST_NEW_DFT_DT
        ),
        -- LGDND: CUR at obs_start, treatment 'A'; new default in [OBS_START, OBS_END]
        lgdnd_new_def AS (
            SELECT o.BASEL_ACCT_ID, MAX(o.ONSET_TM_ID) AS ONSET_TM_ID, MAX(o.ONSET_DT) AS LAST_NEW_DFT_DT
            FROM spl_onset o
            CROSS JOIN lgdnd_obs_tm w
            WHERE o.ONSET_TM_ID BETWEEN w.OBS_START_TM_ID AND w.OBS_END_TM_ID
            GROUP BY o.BASEL_ACCT_ID
        ),
        lgdnd_rows AS (
            SELECT
                snp.BASEL_ACCT_ID,
                w.OBS_START_TM_ID AS OBSVTN_MTH_TM_ID,
                d.LAST_NEW_DFT_DT,
                ob.OS_BAL_AMT AS LAST_NEW_DFT_BAL_AMT,
                CASE WHEN d.LAST_NEW_DFT_DT IS NULL THEN 'N' ELSE 'Y' END AS MODEL_DFT_F
            FROM lgdnd_obs_tm w
            INNER JOIN ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT snp
                ON snp.MTH_TM_ID = w.OBS_START_TM_ID
            INNER JOIN pit  ON pit.BASEL_ACCT_ID = snp.BASEL_ACCT_ID AND pit.OBSN_DT = w.OBS_START_DT
                            AND UPPER(TRIM(pit.PIT_STATUS)) = 'CUR'
            INNER JOIN trt  ON trt.BASEL_ACCT_ID = snp.BASEL_ACCT_ID AND trt.OBSN_DT = w.OBS_START_DT
                            AND trt.TREATMENT_F = 'A'
            LEFT JOIN lgdnd_new_def d ON d.BASEL_ACCT_ID = snp.BASEL_ACCT_ID
            LEFT JOIN spl_bal ob
                ON ob.BASEL_ACCT_ID = d.BASEL_ACCT_ID AND ob.OBSN_DT = d.LAST_NEW_DFT_DT
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
        process_tm AS (
            SELECT TM_ID, TM_LVL_END_DT FROM ingestion.TM_DIM WHERE TM_ID = (SELECT val FROM mth_tm_id)
        ),
        -- KS observation-window boundaries (mth_tm_id units are *40 per month)
        pdead_obs_tm AS (
            SELECT
                obs.TM_ID AS OBS_TM_ID, obs.TM_LVL_END_DT AS OBS_DT,
                obs.TM_ID AS WIN_START_TM_ID,
                (SELECT val FROM mth_tm_id) AS WIN_END_TM_ID
            FROM process_tm p
            INNER JOIN ingestion.TM_DIM obs
                ON TRIM(obs.TM_LVL) = 'Month'
               AND obs.TM_LVL_END_DT = LAST_DAY((p.TM_LVL_END_DT - INTERVAL 12 MONTH)::DATE)
        ),
        lgd_obs_tm AS (
            SELECT
                obs.TM_ID AS OBS_TM_ID, obs.TM_LVL_END_DT AS OBS_DT,
                start_tm.TM_ID AS WIN_START_TM_ID,
                obs.TM_ID AS WIN_END_TM_ID
            FROM process_tm p
            INNER JOIN ingestion.TM_DIM obs
                ON TRIM(obs.TM_LVL) = 'Month'
               AND obs.TM_LVL_END_DT = LAST_DAY((p.TM_LVL_END_DT - INTERVAL 24 MONTH)::DATE)
            INNER JOIN ingestion.TM_DIM start_tm
                ON TRIM(start_tm.TM_LVL) = 'Month'
               AND start_tm.TM_LVL_END_DT = LAST_DAY((p.TM_LVL_END_DT - INTERVAL 48 MONTH)::DATE)
        ),
        -- CUR->DEF onset months from MONTH_DEF (KS onset <=> MONTH_DEF = 0)
        ks_onset AS (
            SELECT md.BASEL_ACCT_ID, md.OBSN_DT AS ONSET_DT, tm.TM_ID AS ONSET_TM_ID
            FROM features.MONTH_DEF md
            INNER JOIN ingestion.TM_DIM tm
                ON TRIM(tm.TM_LVL) = 'Month' AND tm.TM_LVL_END_DT = md.OBSN_DT
            WHERE md.SRC_SYS_CD = 'KS' AND md.MONTH_DEF = 0
        ),
        pit AS (
            SELECT BASEL_ACCT_ID, OBSN_DT, PIT_STATUS_CROSS_DEFAULT_ORIG AS PIT_STATUS
            FROM features.PIT_STATUS_CROSS_DEFAULT_ORIG
            WHERE SRC_SYS_CD = 'KS'
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY BASEL_ACCT_ID, OBSN_DT ORDER BY PIT_STATUS_CROSS_DEFAULT_ORIG DESC NULLS LAST
            ) = 1
        ),
        sml AS (
            SELECT BASEL_ACCT_ID, OBSN_DT, SML_BUS_F
            FROM features.SML_BUS_F
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY BASEL_ACCT_ID, OBSN_DT ORDER BY SML_BUS_F DESC NULLS LAST
            ) = 1
        ),
        trt AS (
            SELECT BASEL_ACCT_ID, OBSN_DT, CONSM_PRD_TREATMNT_CD
            FROM features.CONSM_PRD_TREATMNT_CD
            WHERE SRC_SYS_CD = 'KS'
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY BASEL_ACCT_ID, OBSN_DT ORDER BY CONSM_PRD_TREATMNT_CD DESC NULLS LAST
            ) = 1
        ),
        bal AS (
            SELECT BASEL_ACCT_ID, OBSN_DT, OS_BAL_AMT
            FROM features.OS_BAL_AMT
            WHERE SRC_SYS_CD = 'KS'
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY BASEL_ACCT_ID, OBSN_DT ORDER BY OS_BAL_AMT DESC NULLS LAST
            ) = 1
        ),
        -- PDEAD: CUR at obs (rundate-12), new default in [obs, rundate]
        pdead_new_def AS (
            SELECT o.BASEL_ACCT_ID, MAX(o.ONSET_TM_ID) AS ONSET_TM_ID, MAX(o.ONSET_DT) AS LAST_NEW_DFT_DT
            FROM ks_onset o
            CROSS JOIN pdead_obs_tm w
            WHERE o.ONSET_TM_ID BETWEEN w.WIN_START_TM_ID AND w.WIN_END_TM_ID
            GROUP BY o.BASEL_ACCT_ID
        ),
        pdead_rows AS (
            SELECT
                snp.BASEL_ACCT_ID,
                w.OBS_TM_ID AS OBSVTN_MTH_TM_ID,
                d.LAST_NEW_DFT_DT,
                b.OS_BAL_AMT AS LAST_NEW_DFT_BAL_AMT,
                CASE WHEN d.LAST_NEW_DFT_DT IS NULL THEN 'N' ELSE 'Y' END AS MODEL_DFT_F
            FROM pdead_obs_tm w
            INNER JOIN ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT snp ON snp.MTH_TM_ID = w.OBS_TM_ID
            INNER JOIN pit ON pit.BASEL_ACCT_ID = snp.BASEL_ACCT_ID AND pit.OBSN_DT = w.OBS_DT
                           AND UPPER(TRIM(pit.PIT_STATUS)) = 'CUR'
            INNER JOIN sml ON sml.BASEL_ACCT_ID = snp.BASEL_ACCT_ID AND sml.OBSN_DT = w.OBS_DT
                           AND sml.SML_BUS_F = 'N'
            INNER JOIN trt ON trt.BASEL_ACCT_ID = snp.BASEL_ACCT_ID AND trt.OBSN_DT = w.OBS_DT
                           AND trt.CONSM_PRD_TREATMNT_CD = 'A'
            LEFT JOIN pdead_new_def d ON d.BASEL_ACCT_ID = snp.BASEL_ACCT_ID
            LEFT JOIN bal b ON b.BASEL_ACCT_ID = d.BASEL_ACCT_ID AND b.OBSN_DT = d.LAST_NEW_DFT_DT
        ),
        -- LGD: <>CUR at obs (rundate-24), new default in [rundate-48, rundate-24]
        lgd_new_def AS (
            SELECT o.BASEL_ACCT_ID, MAX(o.ONSET_TM_ID) AS ONSET_TM_ID, MAX(o.ONSET_DT) AS LAST_NEW_DFT_DT
            FROM ks_onset o
            CROSS JOIN lgd_obs_tm w
            WHERE o.ONSET_TM_ID BETWEEN w.WIN_START_TM_ID AND w.WIN_END_TM_ID
            GROUP BY o.BASEL_ACCT_ID
        ),
        lgd_rows AS (
            SELECT
                snp.BASEL_ACCT_ID,
                w.OBS_TM_ID AS OBSVTN_MTH_TM_ID,
                d.LAST_NEW_DFT_DT,
                b.OS_BAL_AMT AS LAST_NEW_DFT_BAL_AMT,
                CASE WHEN d.LAST_NEW_DFT_DT IS NULL THEN 'N' ELSE 'Y' END AS MODEL_DFT_F
            FROM lgd_obs_tm w
            INNER JOIN ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT snp ON snp.MTH_TM_ID = w.OBS_TM_ID
            INNER JOIN pit ON pit.BASEL_ACCT_ID = snp.BASEL_ACCT_ID AND pit.OBSN_DT = w.OBS_DT
                           AND UPPER(TRIM(pit.PIT_STATUS)) <> 'CUR'
            INNER JOIN sml ON sml.BASEL_ACCT_ID = snp.BASEL_ACCT_ID AND sml.OBSN_DT = w.OBS_DT
                           AND sml.SML_BUS_F = 'N'
            INNER JOIN trt ON trt.BASEL_ACCT_ID = snp.BASEL_ACCT_ID AND trt.OBSN_DT = w.OBS_DT
                           AND trt.CONSM_PRD_TREATMNT_CD = 'A'
            INNER JOIN lgd_new_def d ON d.BASEL_ACCT_ID = snp.BASEL_ACCT_ID
            LEFT JOIN bal b ON b.BASEL_ACCT_ID = d.BASEL_ACCT_ID AND b.OBSN_DT = d.LAST_NEW_DFT_DT
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
