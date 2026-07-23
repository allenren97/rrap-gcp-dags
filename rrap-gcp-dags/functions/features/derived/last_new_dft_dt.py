from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

# Windowed CUR->DEF scan reproducing rrap_defaulter_model / the SPL obsvtn job.
#
# For a run at month R (40 tm_id = 1 month) this emits, per account, up to two
# observation rows -- one per model window -- exactly like the SAS obsvtn tables:
#
#   PDEAD (PD/EAD): cohort = CUR at R-12; window [R-12, R]; OBSVTN_MTH_TM_ID = R-12
#   LGD           : cohort = DEF at R-24; window [R-48, R-24]; OBSVTN_MTH_TM_ID = R-24
#
# LAST_NEW_DFT_DT = TM_DIM month-end of max(mth_tm_id) where new_default_flg = 1
# inside the window (the most recent CUR->DEF transition). new_default_flg:
#   SPL: status-only  -> pit_status IN ('DEF','CHG') and prior month not in default
#   KS : status + the balance/charge anti-artifact gate (rrap_defaulter_model)
#
# Only accounts with a qualifying new default in the window emit a row (matches the
# SAS inner join to the defaulter table -> MODEL_DFT_F is always 'Y').
UPSTREAM_ASSET = [
    "features.PIT_STATUS_CROSS_DEFAULT_ORIG",
    "features.OS_BAL_AMT",
    "features.BASEL_PRD_CD",
    "features.HELOC_F",
    "ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT",
    "ingestion.TM_DIM",
]
DOWNSTREAM_ASSET = "features.LAST_NEW_DFT_DT"
DEPENDENCIES = {
    "duckdb_delete": ["export_spl", "export_ks"],
    "export_spl": ["duckdb_load"],
    "export_ks": ["duckdb_load"],
}

_RUNDATE = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
_TM = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}'


def duckdb_delete(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    delete from {DOWNSTREAM_ASSET} where obsn_dt = '{_RUNDATE}'
    """,
):
    pass


# SPL: status-only default definition (no balance gate).
def export_spl(
    duckdb_conn_id="duckdb-conn",
    resource_tier="HIGH",
    pool_slots=96,
    sql=f"""
    WITH panel AS (
        SELECT
            pit.BASEL_ACCT_ID,
            tm.TM_ID AS mth_tm_id,
            TRIM(pit.PIT_STATUS_CROSS_DEFAULT_ORIG) AS pit_status
        FROM features.PIT_STATUS_CROSS_DEFAULT_ORIG pit
        INNER JOIN ingestion.TM_DIM tm
            ON tm.TM_LVL_END_DT = pit.OBSN_DT AND TRIM(tm.TM_LVL) = 'Month'
        WHERE pit.SRC_SYS_CD = 'SPL'
          AND pit.OBSN_DT BETWEEN LAST_DAY(DATE '{_RUNDATE}' - INTERVAL 49 MONTH) AND DATE '{_RUNDATE}'
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY pit.BASEL_ACCT_ID, tm.TM_ID
            ORDER BY pit.PIT_STATUS_CROSS_DEFAULT_ORIG DESC NULLS LAST
        ) = 1
    ),
    newdef AS (
        SELECT
            BASEL_ACCT_ID, mth_tm_id, pit_status,
            CASE
                WHEN pit_status IN ('DEF','CHG')
                     AND (LAG(pit_status) OVER w IS NULL OR LAG(pit_status) OVER w NOT IN ('DEF','CHG'))
                THEN 1 ELSE 0
            END AS new_default_flg
        FROM panel
        WINDOW w AS (PARTITION BY BASEL_ACCT_ID ORDER BY mth_tm_id)
    ),
    obs_status AS (
        SELECT
            BASEL_ACCT_ID,
            MAX(CASE WHEN mth_tm_id = {_TM} - 12 * 40 THEN pit_status END) AS status_r12,
            MAX(CASE WHEN mth_tm_id = {_TM} - 24 * 40 THEN pit_status END) AS status_r24
        FROM panel GROUP BY BASEL_ACCT_ID
    ),
    pdead AS (
        SELECT BASEL_ACCT_ID, OBSVTN_MTH_TM_ID, last_new_dft_tm FROM (
            SELECT n.BASEL_ACCT_ID, {_TM} - 12 * 40 AS OBSVTN_MTH_TM_ID,
                MAX(CASE WHEN n.new_default_flg = 1 AND n.mth_tm_id BETWEEN {_TM} - 12 * 40 AND {_TM}
                         THEN n.mth_tm_id END) AS last_new_dft_tm
            FROM newdef n
            INNER JOIN obs_status o ON o.BASEL_ACCT_ID = n.BASEL_ACCT_ID AND o.status_r12 = 'CUR'
            GROUP BY n.BASEL_ACCT_ID
        ) WHERE last_new_dft_tm IS NOT NULL
    ),
    lgd AS (
        SELECT BASEL_ACCT_ID, OBSVTN_MTH_TM_ID, last_new_dft_tm FROM (
            SELECT n.BASEL_ACCT_ID, {_TM} - 24 * 40 AS OBSVTN_MTH_TM_ID,
                MAX(CASE WHEN n.new_default_flg = 1 AND n.mth_tm_id BETWEEN {_TM} - 48 * 40 AND {_TM} - 24 * 40
                         THEN n.mth_tm_id END) AS last_new_dft_tm
            FROM newdef n
            INNER JOIN obs_status o ON o.BASEL_ACCT_ID = n.BASEL_ACCT_ID AND o.status_r24 IN ('DEF','CHG')
            GROUP BY n.BASEL_ACCT_ID
        ) WHERE last_new_dft_tm IS NOT NULL
    )
    SELECT
        '{_RUNDATE}' AS OBSN_DT,
        b.BASEL_ACCT_ID,
        b.OBSVTN_MTH_TM_ID,
        tm.TM_LVL_END_DT AS LAST_NEW_DFT_DT,
        'SPL' AS SRC_SYS_CD
    FROM (SELECT * FROM pdead UNION ALL SELECT * FROM lgd) b
    INNER JOIN ingestion.TM_DIM tm ON tm.TM_ID = b.last_new_dft_tm AND TRIM(tm.TM_LVL) = 'Month'
    """,
):
    pass


# KS: status + balance/charge anti-artifact gate (rrap_defaulter_model KS branch).
def export_ks(
    duckdb_conn_id="duckdb-conn",
    resource_tier="HIGH",
    pool_slots=96,
    sql=f"""
    WITH panel AS (
        SELECT
            pit.BASEL_ACCT_ID,
            tm.TM_ID AS mth_tm_id,
            TRIM(pit.PIT_STATUS_CROSS_DEFAULT_ORIG) AS pit_status,
            osb.OS_BAL_AMT,
            snp.TOT_UNPAID_FNCL_CHRG_AMT AS charge,
            prd.BASEL_PRD_CD,
            hel.HELOC_F
        FROM features.PIT_STATUS_CROSS_DEFAULT_ORIG pit
        INNER JOIN ingestion.TM_DIM tm
            ON tm.TM_LVL_END_DT = pit.OBSN_DT AND TRIM(tm.TM_LVL) = 'Month'
        LEFT JOIN (
            SELECT BASEL_ACCT_ID, OBSN_DT, OS_BAL_AMT FROM features.OS_BAL_AMT
            WHERE SRC_SYS_CD = 'KS'
            QUALIFY ROW_NUMBER() OVER (PARTITION BY BASEL_ACCT_ID, OBSN_DT ORDER BY OS_BAL_AMT DESC NULLS LAST) = 1
        ) osb ON osb.BASEL_ACCT_ID = pit.BASEL_ACCT_ID AND osb.OBSN_DT = pit.OBSN_DT
        LEFT JOIN ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT snp
            ON snp.BASEL_ACCT_ID = pit.BASEL_ACCT_ID AND snp.MTH_TM_ID = tm.TM_ID
        LEFT JOIN (
            SELECT BASEL_ACCT_ID, OBSN_DT, BASEL_PRD_CD FROM features.BASEL_PRD_CD
            QUALIFY ROW_NUMBER() OVER (PARTITION BY BASEL_ACCT_ID, OBSN_DT ORDER BY BASEL_PRD_CD DESC NULLS LAST) = 1
        ) prd ON prd.BASEL_ACCT_ID = pit.BASEL_ACCT_ID AND prd.OBSN_DT = pit.OBSN_DT
        LEFT JOIN (
            SELECT BASEL_ACCT_ID, OBSN_DT, HELOC_F FROM features.HELOC_F
            QUALIFY ROW_NUMBER() OVER (PARTITION BY BASEL_ACCT_ID, OBSN_DT ORDER BY HELOC_F DESC NULLS LAST) = 1
        ) hel ON hel.BASEL_ACCT_ID = pit.BASEL_ACCT_ID AND hel.OBSN_DT = pit.OBSN_DT
        WHERE pit.SRC_SYS_CD = 'KS'
          AND pit.OBSN_DT BETWEEN LAST_DAY(DATE '{_RUNDATE}' - INTERVAL 49 MONTH) AND DATE '{_RUNDATE}'
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY pit.BASEL_ACCT_ID, tm.TM_ID
            ORDER BY pit.PIT_STATUS_CROSS_DEFAULT_ORIG DESC NULLS LAST
        ) = 1
    ),
    lagged AS (
        SELECT *,
            LAG(pit_status)  OVER w AS lag_status,
            LAG(OS_BAL_AMT)  OVER w AS lag_bal,
            LAG(charge)      OVER w AS lag_charge
        FROM panel
        WINDOW w AS (PARTITION BY BASEL_ACCT_ID ORDER BY mth_tm_id)
    ),
    newdef AS (
        SELECT BASEL_ACCT_ID, mth_tm_id, pit_status,
            CASE WHEN pit_status IN ('DEF','CHG') AND lag_status = 'CUR' THEN
                CASE
                    WHEN BASEL_PRD_CD = 'CC' AND HELOC_F = 'N' THEN
                        CASE
                            WHEN OS_BAL_AMT > 0 AND lag_charge <> lag_bal THEN 1
                            WHEN OS_BAL_AMT > 0 AND lag_charge = lag_bal AND lag_charge <= 0 THEN 1
                            WHEN OS_BAL_AMT > 0 AND lag_charge = lag_bal AND lag_charge > 0
                                 AND charge <> OS_BAL_AMT AND pit_status <> 'CHG' AND OS_BAL_AMT >= 5 THEN 1
                            ELSE 0
                        END
                    WHEN BASEL_PRD_CD = 'CC' AND HELOC_F = 'Y' THEN
                        CASE WHEN lag_charge <> lag_bal AND OS_BAL_AMT > 0 THEN 1 ELSE 0 END
                    WHEN BASEL_PRD_CD <> 'CC' THEN
                        CASE WHEN lag_charge <> lag_bal AND OS_BAL_AMT > 0 THEN 1 ELSE 0 END
                    ELSE 0
                END
            ELSE 0 END AS new_default_flg
        FROM lagged
    ),
    obs_status AS (
        SELECT
            BASEL_ACCT_ID,
            MAX(CASE WHEN mth_tm_id = {_TM} - 12 * 40 THEN pit_status END) AS status_r12,
            MAX(CASE WHEN mth_tm_id = {_TM} - 24 * 40 THEN pit_status END) AS status_r24
        FROM panel GROUP BY BASEL_ACCT_ID
    ),
    pdead AS (
        SELECT BASEL_ACCT_ID, OBSVTN_MTH_TM_ID, last_new_dft_tm FROM (
            SELECT n.BASEL_ACCT_ID, {_TM} - 12 * 40 AS OBSVTN_MTH_TM_ID,
                MAX(CASE WHEN n.new_default_flg = 1 AND n.mth_tm_id BETWEEN {_TM} - 12 * 40 AND {_TM}
                         THEN n.mth_tm_id END) AS last_new_dft_tm
            FROM newdef n
            INNER JOIN obs_status o ON o.BASEL_ACCT_ID = n.BASEL_ACCT_ID AND o.status_r12 = 'CUR'
            GROUP BY n.BASEL_ACCT_ID
        ) WHERE last_new_dft_tm IS NOT NULL
    ),
    lgd AS (
        SELECT BASEL_ACCT_ID, OBSVTN_MTH_TM_ID, last_new_dft_tm FROM (
            SELECT n.BASEL_ACCT_ID, {_TM} - 24 * 40 AS OBSVTN_MTH_TM_ID,
                MAX(CASE WHEN n.new_default_flg = 1 AND n.mth_tm_id BETWEEN {_TM} - 48 * 40 AND {_TM} - 24 * 40
                         THEN n.mth_tm_id END) AS last_new_dft_tm
            FROM newdef n
            INNER JOIN obs_status o ON o.BASEL_ACCT_ID = n.BASEL_ACCT_ID AND o.status_r24 IN ('DEF','CHG')
            GROUP BY n.BASEL_ACCT_ID
        ) WHERE last_new_dft_tm IS NOT NULL
    )
    SELECT
        '{_RUNDATE}' AS OBSN_DT,
        b.BASEL_ACCT_ID,
        b.OBSVTN_MTH_TM_ID,
        tm.TM_LVL_END_DT AS LAST_NEW_DFT_DT,
        'KS' AS SRC_SYS_CD
    FROM (SELECT * FROM pdead UNION ALL SELECT * FROM lgd) b
    INNER JOIN ingestion.TM_DIM tm ON tm.TM_ID = b.last_new_dft_tm AND TRIM(tm.TM_LVL) = 'Month'
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
