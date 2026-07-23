from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

# LAST_NEW_DFT_BAL_AMT = outstanding balance at the new-default month found by the
# same windowed CUR->DEF scan as features.LAST_NEW_DFT_DT (PDEAD [R-12,R] cohort
# CUR@R-12 -> OBSVTN=R-12; LGD [R-48,R-24] cohort DEF@R-24 -> OBSVTN=R-24).
#   KS : GREATEST(OS_BAL_AMT [=TOT_NEW_BAL_AMT] at the default month, 0), with the
#        special case: if that month is CHG or ACCRL_STAT_F='N' and OS_BAL_AMT = 0,
#        use the prior month's (default_tm - 40) balance instead.
#   SPL: OS_BAL_AMT_V2 at the default month (SAS: `OS_BAL_AMT_V2 as OS_BAL_AMT`).
UPSTREAM_ASSET = [
    "features.PIT_STATUS_CROSS_DEFAULT_ORIG",
    "features.OS_BAL_AMT",
    "features.OS_BAL_AMT_V2",
    "features.BASEL_PRD_CD",
    "features.HELOC_F",
    "features.ACCRL_STAT_F",
    "ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT",
    "ingestion.TM_DIM",
]
DOWNSTREAM_ASSET = "features.LAST_NEW_DFT_BAL_AMT"
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


# SPL: status-only default definition (no balance gate); balance = value at default month.
def export_spl(
    duckdb_conn_id="duckdb-conn",
    resource_tier="HIGH",
    pool_slots=96,
    sql=f"""
    WITH panel AS (
        SELECT
            pit.BASEL_ACCT_ID,
            tm.TM_ID AS mth_tm_id,
            TRIM(pit.PIT_STATUS_CROSS_DEFAULT_ORIG) AS pit_status,
            osb.OS_BAL_AMT
        FROM features.PIT_STATUS_CROSS_DEFAULT_ORIG pit
        INNER JOIN ingestion.TM_DIM tm
            ON tm.TM_LVL_END_DT = pit.OBSN_DT AND TRIM(tm.TM_LVL) = 'Month'
        LEFT JOIN (
            SELECT BASEL_ACCT_ID, OBSN_DT, OS_BAL_AMT_V2 AS OS_BAL_AMT FROM features.OS_BAL_AMT_V2
            WHERE SRC_SYS_CD = 'SPL'
            QUALIFY ROW_NUMBER() OVER (PARTITION BY BASEL_ACCT_ID, OBSN_DT ORDER BY OS_BAL_AMT_V2 DESC NULLS LAST) = 1
        ) osb ON osb.BASEL_ACCT_ID = pit.BASEL_ACCT_ID AND osb.OBSN_DT = pit.OBSN_DT
        WHERE pit.SRC_SYS_CD = 'SPL'
          AND pit.OBSN_DT BETWEEN LAST_DAY(DATE '{_RUNDATE}' - INTERVAL 49 MONTH) AND DATE '{_RUNDATE}'
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY pit.BASEL_ACCT_ID, tm.TM_ID
            ORDER BY pit.PIT_STATUS_CROSS_DEFAULT_ORIG DESC NULLS LAST
        ) = 1
    ),
    newdef AS (
        SELECT
            BASEL_ACCT_ID, mth_tm_id,
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
        dm.OS_BAL_AMT AS LAST_NEW_DFT_BAL_AMT,
        'SPL' AS SRC_SYS_CD
    FROM (SELECT * FROM pdead UNION ALL SELECT * FROM lgd) b
    LEFT JOIN panel dm ON dm.BASEL_ACCT_ID = b.BASEL_ACCT_ID AND dm.mth_tm_id = b.last_new_dft_tm
    """,
):
    pass


# KS: status + balance/charge gate; balance with the CHG/ACCRL prior-month rule.
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
            hel.HELOC_F,
            acc.ACCRL_STAT_F
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
        LEFT JOIN (
            SELECT BASEL_ACCT_ID, OBSN_DT, ACCRL_STAT_F FROM features.ACCRL_STAT_F
            QUALIFY ROW_NUMBER() OVER (PARTITION BY BASEL_ACCT_ID, OBSN_DT ORDER BY ACCRL_STAT_F DESC NULLS LAST) = 1
        ) acc ON acc.BASEL_ACCT_ID = pit.BASEL_ACCT_ID AND acc.OBSN_DT = pit.OBSN_DT
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
        SELECT BASEL_ACCT_ID, mth_tm_id,
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
        CASE
            WHEN (dm.pit_status = 'CHG' OR dm.ACCRL_STAT_F = 'N') AND COALESCE(dm.OS_BAL_AMT, 0) = 0
                THEN GREATEST(COALESCE(pm.OS_BAL_AMT, 0), 0)
            ELSE GREATEST(COALESCE(dm.OS_BAL_AMT, 0), 0)
        END AS LAST_NEW_DFT_BAL_AMT,
        'KS' AS SRC_SYS_CD
    FROM (SELECT * FROM pdead UNION ALL SELECT * FROM lgd) b
    LEFT JOIN panel dm ON dm.BASEL_ACCT_ID = b.BASEL_ACCT_ID AND dm.mth_tm_id = b.last_new_dft_tm
    LEFT JOIN panel pm ON pm.BASEL_ACCT_ID = b.BASEL_ACCT_ID AND pm.mth_tm_id = b.last_new_dft_tm - 40
    """,
):
    pass


def duckdb_load(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} by name
    FROM (
        select * from read_parquet([
            '{{{{ task_instance.xcom_pull(task_ids="derived__last_new_dft_bal_amt.export_spl", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__last_new_dft_bal_amt.export_ks", key="parquet") }}}}'],
        union_by_name = true)
    )
    """,
):
    pass
