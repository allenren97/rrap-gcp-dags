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
# KS is batched by MOD(HASH(BASEL_ACCT_ID), 6) to bound peak memory (the 49-month
# panel scan otherwise OOMs on the full KS population). SPL is a single pass.
UPSTREAM_ASSET = [
    "features.PIT_STATUS_CROSS_DEFAULT_ORIG",
    "features.BASEL_PRD_CD",
    "features.HELOC_F",
    "features.SML_BUS_F",
    "features.CONSM_PRD_TREATMNT_CD",
    "ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT",
    "ingestion.TM_DIM",
]
DOWNSTREAM_ASSET = "features.LAST_NEW_DFT_DT"
DEPENDENCIES = {
    "duckdb_delete": ["export_spl", "export_account_buckets"],
    "export_account_buckets": [
        "export_ks_batch_1", "export_ks_batch_2", "export_ks_batch_3",
        "export_ks_batch_4", "export_ks_batch_5", "export_ks_batch_6",
    ],
    "export_spl": ["duckdb_load"],
    "export_ks_batch_1": ["duckdb_load"],
    "export_ks_batch_2": ["duckdb_load"],
    "export_ks_batch_3": ["duckdb_load"],
    "export_ks_batch_4": ["duckdb_load"],
    "export_ks_batch_5": ["duckdb_load"],
    "export_ks_batch_6": ["duckdb_load"],
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


# SPL: status-only default definition (no balance gate). Single pass.
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
    -- SPL new-default per SAS J_RRAP_TL10_2201: MAX_NON_DEF = latest CUR in the
    -- window; LAST_NEW_DEF = earliest DEF after it, or if no CUR in the window the
    -- earliest DEF in the window (:1824-1827, :2005-2011). DEF-only (CHG is neither a
    -- reset nor a new default: :1234/:1695), and no CUR-at-obs cohort filter.
    mnd AS (
        SELECT
            BASEL_ACCT_ID,
            MAX(CASE WHEN pit_status = 'CUR' AND mth_tm_id BETWEEN {_TM} - 12 * 40 AND {_TM}          THEN mth_tm_id END) AS mnd_pd,
            MAX(CASE WHEN pit_status = 'CUR' AND mth_tm_id BETWEEN {_TM} - 48 * 40 AND {_TM} - 24 * 40 THEN mth_tm_id END) AS mnd_lgd
        FROM panel GROUP BY BASEL_ACCT_ID
    ),
    pdead AS (
        SELECT BASEL_ACCT_ID, {_TM} - 12 * 40 AS OBSVTN_MTH_TM_ID, last_new_dft_tm FROM (
            SELECT p.BASEL_ACCT_ID,
                MIN(CASE WHEN p.pit_status = 'DEF'
                          AND p.mth_tm_id BETWEEN {_TM} - 12 * 40 AND {_TM}
                          AND (m.mnd_pd IS NULL OR p.mth_tm_id > m.mnd_pd)
                         THEN p.mth_tm_id END) AS last_new_dft_tm
            FROM panel p
            JOIN mnd m ON m.BASEL_ACCT_ID = p.BASEL_ACCT_ID
            GROUP BY p.BASEL_ACCT_ID
        ) WHERE last_new_dft_tm IS NOT NULL
    ),
    lgd AS (
        SELECT BASEL_ACCT_ID, {_TM} - 24 * 40 AS OBSVTN_MTH_TM_ID, last_new_dft_tm FROM (
            SELECT p.BASEL_ACCT_ID,
                MIN(CASE WHEN p.pit_status = 'DEF'
                          AND p.mth_tm_id BETWEEN {_TM} - 48 * 40 AND {_TM} - 24 * 40
                          AND (m.mnd_lgd IS NULL OR p.mth_tm_id > m.mnd_lgd)
                         THEN p.mth_tm_id END) AS last_new_dft_tm
            FROM panel p
            JOIN mnd m ON m.BASEL_ACCT_ID = p.BASEL_ACCT_ID
            GROUP BY p.BASEL_ACCT_ID
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


def export_account_buckets(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    SELECT DISTINCT
        6 AS BATCH_COUNT,
        MOD(HASH(BASEL_ACCT_ID), 6) AS BATCH_ID,
        BASEL_ACCT_ID
    FROM features.PIT_STATUS_CROSS_DEFAULT_ORIG
    WHERE SRC_SYS_CD = 'KS'
      AND TRIM(PIT_STATUS_CROSS_DEFAULT_ORIG) IN ('DEF', 'CHG')
      AND OBSN_DT BETWEEN LAST_DAY(DATE '{_RUNDATE}' - INTERVAL 49 MONTH) AND DATE '{_RUNDATE}'
    """,
):
    pass


# KS: status + balance/charge anti-artifact gate (rrap_defaulter_model KS branch),
# batched by MOD(HASH(BASEL_ACCT_ID), REPLACE_COUNT) = REPLACE_ID.
RENDER_KS = """
    WITH batch_accounts AS MATERIALIZED (
        SELECT B.BASEL_ACCT_ID
        FROM '{{ task_instance.xcom_pull(task_ids="derived__last_new_dft_dt.export_account_buckets", key="parquet") }}' B
        WHERE B.BATCH_COUNT = REPLACE_COUNT AND B.BATCH_ID = REPLACE_ID
    ),
    panel AS (
        SELECT
            pit.BASEL_ACCT_ID,
            tm.TM_ID AS mth_tm_id,
            TRIM(pit.PIT_STATUS_CROSS_DEFAULT_ORIG) AS pit_status,
            snp.TOT_NEW_BAL_AMT AS OS_BAL_AMT,
            snp.TOT_UNPAID_FNCL_CHRG_AMT AS charge,
            prd.BASEL_PRD_CD,
            hel.HELOC_F,
            smb.SML_BUS_F,
            trt.CONSM_PRD_TREATMNT_CD
        FROM features.PIT_STATUS_CROSS_DEFAULT_ORIG pit
        INNER JOIN batch_accounts ba ON ba.BASEL_ACCT_ID = pit.BASEL_ACCT_ID
        INNER JOIN ingestion.TM_DIM tm
            ON tm.TM_LVL_END_DT = pit.OBSN_DT AND TRIM(tm.TM_LVL) = 'Month'
        LEFT JOIN ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT snp
            ON snp.BASEL_ACCT_ID = pit.BASEL_ACCT_ID AND snp.MTH_TM_ID = tm.TM_ID
           AND snp.MTH_TM_ID BETWEEN {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }} - 49 * 40 AND {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}
        LEFT JOIN (
            SELECT s.BASEL_ACCT_ID, s.OBSN_DT, s.BASEL_PRD_CD FROM features.BASEL_PRD_CD s
            INNER JOIN batch_accounts bacc ON bacc.BASEL_ACCT_ID = s.BASEL_ACCT_ID
            WHERE s.OBSN_DT BETWEEN LAST_DAY(DATE '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' - INTERVAL 49 MONTH) AND DATE '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
            QUALIFY ROW_NUMBER() OVER (PARTITION BY s.BASEL_ACCT_ID, s.OBSN_DT ORDER BY s.BASEL_PRD_CD DESC NULLS LAST) = 1
        ) prd ON prd.BASEL_ACCT_ID = pit.BASEL_ACCT_ID AND prd.OBSN_DT = pit.OBSN_DT
        LEFT JOIN (
            SELECT s.BASEL_ACCT_ID, s.OBSN_DT, s.HELOC_F FROM features.HELOC_F s
            INNER JOIN batch_accounts bacc ON bacc.BASEL_ACCT_ID = s.BASEL_ACCT_ID
            WHERE s.OBSN_DT BETWEEN LAST_DAY(DATE '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' - INTERVAL 49 MONTH) AND DATE '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
            QUALIFY ROW_NUMBER() OVER (PARTITION BY s.BASEL_ACCT_ID, s.OBSN_DT ORDER BY s.HELOC_F DESC NULLS LAST) = 1
        ) hel ON hel.BASEL_ACCT_ID = pit.BASEL_ACCT_ID AND hel.OBSN_DT = pit.OBSN_DT
        LEFT JOIN (
            SELECT s.BASEL_ACCT_ID, s.OBSN_DT, s.SML_BUS_F FROM features.SML_BUS_F s
            INNER JOIN batch_accounts bacc ON bacc.BASEL_ACCT_ID = s.BASEL_ACCT_ID
            WHERE s.OBSN_DT BETWEEN LAST_DAY(DATE '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' - INTERVAL 49 MONTH) AND DATE '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
            QUALIFY ROW_NUMBER() OVER (PARTITION BY s.BASEL_ACCT_ID, s.OBSN_DT ORDER BY s.SML_BUS_F DESC NULLS LAST) = 1
        ) smb ON smb.BASEL_ACCT_ID = pit.BASEL_ACCT_ID AND smb.OBSN_DT = pit.OBSN_DT
        LEFT JOIN (
            SELECT s.BASEL_ACCT_ID, s.OBSN_DT, s.CONSM_PRD_TREATMNT_CD FROM features.CONSM_PRD_TREATMNT_CD s
            INNER JOIN batch_accounts bacc ON bacc.BASEL_ACCT_ID = s.BASEL_ACCT_ID
            WHERE s.SRC_SYS_CD = 'KS'
              AND s.OBSN_DT BETWEEN LAST_DAY(DATE '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' - INTERVAL 49 MONTH) AND DATE '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
            QUALIFY ROW_NUMBER() OVER (PARTITION BY s.BASEL_ACCT_ID, s.OBSN_DT ORDER BY s.CONSM_PRD_TREATMNT_CD DESC NULLS LAST) = 1
        ) trt ON trt.BASEL_ACCT_ID = pit.BASEL_ACCT_ID AND trt.OBSN_DT = pit.OBSN_DT
        WHERE pit.SRC_SYS_CD = 'KS'
          AND pit.OBSN_DT BETWEEN LAST_DAY(DATE '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' - INTERVAL 49 MONTH) AND DATE '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
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
            MAX(CASE WHEN mth_tm_id = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }} - 12 * 40 THEN pit_status END) AS status_r12,
            MAX(CASE WHEN mth_tm_id = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }} - 24 * 40 THEN pit_status END) AS status_r24,
            MAX(CASE WHEN mth_tm_id = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }} - 12 * 40 THEN SML_BUS_F END) AS sml_bus_r12,
            MAX(CASE WHEN mth_tm_id = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }} - 24 * 40 THEN SML_BUS_F END) AS sml_bus_r24,
            MAX(CASE WHEN mth_tm_id = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }} - 12 * 40 THEN CONSM_PRD_TREATMNT_CD END) AS treat_r12,
            MAX(CASE WHEN mth_tm_id = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }} - 24 * 40 THEN CONSM_PRD_TREATMNT_CD END) AS treat_r24
        FROM panel GROUP BY BASEL_ACCT_ID
    ),
    pdead AS (
        SELECT BASEL_ACCT_ID, OBSVTN_MTH_TM_ID, last_new_dft_tm FROM (
            SELECT n.BASEL_ACCT_ID, {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }} - 12 * 40 AS OBSVTN_MTH_TM_ID,
                MAX(CASE WHEN n.new_default_flg = 1
                          AND n.mth_tm_id BETWEEN {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }} - 12 * 40 AND {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}
                         THEN n.mth_tm_id END) AS last_new_dft_tm
            FROM newdef n
            INNER JOIN obs_status o ON o.BASEL_ACCT_ID = n.BASEL_ACCT_ID AND o.status_r12 = 'CUR' AND o.sml_bus_r12 = 'N' AND o.treat_r12 = 'A'
            GROUP BY n.BASEL_ACCT_ID
        ) WHERE last_new_dft_tm IS NOT NULL
    ),
    lgd AS (
        SELECT BASEL_ACCT_ID, OBSVTN_MTH_TM_ID, last_new_dft_tm FROM (
            SELECT n.BASEL_ACCT_ID, {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }} - 24 * 40 AS OBSVTN_MTH_TM_ID,
                MAX(CASE WHEN n.new_default_flg = 1
                          AND n.mth_tm_id BETWEEN {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }} - 48 * 40 AND {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }} - 24 * 40
                         THEN n.mth_tm_id END) AS last_new_dft_tm
            FROM newdef n
            INNER JOIN obs_status o ON o.BASEL_ACCT_ID = n.BASEL_ACCT_ID AND o.status_r24 IN ('DEF','CHG') AND o.sml_bus_r24 = 'N' AND o.treat_r24 = 'A'
            GROUP BY n.BASEL_ACCT_ID
        ) WHERE last_new_dft_tm IS NOT NULL
    )
    SELECT
        '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' AS OBSN_DT,
        b.BASEL_ACCT_ID,
        b.OBSVTN_MTH_TM_ID,
        tm.TM_LVL_END_DT AS LAST_NEW_DFT_DT,
        'KS' AS SRC_SYS_CD
    FROM (SELECT * FROM pdead UNION ALL SELECT * FROM lgd) b
    INNER JOIN ingestion.TM_DIM tm ON tm.TM_ID = b.last_new_dft_tm AND TRIM(tm.TM_LVL) = 'Month'
"""


def export_ks_batch_1(duckdb_conn_id="duckdb-conn", resource_tier="HIGH", pool_slots=96,
                      sql=RENDER_KS.replace("REPLACE_COUNT", "6").replace("REPLACE_ID", "0")):
    pass


def export_ks_batch_2(duckdb_conn_id="duckdb-conn", resource_tier="HIGH", pool_slots=96,
                      sql=RENDER_KS.replace("REPLACE_COUNT", "6").replace("REPLACE_ID", "1")):
    pass


def export_ks_batch_3(duckdb_conn_id="duckdb-conn", resource_tier="HIGH", pool_slots=96,
                      sql=RENDER_KS.replace("REPLACE_COUNT", "6").replace("REPLACE_ID", "2")):
    pass


def export_ks_batch_4(duckdb_conn_id="duckdb-conn", resource_tier="HIGH", pool_slots=96,
                      sql=RENDER_KS.replace("REPLACE_COUNT", "6").replace("REPLACE_ID", "3")):
    pass


def export_ks_batch_5(duckdb_conn_id="duckdb-conn", resource_tier="HIGH", pool_slots=96,
                      sql=RENDER_KS.replace("REPLACE_COUNT", "6").replace("REPLACE_ID", "4")):
    pass


def export_ks_batch_6(duckdb_conn_id="duckdb-conn", resource_tier="HIGH", pool_slots=96,
                      sql=RENDER_KS.replace("REPLACE_COUNT", "6").replace("REPLACE_ID", "5")):
    pass


def duckdb_load(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} by name
    FROM (
        select * from read_parquet([
            '{{{{ task_instance.xcom_pull(task_ids="derived__last_new_dft_dt.export_spl", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__last_new_dft_dt.export_ks_batch_1", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__last_new_dft_dt.export_ks_batch_2", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__last_new_dft_dt.export_ks_batch_3", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__last_new_dft_dt.export_ks_batch_4", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__last_new_dft_dt.export_ks_batch_5", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__last_new_dft_dt.export_ks_batch_6", key="parquet") }}}}'],
        union_by_name = true)
    )
    """,
):
    pass
