from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

# MOR 12-month default-observation window (rewrite of RRAP_MOR_MODEL_02_BNS_MOR_PD_G.sas).
# Scans STATUS (features.PIT_STATUS_CROSS_DEFAULT_ORIG, MOR) + balance (features.CURRENT_BAL, MOR)
# over [rundate-38mo, rundate], builds 13-month forward windows per obs-start, detects the last
# CUR->DEF transition. Batched by MOD(HASH(BASEL_ACCT_ID), 6) to bound peak memory
# (the obs-window fan-out otherwise OOMs on the full MOR population).
UPSTREAM_ASSET = [
    "features.PIT_STATUS_CROSS_DEFAULT_ORIG",
    "features.CURRENT_BAL",
    "ingestion.TM_DIM",
]
DOWNSTREAM_ASSET = "features.DEFAULT_DATE"
DEPENDENCIES = {
    "duckdb_delete": ["export_account_buckets"],
    "export_account_buckets": ["export_mor_batch_1", "export_mor_batch_2", "export_mor_batch_3", "export_mor_batch_4", "export_mor_batch_5", "export_mor_batch_6"],
    "export_mor_batch_1": ["duckdb_load"],
    "export_mor_batch_2": ["duckdb_load"],
    "export_mor_batch_3": ["duckdb_load"],
    "export_mor_batch_4": ["duckdb_load"],
    "export_mor_batch_5": ["duckdb_load"],
    "export_mor_batch_6": ["duckdb_load"],
}


RENDER_SQL = """
    WITH
        params AS (
            SELECT
                DATE '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' AS end_period,
                REPLACE_COUNT AS batch_count,
                REPLACE_ID AS batch_id
        ),
        periods AS (
            SELECT
                p.end_period,
                p.batch_count,
                p.batch_id,
                LAST_DAY(DATE_TRUNC('month', p.end_period) - INTERVAL 38 MONTH) AS start_period,
                -- Forward horizon: the obs-start = end_period window looks 12 months
                -- ahead, so the status/balance scan must reach end_period+12 (SAS
                -- scans full status_final history with no upper bound). Without this,
                -- recent obs-windows truncate at rundate and DEFAULT_* come out NULL.
                LAST_DAY(DATE_TRUNC('month', p.end_period) + INTERVAL 12 MONTH) AS forward_end
            FROM params p
        ),
        batch_accounts AS MATERIALIZED (
            SELECT B.BASEL_ACCT_ID
            FROM '{{ task_instance.xcom_pull(task_ids="derived__default_date.export_account_buckets", key="parquet") }}' B
            CROSS JOIN params P
            WHERE B.BATCH_COUNT = P.batch_count
              AND B.BATCH_ID = P.batch_id
        ),
        obs_starts AS (
            SELECT tm.TM_ID AS obs_start_tm_id, tm.TM_LVL_END_DT AS obs_start
            FROM periods p
            INNER JOIN ingestion.TM_DIM tm
                ON TRIM(tm.TM_LVL) = 'Month'
               AND tm.TM_LVL_END_DT BETWEEN p.start_period AND p.end_period
        ),
        status_hist AS (
            SELECT
                pit.BASEL_ACCT_ID,
                pit.PIT_STATUS_CROSS_DEFAULT_ORIG AS STATUS,
                cb.CURRENT_BAL,
                pit.OBSN_DT AS process_date,
                pit.OBSN_DT AS process_month_end
            FROM (
                SELECT s.BASEL_ACCT_ID, s.OBSN_DT, s.PIT_STATUS_CROSS_DEFAULT_ORIG
                FROM features.PIT_STATUS_CROSS_DEFAULT_ORIG s
                INNER JOIN batch_accounts ba ON s.BASEL_ACCT_ID = ba.BASEL_ACCT_ID
                WHERE s.SRC_SYS_CD = 'MOR'
                  AND s.OBSN_DT BETWEEN (SELECT start_period FROM periods) AND (SELECT forward_end FROM periods)
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY s.BASEL_ACCT_ID, s.OBSN_DT
                    ORDER BY s.PIT_STATUS_CROSS_DEFAULT_ORIG DESC NULLS LAST
                ) = 1
            ) pit
            LEFT JOIN (
                SELECT BASEL_ACCT_ID, OBSN_DT, CURRENT_BAL
                FROM features.CURRENT_BAL
                WHERE SRC_SYS_CD = 'MOR'
                  AND OBSN_DT BETWEEN (SELECT start_period FROM periods) AND (SELECT forward_end FROM periods)
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY BASEL_ACCT_ID, OBSN_DT ORDER BY CURRENT_BAL DESC NULLS LAST
                ) = 1
            ) cb
                ON cb.BASEL_ACCT_ID = pit.BASEL_ACCT_ID AND cb.OBSN_DT = pit.OBSN_DT
        ),
        windowed AS (
            SELECT
                h.BASEL_ACCT_ID, h.STATUS, h.CURRENT_BAL, h.process_date,
                os.obs_start, os.obs_start_tm_id,
                LAST_DAY(DATE_TRUNC('month', os.obs_start) + INTERVAL 12 MONTH) AS window_end_dt
            FROM status_hist h
            INNER JOIN obs_starts os
                ON h.process_month_end >= os.obs_start
               AND h.process_month_end <= LAST_DAY(DATE_TRUNC('month', os.obs_start) + INTERVAL 12 MONTH)
        ),
        ranked AS (
            SELECT w.*,
                ROW_NUMBER() OVER (PARTITION BY w.BASEL_ACCT_ID, w.obs_start ORDER BY w.process_date) AS slot
            FROM windowed w
        ),
        obs_window AS (
            SELECT
                BASEL_ACCT_ID,
                obs_start,
                MAX(obs_start_tm_id) AS obs_start_tm_id,
                MAX(window_end_dt) AS window_end_dt,
                MAX(process_date) AS last_process_date,
                MAX(CASE WHEN slot = 1 THEN STATUS END) AS _status1,
                MAX(CASE WHEN slot = 2 THEN STATUS END) AS _status2,
                MAX(CASE WHEN slot = 3 THEN STATUS END) AS _status3,
                MAX(CASE WHEN slot = 4 THEN STATUS END) AS _status4,
                MAX(CASE WHEN slot = 5 THEN STATUS END) AS _status5,
                MAX(CASE WHEN slot = 6 THEN STATUS END) AS _status6,
                MAX(CASE WHEN slot = 7 THEN STATUS END) AS _status7,
                MAX(CASE WHEN slot = 8 THEN STATUS END) AS _status8,
                MAX(CASE WHEN slot = 9 THEN STATUS END) AS _status9,
                MAX(CASE WHEN slot = 10 THEN STATUS END) AS _status10,
                MAX(CASE WHEN slot = 11 THEN STATUS END) AS _status11,
                MAX(CASE WHEN slot = 12 THEN STATUS END) AS _status12,
                MAX(CASE WHEN slot = 13 THEN STATUS END) AS _status13,
                MAX(CASE WHEN slot = 1 THEN process_date END) AS _process_date1,
                MAX(CASE WHEN slot = 2 THEN process_date END) AS _process_date2,
                MAX(CASE WHEN slot = 3 THEN process_date END) AS _process_date3,
                MAX(CASE WHEN slot = 4 THEN process_date END) AS _process_date4,
                MAX(CASE WHEN slot = 5 THEN process_date END) AS _process_date5,
                MAX(CASE WHEN slot = 6 THEN process_date END) AS _process_date6,
                MAX(CASE WHEN slot = 7 THEN process_date END) AS _process_date7,
                MAX(CASE WHEN slot = 8 THEN process_date END) AS _process_date8,
                MAX(CASE WHEN slot = 9 THEN process_date END) AS _process_date9,
                MAX(CASE WHEN slot = 10 THEN process_date END) AS _process_date10,
                MAX(CASE WHEN slot = 11 THEN process_date END) AS _process_date11,
                MAX(CASE WHEN slot = 12 THEN process_date END) AS _process_date12,
                MAX(CASE WHEN slot = 13 THEN process_date END) AS _process_date13,
                MAX(CASE WHEN slot = 1 THEN CURRENT_BAL END) AS _current_bal1,
                MAX(CASE WHEN slot = 2 THEN CURRENT_BAL END) AS _current_bal2,
                MAX(CASE WHEN slot = 3 THEN CURRENT_BAL END) AS _current_bal3,
                MAX(CASE WHEN slot = 4 THEN CURRENT_BAL END) AS _current_bal4,
                MAX(CASE WHEN slot = 5 THEN CURRENT_BAL END) AS _current_bal5,
                MAX(CASE WHEN slot = 6 THEN CURRENT_BAL END) AS _current_bal6,
                MAX(CASE WHEN slot = 7 THEN CURRENT_BAL END) AS _current_bal7,
                MAX(CASE WHEN slot = 8 THEN CURRENT_BAL END) AS _current_bal8,
                MAX(CASE WHEN slot = 9 THEN CURRENT_BAL END) AS _current_bal9,
                MAX(CASE WHEN slot = 10 THEN CURRENT_BAL END) AS _current_bal10,
                MAX(CASE WHEN slot = 11 THEN CURRENT_BAL END) AS _current_bal11,
                MAX(CASE WHEN slot = 12 THEN CURRENT_BAL END) AS _current_bal12,
                MAX(CASE WHEN slot = 13 THEN CURRENT_BAL END) AS _current_bal13
            FROM ranked
            WHERE slot <= 13
            GROUP BY BASEL_ACCT_ID, obs_start
        ),
        with_default AS (
            SELECT
                ow.*,
                COALESCE(
                CASE WHEN _status12 = 'CUR' AND _status13 = 'DEF' THEN _process_date13 END,
                CASE WHEN _status11 = 'CUR' AND _status12 = 'DEF' THEN _process_date12 END,
                CASE WHEN _status10 = 'CUR' AND _status11 = 'DEF' THEN _process_date11 END,
                CASE WHEN _status9 = 'CUR' AND _status10 = 'DEF' THEN _process_date10 END,
                CASE WHEN _status8 = 'CUR' AND _status9 = 'DEF' THEN _process_date9 END,
                CASE WHEN _status7 = 'CUR' AND _status8 = 'DEF' THEN _process_date8 END,
                CASE WHEN _status6 = 'CUR' AND _status7 = 'DEF' THEN _process_date7 END,
                CASE WHEN _status5 = 'CUR' AND _status6 = 'DEF' THEN _process_date6 END,
                CASE WHEN _status4 = 'CUR' AND _status5 = 'DEF' THEN _process_date5 END,
                CASE WHEN _status3 = 'CUR' AND _status4 = 'DEF' THEN _process_date4 END,
                CASE WHEN _status2 = 'CUR' AND _status3 = 'DEF' THEN _process_date3 END,
                CASE WHEN _status1 = 'CUR' AND _status2 = 'DEF' THEN _process_date2 END
            ) AS default_date,
                COALESCE(
                CASE WHEN _status12 = 'CUR' AND _status13 = 'DEF' THEN _current_bal13 END,
                CASE WHEN _status11 = 'CUR' AND _status12 = 'DEF' THEN _current_bal12 END,
                CASE WHEN _status10 = 'CUR' AND _status11 = 'DEF' THEN _current_bal11 END,
                CASE WHEN _status9 = 'CUR' AND _status10 = 'DEF' THEN _current_bal10 END,
                CASE WHEN _status8 = 'CUR' AND _status9 = 'DEF' THEN _current_bal9 END,
                CASE WHEN _status7 = 'CUR' AND _status8 = 'DEF' THEN _current_bal8 END,
                CASE WHEN _status6 = 'CUR' AND _status7 = 'DEF' THEN _current_bal7 END,
                CASE WHEN _status5 = 'CUR' AND _status6 = 'DEF' THEN _current_bal6 END,
                CASE WHEN _status4 = 'CUR' AND _status5 = 'DEF' THEN _current_bal5 END,
                CASE WHEN _status3 = 'CUR' AND _status4 = 'DEF' THEN _current_bal4 END,
                CASE WHEN _status2 = 'CUR' AND _status3 = 'DEF' THEN _current_bal3 END,
                CASE WHEN _status1 = 'CUR' AND _status2 = 'DEF' THEN _current_bal2 END
            ) AS default_bal
            FROM obs_window ow
        )
    SELECT
        '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' AS OBSN_DT,
        BASEL_ACCT_ID,
        obs_start_tm_id AS OBSVTN_MTH_TM_ID,
        CASE WHEN _status1 = 'CUR' THEN default_date END AS DEFAULT_DATE,
        'MOR' AS SRC_SYS_CD
    FROM with_default
"""


def export_account_buckets(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    WITH periods AS (
        SELECT
            DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS end_period,
            LAST_DAY(DATE_TRUNC('month', DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}') - INTERVAL 38 MONTH) AS start_period
    )
    SELECT DISTINCT
        6 AS BATCH_COUNT,
        MOD(HASH(s.BASEL_ACCT_ID), 6) AS BATCH_ID,
        s.BASEL_ACCT_ID
    FROM features.PIT_STATUS_CROSS_DEFAULT_ORIG s
    CROSS JOIN periods p
    WHERE s.SRC_SYS_CD = 'MOR'
      AND s.OBSN_DT BETWEEN p.start_period AND p.end_period
    """,
):
    pass


def export_mor_batch_1(
    duckdb_conn_id="duckdb-conn",
    resource_tier="HIGH",
    pool_slots=96,
    sql=RENDER_SQL.replace("REPLACE_COUNT", "6").replace("REPLACE_ID", "0"),
):
    pass


def export_mor_batch_2(
    duckdb_conn_id="duckdb-conn",
    resource_tier="HIGH",
    pool_slots=96,
    sql=RENDER_SQL.replace("REPLACE_COUNT", "6").replace("REPLACE_ID", "1"),
):
    pass


def export_mor_batch_3(
    duckdb_conn_id="duckdb-conn",
    resource_tier="HIGH",
    pool_slots=96,
    sql=RENDER_SQL.replace("REPLACE_COUNT", "6").replace("REPLACE_ID", "2"),
):
    pass


def export_mor_batch_4(
    duckdb_conn_id="duckdb-conn",
    resource_tier="HIGH",
    pool_slots=96,
    sql=RENDER_SQL.replace("REPLACE_COUNT", "6").replace("REPLACE_ID", "3"),
):
    pass


def export_mor_batch_5(
    duckdb_conn_id="duckdb-conn",
    resource_tier="HIGH",
    pool_slots=96,
    sql=RENDER_SQL.replace("REPLACE_COUNT", "6").replace("REPLACE_ID", "4"),
):
    pass


def export_mor_batch_6(
    duckdb_conn_id="duckdb-conn",
    resource_tier="HIGH",
    pool_slots=96,
    sql=RENDER_SQL.replace("REPLACE_COUNT", "6").replace("REPLACE_ID", "5"),
):
    pass



def duckdb_delete(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    delete from {DOWNSTREAM_ASSET} where obsn_dt = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """,
):
    pass


def duckdb_load(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} by name
    FROM (
        select * from read_parquet([
            '{{{{ task_instance.xcom_pull(task_ids="derived__default_date.export_mor_batch_1", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__default_date.export_mor_batch_2", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__default_date.export_mor_batch_3", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__default_date.export_mor_batch_4", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__default_date.export_mor_batch_5", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__default_date.export_mor_batch_6", key="parquet") }}}}'
        ], union_by_name = true)
    )
    """,
):
    pass
