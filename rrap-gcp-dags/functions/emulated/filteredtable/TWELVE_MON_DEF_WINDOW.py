"""
Rewrite of RRAP_MOR_MODEL_02_BNS_MOR_PD_G.sas (create_pd_obs_window + last_new_default).

Builds emulated.TWELVE_MON_DEF_WINDOW for the process month (end_period = rundate):
  export_result — 13-month observation windows + CUR→DEF default detection
  duckdb_load   — insert parquet into DuckLake

Reads stacked emulated.STATUS_FINAL history from start_period through end_period,
where start_period = month-end(end_period - 38 months) per SAS %get_model_period_dates.

Each run emits obs-window rows for every obs-start month-end in
[start_period, end_period] (~39 windows). Requires STATUS_FINAL backfill over that range.
"""

UPSTREAM_ASSET = [
    "emulated.STATUS_FINAL",
]

DOWNSTREAM_ASSET = "emulated.TWELVE_MON_DEF_WINDOW"

_TASK_GROUP = "filteredtable__TWELVE_MON_DEF_WINDOW"

DEPENDENCIES = {
    "duckdb_delete": ["export_result"],
    "export_result": ["duckdb_load"],
}

# Pivot slots 1..13 from ranked window months (mirrors SAS _status/_process_date/_current_bal arrays).
_SLOT_STATUS = ",\n                ".join(
    f"MAX(CASE WHEN slot = {i} THEN STATUS END) AS _status{i}" for i in range(1, 14)
)
_SLOT_PROCESS_DATE = ",\n                ".join(
    f"MAX(CASE WHEN slot = {i} THEN process_date END) AS _process_date{i}"
    for i in range(1, 14)
)
_SLOT_CURRENT_BAL = ",\n                ".join(
    f"MAX(CASE WHEN slot = {i} THEN CURRENT_BAL END) AS _current_bal{i}"
    for i in range(1, 14)
)

# SAS last_new_default loops ii=1..12 ascending; last match wins → COALESCE ii=12..1.
_DEFAULT_DATE = "COALESCE(\n                " + ",\n                ".join(
    f"CASE WHEN _status{i} = 'CUR' AND _status{i + 1} = 'DEF' THEN _process_date{i + 1} END"
    for i in range(12, 0, -1)
) + "\n            )"
_DEFAULT_BAL = "COALESCE(\n                " + ",\n                ".join(
    f"CASE WHEN _status{i} = 'CUR' AND _status{i + 1} = 'DEF' THEN _current_bal{i + 1} END"
    for i in range(12, 0, -1)
) + "\n            )"


def duckdb_delete(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    DELETE FROM {DOWNSTREAM_ASSET}
    WHERE OBSN_DT = DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
      AND STREAM = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}'
    """,
):
    pass


def export_result(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    WITH
        params AS (
            SELECT
                DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS end_period
        ),
        periods AS (
            SELECT
                p.end_period,
                LAST_DAY(
                    DATE_TRUNC('month', p.end_period) - INTERVAL 38 MONTH
                ) AS start_period
            FROM params p
        ),
        obs_starts AS (
            SELECT LAST_DAY(d::DATE) AS obs_start
            FROM periods p
            CROSS JOIN generate_series(
                DATE_TRUNC('month', p.start_period)::DATE,
                DATE_TRUNC('month', p.end_period)::DATE,
                INTERVAL 1 MONTH
            ) AS t(d)
        ),
        status_hist AS (
            SELECT
                sf.MORTGAGE_NO,
                sf.STATUS,
                sf.CURRENT_BAL,
                CAST(sf.PROCESS_DATE AS DATE) AS process_date,
                LAST_DAY(CAST(sf.PROCESS_DATE AS DATE)) AS process_month_end
            FROM emulated.STATUS_FINAL sf
            CROSS JOIN periods p
            WHERE sf.STREAM = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}'
              AND CAST(sf.PROCESS_DATE AS DATE) >= p.start_period
              AND CAST(sf.PROCESS_DATE AS DATE) <= p.end_period
        ),
        windowed AS (
            SELECT
                h.MORTGAGE_NO,
                h.STATUS,
                h.CURRENT_BAL,
                h.process_date,
                os.obs_start,
                LAST_DAY(
                    DATE_TRUNC('month', os.obs_start) + INTERVAL 12 MONTH
                ) AS window_end_dt
            FROM status_hist h
            INNER JOIN obs_starts os
                ON h.process_month_end >= os.obs_start
               AND h.process_month_end <= LAST_DAY(
                    DATE_TRUNC('month', os.obs_start) + INTERVAL 12 MONTH
                )
        ),
        ranked AS (
            SELECT
                w.*,
                ROW_NUMBER() OVER (
                    PARTITION BY w.MORTGAGE_NO, w.obs_start
                    ORDER BY w.process_date
                ) AS slot
            FROM windowed w
        ),
        obs_window AS (
            SELECT
                MORTGAGE_NO,
                obs_start,
                MAX(window_end_dt) AS window_end_dt,
                {_SLOT_STATUS},
                {_SLOT_PROCESS_DATE},
                {_SLOT_CURRENT_BAL}
            FROM ranked
            WHERE slot <= 13
            GROUP BY MORTGAGE_NO, obs_start
        ),
        with_default AS (
            SELECT
                ow.*,
                CASE
                    WHEN _status1 = 'CUR' AND {_DEFAULT_DATE} IS NOT NULL THEN 1
                    WHEN _status1 = 'CUR' THEN 0
                END AS default_ind,
                CASE
                    WHEN _status1 = 'CUR' THEN {_DEFAULT_DATE}
                END AS default_date,
                CASE
                    WHEN _status1 = 'CUR' THEN COALESCE({_DEFAULT_BAL}, 0)
                END AS default_bal
            FROM obs_window ow
        )
    SELECT
        DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}' AS STREAM,
        {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}} AS MTH_TM_ID,
        MORTGAGE_NO,
        obs_start AS PROCESS_DATE,
        window_end_dt AS WINDOW_END_DT,
        _status1 AS STATUS1,
        default_date AS DEFAULT_DATE,
        default_bal AS DEFAULT_BAL,
        default_ind AS DEFAULT_IND,
        CURRENT_TIMESTAMP AS INSRT_PROCESS_TMSTMP,
        CURRENT_TIMESTAMP AS UPDT_PROCESS_TMSTMP
    FROM with_default
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
