"""
Rewrite of RRAP_MOR_MODEL_02_BNS_MOR_PD_G.sas (create_pd_obs_window + last_new_default).

Thin join over the MOR 12-month default-window features. The 13-month observation
window + CUR->DEF detection now lives in the features (DEFAULT_DATE, DEFAULT_BAL,
DEFAULT_IND), which scan STATUS + CURRENT_BAL feature history directly — so this table
no longer reads emulated.STATUS_FINAL or emulated.MORTGAGE_HIST.

One row per (MORTGAGE_NO, OBSVTN_MTH_TM_ID). OBSVTN_MTH_TM_ID identifies the obs-window
start month; PROCESS_DATE is that obs-window start month-end (SAS mth_end_dt&mm).
"""

UPSTREAM_ASSET = [
    "features.DEFAULT_IND",
    "features.DEFAULT_DATE",
    "features.DEFAULT_BAL",
    "ingestion.TM_DIM",
]

DOWNSTREAM_ASSET = "emulated.TWELVE_MON_DEF_WINDOW"

DEPENDENCIES = {
    "duckdb_delete": ["duckdb_load"],
}


def duckdb_delete(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    DELETE FROM {DOWNSTREAM_ASSET}
    WHERE OBSN_DT = DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
      AND STREAM = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}'
    """,
):
    pass


def duckdb_load(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} BY NAME
    SELECT
        DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}' AS STREAM,
        ind.MORTGAGE_NO,
        obs.TM_LVL_END_DT AS PROCESS_DATE,
        dt.DEFAULT_DATE,
        bal.DEFAULT_BAL,
        ind.DEFAULT_IND,
        CURRENT_TIMESTAMP AS INSRT_PROCESS_TMSTMP,
        CURRENT_TIMESTAMP AS UPDT_PROCESS_TMSTMP
    FROM features.DEFAULT_IND ind
    INNER JOIN (
        SELECT TM_ID, TM_LVL_END_DT FROM ingestion.TM_DIM
        WHERE TRIM(TM_LVL) = 'Month'
        QUALIFY ROW_NUMBER() OVER (PARTITION BY TM_ID ORDER BY TM_LVL_END_DT) = 1
    ) obs ON obs.TM_ID = ind.OBSVTN_MTH_TM_ID
    LEFT JOIN features.DEFAULT_DATE dt
        ON dt.MORTGAGE_NO = ind.MORTGAGE_NO
       AND dt.OBSVTN_MTH_TM_ID = ind.OBSVTN_MTH_TM_ID
       AND dt.OBSN_DT = ind.OBSN_DT
       AND dt.SRC_SYS_CD = ind.SRC_SYS_CD
    LEFT JOIN features.DEFAULT_BAL bal
        ON bal.MORTGAGE_NO = ind.MORTGAGE_NO
       AND bal.OBSVTN_MTH_TM_ID = ind.OBSVTN_MTH_TM_ID
       AND bal.OBSN_DT = ind.OBSN_DT
       AND bal.SRC_SYS_CD = ind.SRC_SYS_CD
    WHERE ind.OBSN_DT = DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
      AND ind.SRC_SYS_CD = 'MOR'
    """,
):
    pass
