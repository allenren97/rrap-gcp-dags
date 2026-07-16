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
    "features.MORT_NUM",
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
        {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}} AS MTH_TM_ID,
        TRY_CAST(mn.MORT_NUM AS BIGINT) AS MORTGAGE_NO,
        ind.OBSVTN_MTH_TM_ID,
        obs.TM_LVL_END_DT AS PROCESS_DATE,
        dt.DEFAULT_DATE,
        bal.DEFAULT_BAL,
        ind.DEFAULT_IND,
        CURRENT_TIMESTAMP AS INSRT_PROCESS_TMSTMP,
        CURRENT_TIMESTAMP AS UPDT_PROCESS_TMSTMP
    FROM features.DEFAULT_IND ind
    INNER JOIN (
        SELECT BASEL_ACCT_ID, MORT_NUM
        FROM features.MORT_NUM
        WHERE SRC_SYS_CD = 'MOR'
          AND OBSN_DT = DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY BASEL_ACCT_ID ORDER BY MORT_NUM DESC NULLS LAST
        ) = 1
    ) mn ON mn.BASEL_ACCT_ID = ind.BASEL_ACCT_ID
    INNER JOIN ingestion.TM_DIM obs
        ON obs.TM_ID = ind.OBSVTN_MTH_TM_ID
       AND TRIM(obs.TM_LVL) = 'Month'
    LEFT JOIN features.DEFAULT_DATE dt
        ON dt.BASEL_ACCT_ID = ind.BASEL_ACCT_ID
       AND dt.OBSVTN_MTH_TM_ID = ind.OBSVTN_MTH_TM_ID
       AND dt.OBSN_DT = ind.OBSN_DT
       AND dt.SRC_SYS_CD = ind.SRC_SYS_CD
    LEFT JOIN features.DEFAULT_BAL bal
        ON bal.BASEL_ACCT_ID = ind.BASEL_ACCT_ID
       AND bal.OBSVTN_MTH_TM_ID = ind.OBSVTN_MTH_TM_ID
       AND bal.OBSN_DT = ind.OBSN_DT
       AND bal.SRC_SYS_CD = ind.SRC_SYS_CD
    WHERE ind.OBSN_DT = DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
      AND ind.SRC_SYS_CD = 'MOR'
    """,
):
    pass
