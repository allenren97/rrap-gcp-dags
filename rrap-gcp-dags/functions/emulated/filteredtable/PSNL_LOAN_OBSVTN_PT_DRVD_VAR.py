"""
Rewrite of J_RRAP_TL10_2201_PSNL_LOAN_OBSVTN_PT_DRVD_VAR.sas.

Thin join over the observation-point features (SPL branch). Projects the default
features (LAST_NEW_DFT_DT, LAST_NEW_DFT_BAL_AMT, MODEL_DFT_F).

The observation-point features now do the full windowed CUR->DEF scan and emit the
correct OBSVTN_MTH_TM_ID per window (R-12 for PDEAD, R-24 for LGD) directly, so this
table just projects them and stamps the run month:
  PROCESS_MTH_TM_ID / PROCESS_DATE  = the RUN month R (delete/replace key)
  OBSVTN_MTH_TM_ID                  = from the feature (R-12 PDEAD / R-24 LGD)

NOTE: custuniv joins the obsvtn tables on OBSVTN_MTH_TM_ID, so the row for reporting
month M is the one produced by the run at R = M+12 (PDEAD) / M+24 (LGD). For a
same-month pipeline, custuniv would instead need to join on PROCESS_MTH_TM_ID.
"""

UPSTREAM_ASSET = [
    "features.PIT_STATUS_CROSS_DEFAULT_ORIG",
    "features.TREATMENT_F",
    "features.MODEL_DFT_F",
    "features.LAST_NEW_DFT_DT",
    "features.LAST_NEW_DFT_BAL_AMT",
    "ingestion.TM_DIM",
]

DOWNSTREAM_ASSET = "emulated.PSNL_LOAN_OBSVTN_PT_DRVD_VAR"

DEPENDENCIES = {
    "duckdb_delete": ["duckdb_load"],
}


def duckdb_delete(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    DELETE FROM {DOWNSTREAM_ASSET}
    WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
      AND STREAM = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}'
    """,
):
    pass


# Observation cohort, mirroring SAS J_RRAP_TL10_2201 -- a small defaulter cohort, NOT
# the full active book:
#   PDEAD (R-12, LGD-ND :5587-5613): CUR + TREATMNT_F='A' at R-12, restricted to accounts
#     with DEF/CHG in [R-12, R] (HD_FILTER_ACCOUNT). MODEL_DFT_F = 'Y'/'N' (:5726).
#   LGD   (R-24, LGD-D :2657-2807): DEF + TREATMNT_F='A' at R-24, defaulters only.
#     MODEL_DFT_F is always NULL (:2635 init '', never set). Sourced from features.MODEL_DFT_F.
def duckdb_load(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} BY NAME
    WITH cohort AS (
        -- PDEAD (R-12): SAS LGD-ND cohort (J_RRAP_TL10_2201:5587-5613). NOT the full
        -- active book -- accounts that are CUR + TREATMNT_F='A' at R-12 (obs_month_start)
        -- AND hit DEF/CHG somewhere in [R-12, R] (the HD_FILTER_ACCOUNT inner join,
        -- :2004/:2034). MODEL_DFT_F is then Y/N within that cohort (:5726-5730).
        SELECT DISTINCT cur12.BASEL_ACCT_ID,
            {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}} - 12 * 40 AS OBSVTN_MTH_TM_ID
        FROM (
            -- CUR at R-12
            SELECT pit.BASEL_ACCT_ID
            FROM features.PIT_STATUS_CROSS_DEFAULT_ORIG pit
            JOIN ingestion.TM_DIM tm ON tm.TM_LVL_END_DT = pit.OBSN_DT AND TRIM(tm.TM_LVL) = 'Month'
            WHERE tm.TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}} - 12 * 40
              AND pit.SRC_SYS_CD = 'SPL' AND TRIM(pit.PIT_STATUS_CROSS_DEFAULT_ORIG) = 'CUR'
        ) cur12
        JOIN (
            -- TREATMNT_F='A' at R-12
            SELECT t.BASEL_ACCT_ID
            FROM features.TREATMENT_F t
            JOIN ingestion.TM_DIM tm ON tm.TM_LVL_END_DT = t.OBSN_DT AND TRIM(tm.TM_LVL) = 'Month'
            WHERE tm.TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}} - 12 * 40
              AND t.TREATMENT_F = 'A'
        ) trt12 ON trt12.BASEL_ACCT_ID = cur12.BASEL_ACCT_ID
        JOIN (
            -- HD_FILTER_ACCOUNT: DEF/CHG somewhere in [R-12, R]
            SELECT DISTINCT p.BASEL_ACCT_ID
            FROM features.PIT_STATUS_CROSS_DEFAULT_ORIG p
            JOIN ingestion.TM_DIM tm ON tm.TM_LVL_END_DT = p.OBSN_DT AND TRIM(tm.TM_LVL) = 'Month'
            WHERE p.SRC_SYS_CD = 'SPL'
              AND tm.TM_ID BETWEEN {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}} - 12 * 40
                              AND {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
              AND TRIM(p.PIT_STATUS_CROSS_DEFAULT_ORIG) IN ('DEF', 'CHG')
        ) filt ON filt.BASEL_ACCT_ID = cur12.BASEL_ACCT_ID
        UNION ALL
        -- LGD (R-24): DEFAULTERS ONLY. SAS LGD-D builds WLGH4H as the cohort
        -- (PIT_STATUS_V2='DEF' AND TREATMNT_F='A' at R-24, :2657-2658) INNER JOIN
        -- PreFinal (= LAST_NEW_DEF_DATE, defaulters, :2520/:2807), so non-defaulters are
        -- dropped. Those two cohort filters are already applied inside features.MODEL_DFT_F
        -- LGD, so its rows are exactly that intersection -- source the R-24 cohort from it.
        SELECT DISTINCT BASEL_ACCT_ID, OBSVTN_MTH_TM_ID
        FROM features.MODEL_DFT_F
        WHERE OBSVTN_MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}} - 24 * 40
          AND OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
          AND SRC_SYS_CD = 'SPL'
    )
    SELECT
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}' AS STREAM,
        c.BASEL_ACCT_ID,
        c.OBSVTN_MTH_TM_ID,
        {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}} AS PROCESS_MTH_TM_ID,
        DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS PROCESS_DATE,
        dt.LAST_NEW_DFT_DT,
        bal.LAST_NEW_DFT_BAL_AMT,
        -- PDEAD (R-12): 'Y' if the account has a new default, else 'N' (SAS LGD-ND
        -- case, :5726-5730). LGD (R-24): always NULL -- SAS LGD-D inits MODEL_DFT_F=''
        -- and never sets it (:2635, WLGH4H :2778), so the whole R-24 column is blank/NULL.
        CASE
            WHEN c.OBSVTN_MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}} - 24 * 40 THEN NULL
            WHEN mdf.BASEL_ACCT_ID IS NOT NULL THEN 'Y'
            ELSE 'N'
        END AS MODEL_DFT_F,
        CURRENT_TIMESTAMP AS INSRT_PROCESS_TMSTMP,
        CURRENT_TIMESTAMP AS UPDT_PROCESS_TMSTMP
    FROM cohort c
    LEFT JOIN features.MODEL_DFT_F mdf
        ON mdf.BASEL_ACCT_ID = c.BASEL_ACCT_ID
       AND mdf.OBSVTN_MTH_TM_ID = c.OBSVTN_MTH_TM_ID
       AND mdf.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
       AND mdf.SRC_SYS_CD = 'SPL'
    LEFT JOIN features.LAST_NEW_DFT_DT dt
        ON dt.BASEL_ACCT_ID = c.BASEL_ACCT_ID
       AND dt.OBSVTN_MTH_TM_ID = c.OBSVTN_MTH_TM_ID
       AND dt.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
       AND dt.SRC_SYS_CD = 'SPL'
    LEFT JOIN features.LAST_NEW_DFT_BAL_AMT bal
        ON bal.BASEL_ACCT_ID = c.BASEL_ACCT_ID
       AND bal.OBSVTN_MTH_TM_ID = c.OBSVTN_MTH_TM_ID
       AND bal.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
       AND bal.SRC_SYS_CD = 'SPL'
    """,
):
    pass
