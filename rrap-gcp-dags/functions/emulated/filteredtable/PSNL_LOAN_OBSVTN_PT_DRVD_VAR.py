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
    "ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT",
    "features.MODEL_DFT_F",
    "features.LAST_NEW_DFT_DT",
    "features.LAST_NEW_DFT_BAL_AMT",
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


# Full observation cohort, mirroring SAS J_RRAP_TL10_2201: the obsvtn table is the
# personal-loan snapshot at the observation month (:2597,:5758) with MODEL_DFT_F='Y'
# only for accounts that have a new default (LAST_NEW_DEF_DATE), else 'N' (:5726).
# The features supply the 'Y' rows (defaulters); the cohort supplies the 'N' rows.
# Cohort per window: SPL accounts in the snapshot at OBSVTN month (R-12 PDEAD /
# R-24 LGD), RECD_STAT_CD in (4,5,6,7,8), one row per BASEL_ACCT_ID.
def duckdb_load(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} BY NAME
    WITH cohort AS (
        SELECT DISTINCT BASEL_ACCT_ID,
            {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}} - 12 * 40 AS OBSVTN_MTH_TM_ID
        FROM ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT
        WHERE MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}} - 12 * 40
          AND RECD_STAT_CD IN (4, 5, 6, 7, 8)
        UNION ALL
        SELECT DISTINCT BASEL_ACCT_ID,
            {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}} - 24 * 40 AS OBSVTN_MTH_TM_ID
        FROM ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT
        WHERE MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}} - 24 * 40
          AND RECD_STAT_CD IN (4, 5, 6, 7, 8)
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
        CASE WHEN mdf.BASEL_ACCT_ID IS NOT NULL THEN 'Y' ELSE 'N' END AS MODEL_DFT_F,
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
