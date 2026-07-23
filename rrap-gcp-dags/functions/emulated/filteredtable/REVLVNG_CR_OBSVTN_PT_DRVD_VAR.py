"""
Rewrite of J_RRII_KS10_2510_REVLVNG_CR_OBSVTN_PT_DRVD_VAR.sas.

Thin join over the observation-point features (KS branch). Projects the default
features (LAST_NEW_DFT_DT, LAST_NEW_DFT_BAL_AMT, MODEL_DFT_F).

Two month ids, matching production (SAS `a.MTH_TM_ID AS OBSVTN_MTH_TM_ID`,
`&mth_tm_id AS process_mth_tm_id`):
  PROCESS_MTH_TM_ID / PROCESS_DATE  = the RUN month R (delete/replace key)
  OBSVTN_MTH_TM_ID                  = the OBSERVATION month = R - 12 months
                                      (PDEAD/CUR observation; 40 tm_id = 1 month)

NOTE: custuniv joins the obsvtn tables on OBSVTN_MTH_TM_ID, so with OBSVTN = R-12
the row for reporting month M is the one produced by the run at R = M+12. For a
same-month pipeline, custuniv would instead need to join on PROCESS_MTH_TM_ID.
The LAST_NEW_DFT_* / MODEL_DFT_F values are still the current-state feature
derivation (forward-window value parity is a separate reconciliation item).
"""

UPSTREAM_ASSET = [
    "features.MODEL_DFT_F",
    "features.LAST_NEW_DFT_DT",
    "features.LAST_NEW_DFT_BAL_AMT",
]

DOWNSTREAM_ASSET = "emulated.REVLVNG_CR_OBSVTN_PT_DRVD_VAR"

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


def duckdb_load(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} BY NAME
    SELECT
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}' AS STREAM,
        mdf.BASEL_ACCT_ID,
        mdf.OBSVTN_MTH_TM_ID - 12 * 40 AS OBSVTN_MTH_TM_ID,
        mdf.OBSVTN_MTH_TM_ID AS PROCESS_MTH_TM_ID,
        DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS PROCESS_DATE,
        dt.LAST_NEW_DFT_DT,
        bal.LAST_NEW_DFT_BAL_AMT,
        mdf.MODEL_DFT_F,
        CURRENT_TIMESTAMP AS INSRT_PROCESS_TMSTMP,
        CURRENT_TIMESTAMP AS UPDT_PROCESS_TMSTMP
    FROM features.MODEL_DFT_F mdf
    LEFT JOIN features.LAST_NEW_DFT_DT dt
        ON dt.BASEL_ACCT_ID = mdf.BASEL_ACCT_ID
       AND dt.OBSVTN_MTH_TM_ID = mdf.OBSVTN_MTH_TM_ID
       AND dt.OBSN_DT = mdf.OBSN_DT
       AND dt.SRC_SYS_CD = mdf.SRC_SYS_CD
    LEFT JOIN features.LAST_NEW_DFT_BAL_AMT bal
        ON bal.BASEL_ACCT_ID = mdf.BASEL_ACCT_ID
       AND bal.OBSVTN_MTH_TM_ID = mdf.OBSVTN_MTH_TM_ID
       AND bal.OBSN_DT = mdf.OBSN_DT
       AND bal.SRC_SYS_CD = mdf.SRC_SYS_CD
    WHERE mdf.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
      AND mdf.SRC_SYS_CD = 'KS'
    """,
):
    pass
