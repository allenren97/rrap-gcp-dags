"""
Rewrite of J_RRII_KS10_2510_REVLVNG_CR_OBSVTN_PT_DRVD_VAR.sas.

Thin join over the observation-point features (KS branch). The PDEAD/LGD window
derivation now lives in the features (LAST_NEW_DFT_DT, LAST_NEW_DFT_BAL_AMT,
MODEL_DFT_F), so this table just projects them keyed by
BASEL_ACCT_ID + OBSVTN_MTH_TM_ID for SRC_SYS_CD = 'KS'.
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
        mdf.OBSVTN_MTH_TM_ID,
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
