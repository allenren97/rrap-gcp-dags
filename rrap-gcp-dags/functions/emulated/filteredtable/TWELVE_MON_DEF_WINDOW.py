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
        mn.MORTGAGE_NO,
        obs.TM_LVL_END_DT AS PROCESS_DATE,
        dt.DEFAULT_DATE,
        bal.DEFAULT_BAL,
        ind.DEFAULT_IND,
        CURRENT_TIMESTAMP AS INSRT_PROCESS_TMSTMP,
        CURRENT_TIMESTAMP AS UPDT_PROCESS_TMSTMP
    FROM features.DEFAULT_IND ind
    INNER JOIN (
        SELECT TM_ID, TM_LVL_END_DT
        FROM ingestion.TM_DIM
        WHERE TRIM(TM_LVL) = 'Month'
        QUALIFY ROW_NUMBER() OVER (PARTITION BY TM_ID ORDER BY TM_LVL_END_DT) = 1
    ) obs ON obs.TM_ID = ind.OBSVTN_MTH_TM_ID
    INNER JOIN (
        SELECT BASEL_ACCT_ID, OBSN_DT, TRY_CAST(MORT_NUM AS BIGINT) AS MORTGAGE_NO
        FROM features.MORT_NUM
        WHERE SRC_SYS_CD = 'MOR'
          AND TRY_CAST(MORT_NUM AS BIGINT) IS NOT NULL
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY BASEL_ACCT_ID, OBSN_DT ORDER BY MORT_NUM DESC NULLS LAST
        ) = 1
    ) mn ON mn.BASEL_ACCT_ID = ind.BASEL_ACCT_ID
        AND mn.OBSN_DT = obs.TM_LVL_END_DT
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
