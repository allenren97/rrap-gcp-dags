"""
Rewrite of RRAP_MOR_MODEL_01_DEFINE_STATUS_G.sas + RRAP_MOR_MODEL_01A_GATHER_LOAD_G.sas.

Thin feature-join assembly of emulated.STATUS_FINAL — no emulated.MORTGAGE_HIST.
Every column now comes from features (all keyed on BASEL_ACCT_ID + OBSN_DT, SRC_SYS_CD='MOR'):
  MORTGAGE_NO    — features.MORT_NUM
  STATUS         — features.PIT_STATUS_CROSS_DEFAULT_ORIG
  CURRENT_BAL    — features.CURRENT_BAL      (raw CRNT_BAL_AMT)
  TOTAL_SUSPENSE — features.TOTAL_SUSPENSE   (TOT_SUSP_BAL_AMT)
  LRA_STATUS     — features.LRA_STATUS       (LRA_STAT)
  PAID_OFF_DATE  — features.PAID_OFF_DATE    (PD_OFF_DT)
  MODEL_EXCL     — derived from STATUS + CURRENT_BAL + features.PD_OFF_F
  PROCESS_DATE   — the process month-end (rundate)
"""

UPSTREAM_ASSET = [
    "features.MORT_NUM",
    "features.PIT_STATUS_CROSS_DEFAULT_ORIG",
    "features.CURRENT_BAL",
    "features.TOTAL_SUSPENSE",
    "features.LRA_STATUS",
    "features.PAID_OFF_DATE",
    "features.PD_OFF_F",
]

DOWNSTREAM_ASSET = "emulated.STATUS_FINAL"

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
        {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}} AS MTH_TM_ID,
        TRY_CAST(mn.MORT_NUM AS BIGINT) AS MORTGAGE_NO,
        pit.PIT_STATUS_CROSS_DEFAULT_ORIG AS STATUS,
        CASE
            WHEN UPPER(TRIM(COALESCE(pof.PD_OFF_F, ''))) = 'Y'
                 OR UPPER(TRIM(COALESCE(pit.PIT_STATUS_CROSS_DEFAULT_ORIG, ''))) <> 'CUR'
                 OR COALESCE(cb.CURRENT_BAL, 0) <= 0
            THEN 'Y'
            ELSE 'N'
        END AS MODEL_EXCL,
        lra.LRA_STATUS,
        pod.PAID_OFF_DATE,
        cb.CURRENT_BAL,
        ts.TOTAL_SUSPENSE,
        CAST('{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS TIMESTAMP) AS PROCESS_DATE,
        CURRENT_TIMESTAMP AS INSRT_PROCESS_TMSTMP,
        CURRENT_TIMESTAMP AS UPDT_PROCESS_TMSTMP
    FROM (
        SELECT BASEL_ACCT_ID, MORT_NUM
        FROM features.MORT_NUM
        WHERE SRC_SYS_CD = 'MOR'
          AND OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY BASEL_ACCT_ID ORDER BY MORT_NUM DESC NULLS LAST
        ) = 1
    ) mn
    LEFT JOIN (
        SELECT BASEL_ACCT_ID, PIT_STATUS_CROSS_DEFAULT_ORIG
        FROM features.PIT_STATUS_CROSS_DEFAULT_ORIG
        WHERE SRC_SYS_CD = 'MOR'
          AND OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY BASEL_ACCT_ID ORDER BY PIT_STATUS_CROSS_DEFAULT_ORIG DESC NULLS LAST
        ) = 1
    ) pit ON pit.BASEL_ACCT_ID = mn.BASEL_ACCT_ID
    LEFT JOIN (
        SELECT BASEL_ACCT_ID, CURRENT_BAL
        FROM features.CURRENT_BAL
        WHERE SRC_SYS_CD = 'MOR'
          AND OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY BASEL_ACCT_ID ORDER BY CURRENT_BAL DESC NULLS LAST
        ) = 1
    ) cb ON cb.BASEL_ACCT_ID = mn.BASEL_ACCT_ID
    LEFT JOIN (
        SELECT BASEL_ACCT_ID, TOTAL_SUSPENSE
        FROM features.TOTAL_SUSPENSE
        WHERE SRC_SYS_CD = 'MOR'
          AND OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY BASEL_ACCT_ID ORDER BY TOTAL_SUSPENSE DESC NULLS LAST
        ) = 1
    ) ts ON ts.BASEL_ACCT_ID = mn.BASEL_ACCT_ID
    LEFT JOIN (
        SELECT BASEL_ACCT_ID, LRA_STATUS
        FROM features.LRA_STATUS
        WHERE SRC_SYS_CD = 'MOR'
          AND OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY BASEL_ACCT_ID ORDER BY LRA_STATUS DESC NULLS LAST
        ) = 1
    ) lra ON lra.BASEL_ACCT_ID = mn.BASEL_ACCT_ID
    LEFT JOIN (
        SELECT BASEL_ACCT_ID, PAID_OFF_DATE
        FROM features.PAID_OFF_DATE
        WHERE SRC_SYS_CD = 'MOR'
          AND OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY BASEL_ACCT_ID ORDER BY PAID_OFF_DATE DESC NULLS LAST
        ) = 1
    ) pod ON pod.BASEL_ACCT_ID = mn.BASEL_ACCT_ID
    LEFT JOIN (
        SELECT BASEL_ACCT_ID, PD_OFF_F
        FROM features.PD_OFF_F
        WHERE SRC_SYS_CD = 'MOR'
          AND OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY BASEL_ACCT_ID ORDER BY PD_OFF_F DESC NULLS LAST
        ) = 1
    ) pof ON pof.BASEL_ACCT_ID = mn.BASEL_ACCT_ID
    """,
):
    pass
