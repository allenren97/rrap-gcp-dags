from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

# Current-state last-new-default BALANCE (os_bal_amt at the onset month).
#
# CIS_DATA_POP is a current-month population, so it needs the account's default state
# AS OF the run month -- not the completed-outcome LGD/PD training windows. So we read
# features.MONTH_DEF at the run month and back-calculate the onset of the current
# default spell:
#     SPL onset <=> MONTH_DEF = 1 (inclusive)  -> onset_tm_id = mth_tm_id - (MONTH_DEF-1)*40
#     KS  onset <=> MONTH_DEF = 0 (exclusive)  -> onset_tm_id = mth_tm_id -  MONTH_DEF   *40
# A month = 40 TM_ID units. Only in-default accounts emit a row; custuniv treats a
# missing/null obsvtn row as not-default. OBSVTN_MTH_TM_ID = the run month.
#   last_new_dft_dt      = month-end of the onset month
#   last_new_dft_bal_amt = features.OS_BAL_AMT at the onset month
#   model_dft_f          = 'Y' (row exists only for in-default accounts)
UPSTREAM_ASSET = [
    "features.MONTH_DEF",
    "features.OS_BAL_AMT",
    "ingestion.TM_DIM",
]
DOWNSTREAM_ASSET = "features.LAST_NEW_DFT_BAL_AMT"
DEPENDENCIES = {
    "duckdb_delete": ["export_spl", "export_ks"],
    "export_spl": ["duckdb_load"],
    "export_ks": ["duckdb_load"],
}


def duckdb_delete(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    delete from {DOWNSTREAM_ASSET} where obsn_dt = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """,
):
    pass


def export_spl(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    WITH
        md AS (
            SELECT BASEL_ACCT_ID, MONTH_DEF
            FROM features.MONTH_DEF
            WHERE SRC_SYS_CD = 'SPL'
              AND OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
              AND MONTH_DEF >= 1
            QUALIFY ROW_NUMBER() OVER (PARTITION BY BASEL_ACCT_ID ORDER BY MONTH_DEF DESC) = 1
        ),
        onset AS (
            SELECT
                BASEL_ACCT_ID,
                {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}} - (MONTH_DEF - 1) * 40 AS ONSET_TM_ID
            FROM md
        ),
        onset_dt AS (
            SELECT
                o.BASEL_ACCT_ID,
                tm.TM_LVL_END_DT AS LAST_NEW_DFT_DT,
                ob.OS_BAL_AMT AS LAST_NEW_DFT_BAL_AMT
            FROM onset o
            INNER JOIN ingestion.TM_DIM tm
                ON tm.TM_ID = o.ONSET_TM_ID AND TRIM(tm.TM_LVL) = 'Month'
            LEFT JOIN (
                SELECT BASEL_ACCT_ID, OBSN_DT, OS_BAL_AMT
                FROM features.OS_BAL_AMT
                WHERE SRC_SYS_CD = 'SPL'
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY BASEL_ACCT_ID, OBSN_DT ORDER BY OS_BAL_AMT DESC NULLS LAST
                ) = 1
            ) ob
                ON ob.BASEL_ACCT_ID = o.BASEL_ACCT_ID AND ob.OBSN_DT = tm.TM_LVL_END_DT
        )
    SELECT
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        od.BASEL_ACCT_ID,
        {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}} AS OBSVTN_MTH_TM_ID,
        od.LAST_NEW_DFT_BAL_AMT,
        'SPL' AS SRC_SYS_CD
    FROM onset_dt od
    """,
):
    pass


def export_ks(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    WITH
        md AS (
            SELECT BASEL_ACCT_ID, MONTH_DEF
            FROM features.MONTH_DEF
            WHERE SRC_SYS_CD = 'KS'
              AND OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
              AND MONTH_DEF IS NOT NULL
            QUALIFY ROW_NUMBER() OVER (PARTITION BY BASEL_ACCT_ID ORDER BY MONTH_DEF DESC) = 1
        ),
        onset AS (
            SELECT
                BASEL_ACCT_ID,
                {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}} - MONTH_DEF * 40 AS ONSET_TM_ID
            FROM md
        ),
        onset_dt AS (
            SELECT
                o.BASEL_ACCT_ID,
                tm.TM_LVL_END_DT AS LAST_NEW_DFT_DT,
                ob.OS_BAL_AMT AS LAST_NEW_DFT_BAL_AMT
            FROM onset o
            INNER JOIN ingestion.TM_DIM tm
                ON tm.TM_ID = o.ONSET_TM_ID AND TRIM(tm.TM_LVL) = 'Month'
            LEFT JOIN (
                SELECT BASEL_ACCT_ID, OBSN_DT, OS_BAL_AMT
                FROM features.OS_BAL_AMT
                WHERE SRC_SYS_CD = 'KS'
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY BASEL_ACCT_ID, OBSN_DT ORDER BY OS_BAL_AMT DESC NULLS LAST
                ) = 1
            ) ob
                ON ob.BASEL_ACCT_ID = o.BASEL_ACCT_ID AND ob.OBSN_DT = tm.TM_LVL_END_DT
        )
    SELECT
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        od.BASEL_ACCT_ID,
        {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}} AS OBSVTN_MTH_TM_ID,
        od.LAST_NEW_DFT_BAL_AMT,
        'KS' AS SRC_SYS_CD
    FROM onset_dt od
    """,
):
    pass


def duckdb_load(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} by name
    FROM (
        select * from read_parquet([
            '{{{{ task_instance.xcom_pull(task_ids="derived__last_new_dft_bal_amt.export_spl", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__last_new_dft_bal_amt.export_ks", key="parquet") }}}}'],
        union_by_name = true)
    )
    """,
):
    pass
