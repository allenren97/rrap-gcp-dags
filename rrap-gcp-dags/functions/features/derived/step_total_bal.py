from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)


UPSTREAM_ASSET = [
    "ingestion.MORT_MTH_SNAPSHOT",
    "ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT",
    "ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT",
    "features.TOTAL_BALANCE",
    "features.OS_BAL_AMT_V2",
    "features.MODEL_EXCL_F",
    "features.WRITTEN_OUT_F",
    "features.TREATMENT_F"
]
DOWNSTREAM_ASSET = "features.STEP_TOTAL_BAL"
DEPENDENCIES = {
    "duckdb_delete": ["duckdb_load"],
}


def duckdb_delete(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        DELETE FROM {DOWNSTREAM_ASSET}
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """,
):
    pass


def duckdb_load(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        INSERT INTO {DOWNSTREAM_ASSET} BY NAME
        FROM (
            WITH non_excl AS (
                SELECT BASEL_ACCT_ID FROM features.MODEL_EXCL_F 
                WHERE MODEL_EXCL_F = 'N' AND OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
                INTERSECT
                SELECT BASEL_ACCT_ID FROM features.WRITTEN_OUT_F
                WHERE WRITTEN_OUT_F = 'N' AND OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
                INTERSECT
                SELECT BASEL_ACCT_ID FROM features.TREATMENT_F
                WHERE TREATMENT_F = 'A' AND OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
            ), revl AS (
                SELECT TRIM(STEP_PLN_AGRMNT_NUM) AS STEP_PLN_AGRMNT_NUM, TOT_NEW_BAL_AMT AS TOTAL_BAL
                FROM ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT ss
                RIGHT JOIN non_excl ON ss.BASEL_ACCT_ID = non_excl.BASEL_ACCT_ID
                WHERE ss.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
                    AND STEP_PLN_AGRMNT_NUM IS NOT NULL AND TRIM(STEP_PLN_AGRMNT_NUM) != ''
            ), mort AS (
                SELECT TRIM(STEP_PLN_AGRMNT_NUM) AS STEP_PLN_AGRMNT_NUM, TOTAL_BALANCE AS TOTAL_BAL
                FROM non_excl
                JOIN ingestion.MORT_MTH_SNAPSHOT ss ON ss.BASEL_ACCT_ID = non_excl.BASEL_ACCT_ID
                JOIN features.TOTAL_BALANCE tb ON tb.BASEL_ACCT_ID = non_excl.BASEL_ACCT_ID
                WHERE ss.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
                    AND tb.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
                    AND STEP_PLN_AGRMNT_NUM IS NOT NULL AND TRIM(STEP_PLN_AGRMNT_NUM) != ''
            ), psnl AS (
                SELECT
                    TRIM(STEP_PLN_AGRMNT_NUM) AS STEP_PLN_AGRMNT_NUM,
                    OS_BAL_AMT_V2 AS TOTAL_BAL
                FROM non_excl
                JOIN ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT ss ON ss.BASEL_ACCT_ID = non_excl.BASEL_ACCT_ID
                JOIN features.OS_BAL_AMT_V2 a ON a.BASEL_ACCT_ID = ss.BASEL_ACCT_ID
                WHERE ss.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
                    AND a.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
                    AND STEP_PLN_AGRMNT_NUM IS NOT NULL AND TRIM(STEP_PLN_AGRMNT_NUM) != ''  
            )
            SELECT
                '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
                STEP_PLN_AGRMNT_NUM,
                SUM(TOTAL_BAL) AS STEP_TOTAL_BAL
            FROM
                (SELECT * FROM revl
                UNION ALL
                SELECT * FROM mort
                UNION ALL
                SELECT * FROM psnl
                )
            GROUP BY STEP_PLN_AGRMNT_NUM
        )
    """,
):
    pass


