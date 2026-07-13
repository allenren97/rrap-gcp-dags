from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)


UPSTREAM_ASSET = [
    "ingestion.MORT_MTH_SNAPSHOT",
    "ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT",
    "ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT" 
]
DOWNSTREAM_ASSET = "features.STEP_ACCT_OP_DT_REVL"
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
                INTERSECT ALL
                SELECT BASEL_ACCT_ID FROM features.WRITTEN_OUT_F
                WHERE WRITTEN_OUT_F = 'N' AND OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
                INTERSECT ALL
                SELECT BASEL_ACCT_ID FROM features.TREATMENT_F
                WHERE TREATMENT_F = 'A' AND OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
            ), step_accts AS (
                SELECT MAX(ACCT_OPND_DT) AS ACCT_OPND_DT, TRIM(STEP_PLN_AGRMNT_NUM) AS STEP_PLN_AGRMNT_NUM
                FROM ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT ss
                JOIN non_excl on ss.BASEL_ACCT_ID = non_excl.BASEL_ACCT_ID
                WHERE MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
                    AND STEP_PLN_AGRMNT_NUM IS NOT NULL AND TRIM(STEP_PLN_AGRMNT_NUM) != ''
                GROUP BY TRIM(STEP_PLN_AGRMNT_NUM)
                UNION ALL
                SELECT NULL AS ACCT_OPND_DT, TRIM(STEP_PLN_AGRMNT_NUM) AS STEP_PLN_AGRMNT_NUM
                FROM ingestion.MORT_MTH_SNAPSHOT ss
                JOIN non_excl on ss.BASEL_ACCT_ID = non_excl.BASEL_ACCT_ID
                WHERE MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
                    AND STEP_PLN_AGRMNT_NUM IS NOT NULL AND TRIM(STEP_PLN_AGRMNT_NUM) != ''
                GROUP BY TRIM(STEP_PLN_AGRMNT_NUM)
                UNION ALL
                SELECT NULL AS ACCT_OPND_DT, TRIM(STEP_PLN_AGRMNT_NUM) AS STEP_PLN_AGRMNT_NUM
                FROM ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT ss
                JOIN non_excl on ss.BASEL_ACCT_ID = non_excl.BASEL_ACCT_ID
                WHERE MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
                    AND STEP_PLN_AGRMNT_NUM IS NOT NULL AND TRIM(STEP_PLN_AGRMNT_NUM) != ''
                GROUP BY TRIM(STEP_PLN_AGRMNT_NUM)
            ) SELECT 
                '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT, 
                STEP_PLN_AGRMNT_NUM,
                MAX(ACCT_OPND_DT) AS STEP_ACCT_OP_DT_REVL
            FROM step_accts
            GROUP BY STEP_PLN_AGRMNT_NUM
        )
    """,
):
    pass


