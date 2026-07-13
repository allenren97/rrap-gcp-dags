from airflow.sdk import get_current_context
from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

UPSTREAM_ASSET = ["features.STEP_TOT_CR_BAL_DPD30_90_KSA", ]
DOWNSTREAM_ASSET = "features.STEP_TOT_CR_BAL_DPD30_90_KSA_AVG3M"
DEPENDENCIES = {
    "duckdb_delete_step_tot_cr_bal_dpd30_90_ksa_avg3m": ["duckdb_load_step_tot_cr_bal_dpd30_90_ksa_avg3m"],
}


def duckdb_delete_step_tot_cr_bal_dpd30_90_ksa_avg3m(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        DELETE FROM {DOWNSTREAM_ASSET}
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """,
):
    pass

def duckdb_load_step_tot_cr_bal_dpd30_90_ksa_avg3m(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    INSERT INTO { DOWNSTREAM_ASSET } BY NAME
    WITH base AS (
        SELECT 
            STEP_PLN_AGRMNT_NUM,
            (AVG(STEP_TOT_CR_BAL_DPD30_90_KSA))::DECIMAL(31,3) AS STEP_TOT_CR_BAL_DPD30_90_KSA_AVG3M
        FROM 
            {UPSTREAM_ASSET[0]}
        WHERE 
            OBSN_DT BETWEEN 
                DATE_ADD(DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}', - INTERVAL 2 MONTH)
                AND
                '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
        GROUP BY 
            STEP_PLN_AGRMNT_NUM
    )
    SELECT 
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate")  }}}}' AS OBSN_DT,
        STEP_PLN_AGRMNT_NUM, 
        STEP_TOT_CR_BAL_DPD30_90_KSA_AVG3M 
    FROM base
    """,
):
    pass

