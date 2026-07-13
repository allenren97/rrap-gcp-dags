from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)


UPSTREAM_ASSET = ["ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT"]
DOWNSTREAM_ASSET = "features.STEP_TOT_CR_BAL_DPD30_90_KSA"
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
    INSERT INTO {DOWNSTREAM_ASSET} by name
    SELECT
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        t.STEP_PLN_AGRMNT_NUM,
        SUM(t.TOT_CR_BAL_DPD30_90) AS STEP_TOT_CR_BAL_DPD30_90_KSA
    FROM (
        SELECT  
            TRIM(revl.STEP_PLN_AGRMNT_NUM) AS STEP_PLN_AGRMNT_NUM,
            revl.PRIM_BASEL_CUST_ID,
            revl.BASEL_ACCT_ID,
            MTH_TM_ID,
            CASE 
                WHEN revl.BNS_DLQNT_DAY > 29 
                AND revl.BNS_DLQNT_DAY < 90 
                THEN revl.TOT_NEW_BAL_AMT
                ELSE 0
            END AS TOT_CR_BAL_DPD30_90
            FROM
                {UPSTREAM_ASSET[0]} revl
            WHERE 
                revl.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
                AND revl.PRIM_BASEL_CUST_ID IS NOT NULL
                AND revl.PRIM_BASEL_CUST_ID <> -1
                AND TRIM(revl.STEP_PLN_AGRMNT_NUM) != ''
        ) t
    GROUP BY t.STEP_PLN_AGRMNT_NUM
    """,
):
    pass
