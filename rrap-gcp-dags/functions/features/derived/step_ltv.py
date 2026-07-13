from airflow.sdk import get_current_context
from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

UPSTREAM_ASSET = ["ingestion.MORT_MTH_SNAPSHOT",
                  "features.MAX_LEND_VALUE",
                  "features.MODEL_EXCL_F",
                  "features.WRITTEN_OUT_F",
                  "features.PIT_STATUS_STEP"]
DOWNSTREAM_ASSET = "features.STEP_LTV"
DEPENDENCIES = {
    "duckdb_delete_step_ltv": ["duckdb_load_step_ltv"],
}


def duckdb_delete_step_ltv(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        DELETE FROM {DOWNSTREAM_ASSET}
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """,
):
    pass

def duckdb_load_step_ltv(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        INSERT INTO {DOWNSTREAM_ASSET} BY NAME
            WITH Mapped AS (
            SELECT
                MORT_AUTH_DT AS MADE_DT,
                MTH_TM_ID,
                LND_VAL AS LEND_VALUE,
                CRNT_BAL_AMT AS CRNT_BAL,
                INTR_ACCR_AMT AS INEREST_ACCR_AMT,
                mort.STEP_PLN_AGRMNT_NUM
            FROM {UPSTREAM_ASSET[0]} mort
            INNER JOIN (SELECT * FROM {UPSTREAM_ASSET[2]} WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate")  }}}}') exclsn ON
                mort.BASEL_ACCT_ID = exclsn.BASEL_ACCT_ID
            INNER JOIN (SELECT * FROM {UPSTREAM_ASSET[3]} WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate")  }}}}') written ON
                mort.BASEL_ACCT_ID = written.BASEL_ACCT_ID
            LEFT JOIN (SELECT * FROM {UPSTREAM_ASSET[4]} WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate")  }}}}') step_pit ON
                mort.BASEL_ACCT_ID = step_pit.BASEL_ACCT_ID
            WHERE UPPER(TRIM(COMM_TP)) = 'RESIDENTIAL'
            AND CRNT_BAL_AMT > 0
            AND TRIM(PD_OFF_F) = 'N'
            AND TRIM(exclsn.MODEL_EXCL_F) = 'N'
            AND TRIM(written.WRITTEN_OUT_F) = 'N'
            AND UPPER(TRIM(step_pit.PIT_STATUS_STEP)) = 'CUR'
            AND MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
        )

        SELECT
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate")  }}}}' AS OBSN_DT,
            t.STEP_PLN_AGRMNT_NUM,
            ((SUM(CRNT_BAL) + SUM(INEREST_ACCR_AMT)) / NULLIF(ANY_VALUE(lnd.MAX_LEND_VALUE), 0)) AS STEP_LTV
        FROM (SELECT 
                STEP_PLN_AGRMNT_NUM,
                CRNT_BAL,
                INEREST_ACCR_AMT,
                MADE_DT
            FROM Mapped) t 
        LEFT JOIN 
            (SELECT * FROM {UPSTREAM_ASSET[1]} 
            WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate")  }}}}') 
        lnd ON
            t.STEP_PLN_AGRMNT_NUM = lnd.STEP_PLN_AGRMNT_NUM
            AND t.MADE_DT = lnd.MORT_AUTH_DT
        WHERE t.STEP_PLN_AGRMNT_NUM IS NOT NULL
        GROUP BY t.STEP_PLN_AGRMNT_NUM
    """,
):
    pass

