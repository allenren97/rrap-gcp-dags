from airflow.sdk import get_current_context
from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

UPSTREAM_ASSET = ["ingestion.AIRB_MORT_MTH_SNAPSHOT", 
                  "ingestion.BASEL_MORT_MTH_SNAPSHOT"]
DOWNSTREAM_ASSET = "features.STEP_LEND_VALUE"
DEPENDENCIES = {
    "duckdb_delete_step_lend_value": ["duckdb_load_step_lend_value"],
}


def duckdb_delete_step_lend_value(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        DELETE FROM {DOWNSTREAM_ASSET}
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """,
):
    pass

def duckdb_load_step_lend_value(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        INSERT INTO {DOWNSTREAM_ASSET} BY NAME
            WITH Merged AS (-- pure join and filter logic; will need to be switched to the MORT_MTH_SNAPSHOT view eventually
                SELECT
                    b.STEP_PLN_AGRMNT_NUM,
                    b.PRIM_BASEL_CUST_ID,
                    b.MTH_TM_ID AS TM_ID,
                    CAST(TRIM(b.MORT_NUM)AS BIGINT) AS MORT_NUM,
                    a.MADE_DT,
                    a.LEND_VALUE
                FROM {UPSTREAM_ASSET[1]} b
                JOIN {UPSTREAM_ASSET[0]} a
                ON a.MORT_NUM = CAST(TRIM(b.MORT_NUM) AS BIGINT)
                AND a.TM_ID = b.MTH_TM_ID
                WHERE b.STEP_PLN_AGRMNT_NUM = 210461448
                AND b.MTH_TM_ID BETWEEN ({{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}} - 11*40) 
                AND {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
                AND UPPER(TRIM(a.COMM_TP)) = 'RESIDENTIAL'
                AND a.CRNT_BAL > 0
                AND TRIM(a.PD_OFF_F) = 'N'
            ),
            MostRecent AS (
                SELECT
                    STEP_PLN_AGRMNT_NUM,
                    PRIM_BASEL_CUST_ID,
                    TM_ID,
                    MAX(MADE_DT) AS MOST_RECENT_ORIG_DATE
                FROM Merged
                GROUP BY STEP_PLN_AGRMNT_NUM, PRIM_BASEL_CUST_ID, TM_ID
            ),
            -- IMPORTANT: global at-date fetch (no month filter, no filter), MAX at date
            AtDate_Global AS (
                SELECT
                    r.STEP_PLN_AGRMNT_NUM,
                    r.PRIM_BASEL_CUST_ID,
                    r.TM_ID AS MTH_TM_ID,
                    MAX(a2.LEND_VALUE) AS LEND_VALUE_AT_DATE,
                    m0.MORT_NUM
                FROM MostRecent r
                JOIN Merged m0
                    ON m0.STEP_PLN_AGRMNT_NUM = r.STEP_PLN_AGRMNT_NUM
                    AND m0.PRIM_BASEL_CUST_ID = r.PRIM_BASEL_CUST_ID
                    AND m0.TM_ID = r.TM_ID
                    AND m0.MADE_DT = r.MOST_RECENT_ORIG_DATE
                -- RE-JOIN AIRB globally (no TM_ID filter, no CRNT_BAL/PD_OFF filters)
                JOIN {UPSTREAM_ASSET[0]} a2
                    ON a2.MORT_NUM = m0.MORT_NUM
                    AND a2.MADE_DT = r.MOST_RECENT_ORIG_DATE
                GROUP BY r.STEP_PLN_AGRMNT_NUM, r.PRIM_BASEL_CUST_ID, r.TM_ID, m0.MORT_NUM
            )
            -- STEP-level monthly from rejoining
                SELECT
                    '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate")  }}}}' AS OBSN_DT,
                    STEP_PLN_AGRMNT_NUM,
                    MORT_NUM,
                    MAX(LEND_VALUE_AT_DATE) AS STEP_LEND_VALUE
                FROM AtDate_Global
                GROUP BY STEP_PLN_AGRMNT_NUM, MORT_NUM
    """,
):
    pass

