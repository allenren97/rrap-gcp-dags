from airflow.sdk import get_current_context
from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

UPSTREAM_ASSET = [
    "ingestion.MORT_MTH_SNAPSHOT",
    "features.MAX_LEND_VALUE",
    "features.STEP_PRIM_CUST_ID",
]
DOWNSTREAM_ASSET = "features.STEP_LTV_MIN6M_ACCT"
DEPENDENCIES = {
    "export_first_order": ["duckdb_delete_step_ltv_min6m_acct"],
    "duckdb_delete_step_ltv_min6m_acct": ["duckdb_load_step_ltv_min6m_acct"],
}

"""
Note on anti-pattern logic:
This variable derives everything in the "2nd order" as calculating a "1st order" STEP_LTV is not useful,
the calculation of STEP_LTV for each month in the 6 month window needed for a STEP's LTV involves
summing up values of only the current run month valid accounts for that STEP, which cannot be done in the "past"
"""


def export_first_order(
    duckdb_conn_id="duckdb-conn",
    sql=r"""
        WITH
        mort_6m AS (
            SELECT
                MTH_TM_ID,
                mort.BASEL_ACCT_ID,
                CRNT_BAL_AMT,
                INTR_ACCR_AMT,
                TRIM(mort.STEP_PLN_AGRMNT_NUM) AS STEP_PLN_AGRMNT_NUM,
                mort.PRIM_BASEL_CUST_ID,
                mort.mort_num
            FROM
                ingestion.MORT_MTH_SNAPSHOT AS mort
                
            WHERE
                mort.MTH_TM_ID BETWEEN {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }} - 5 * 40 AND {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}
                AND UPPER(TRIM(COMM_TP)) = 'RESIDENTIAL'
                AND CRNT_BAL_AMT > 0
                AND TRIM(PD_OFF_F) = 'N'
        ),
        accounts AS (
            SELECT
                m.step_pln_agrmnt_num,
                m.mort_num,
                step_prim_cust_id
            FROM
                mort_6m AS m
                LEFT JOIN features.step_prim_cust_id AS spci ON spci.step_pln_agrmnt_num = m.step_pln_agrmnt_num
            WHERE
                MTH_TM_ID = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}
                AND step_prim_cust_id > 0
            GROUP BY
                m.step_pln_agrmnt_num,
                m.mort_num,
                step_prim_cust_id
        ),
        max_lend AS (
            SELECT
                mort.MTH_TM_ID,
                mort.CRNT_BAL_AMT,
                mort.INTR_ACCR_AMT,
                acc.STEP_PLN_AGRMNT_NUM,
                acc.step_prim_cust_id AS PRIM_BASEL_CUST_ID,
                mlv.MORT_AUTH_DT,
                mlv.MAX_LEND_VALUE,
                mort.mort_num
            FROM
                mort_6m AS mort
                INNER JOIN accounts AS acc ON acc.mort_num = mort.mort_num
                LEFT JOIN features.max_lend_value AS mlv ON mlv.STEP_PLN_AGRMNT_NUM = acc.STEP_PLN_AGRMNT_NUM
                AND mlv.obsn_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
        )
    SELECT
        STEP_PLN_AGRMNT_NUM,
        PRIM_BASEL_CUST_ID,
        MTH_TM_ID,
        (
            (SUM(CRNT_BAL_AMT) + SUM(INTR_ACCR_AMT)) / NULLIF(MAX(MAX_LEND_VALUE), 0)
        ) AS STEP_LTV
    FROM
        max_lend
    GROUP BY
        STEP_PLN_AGRMNT_NUM,
        PRIM_BASEL_CUST_ID,
        MTH_TM_ID
    """,
):
    pass


def duckdb_delete_step_ltv_min6m_acct(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        DELETE FROM {DOWNSTREAM_ASSET}
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """,
):
    pass


def duckdb_load_step_ltv_min6m_acct(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} BY NAME
    FROM(
        SELECT 
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate")  }}}}' AS OBSN_DT,
            STEP_PLN_AGRMNT_NUM, 
            trunc(MIN(STEP_LTV),6) AS STEP_LTV_MIN6M_ACCT 
        FROM read_parquet('{{{{ task_instance.xcom_pull(task_ids="derived__step_ltv_min6m_acct.export_first_order", key="parquet") }}}}')
        GROUP BY STEP_PLN_AGRMNT_NUM
    )
    """,
):
    pass
