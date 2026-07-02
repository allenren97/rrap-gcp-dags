from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)


UPSTREAM_ASSET = [
    "ingestion.MORT_MTH_SNAPSHOT",
    "ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT",
    "ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT",
    "features.MODEL_EXCL_F",
    "features.WRITTEN_OUT_F",
    "features.OS_BAL_AMT",
    "features.STEP_SUB_PORT",
    "features.INSURANCE_F"
]
DOWNSTREAM_ASSET = "features.STEP_INSURANCE"
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
                INTERSECT
                (   -- KS
                    (SELECT BASEL_ACCT_ID FROM features.OS_BAL_AMT
                    WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AND OS_BAL_AMT > 0
                    UNION
                    SELECT BASEL_ACCT_ID FROM features.CR_LMT_AMT
                    WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AND CR_LMT_AMT > 0)
                    UNION
                    -- MOR
                    (SELECT BASEL_ACCT_ID FROM features.TOTAL_BALANCE
                    WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AND TOTAL_BALANCE > 0)
                    UNION
                    -- SPL
                    (SELECT BASEL_ACCT_ID FROM features.OS_BAL_AMT_V2
                    WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AND OS_BAL_AMT_V2 > 0)
                )
            ), step_acct AS (
                SELECT
                    TRIM(m.STEP_PLN_AGRMNT_NUM) AS STEP_PLN_AGRMNT_NUM,
                    m.BASEL_ACCT_ID,
                    i.INSURANCE_F
                FROM non_excl
                JOIN ingestion.MORT_MTH_SNAPSHOT m ON m.BASEL_ACCT_ID = non_excl.BASEL_ACCT_ID
                JOIN features.INSURANCE_F i ON non_excl.BASEL_ACCT_ID = i.BASEL_ACCT_ID
                WHERE m.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
                    AND i.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
                    AND m.STEP_PLN_AGRMNT_NUM IS NOT NULL AND TRIM(m.STEP_PLN_AGRMNT_NUM) != ''
                UNION
                SELECT 
                    TRIM(m.STEP_PLN_AGRMNT_NUM) AS STEP_PLN_AGRMNT_NUM,
                    m.BASEL_ACCT_ID,
                    'uninsured' AS INSURANCE_F
                FROM non_excl
                JOIN ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT m ON m.BASEL_ACCT_ID = non_excl.BASEL_ACCT_ID
                WHERE m.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
                 AND m.STEP_PLN_AGRMNT_NUM IS NOT NULL AND TRIM(m.STEP_PLN_AGRMNT_NUM) != ''
                UNION
                SELECT 
                    TRIM(m.STEP_PLN_AGRMNT_NUM) AS STEP_PLN_AGRMNT_NUM,
                    m.BASEL_ACCT_ID,
                    'uninsured' AS INSURANCE_F
                FROM non_excl
                JOIN ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT m ON m.BASEL_ACCT_ID = non_excl.BASEL_ACCT_ID
                WHERE m.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
                 AND m.STEP_PLN_AGRMNT_NUM IS NOT NULL AND TRIM(m.STEP_PLN_AGRMNT_NUM) != ''
            ),
            step_rollup AS (
                SELECT
                    STEP_PLN_AGRMNT_NUM,
                    COUNT(*) AS TOTAL_ACCTS,
                    SUM(CASE WHEN INSURANCE_F = 'Insured' THEN 1 ELSE 0 END) AS CNT_INSURED,
                    SUM(CASE WHEN INSURANCE_F = 'uninsured' THEN 1 ELSE 0 END) AS CNT_UNINSURED,
                    SUM(CASE WHEN INSURANCE_F = 'BULK' THEN 1 ELSE 0 END) AS CNT_BULK
                FROM step_acct
                GROUP BY STEP_PLN_AGRMNT_NUM
            )
            SELECT
                '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
                TRIM(s.STEP_PLN_AGRMNT_NUM) AS STEP_PLN_AGRMNT_NUM,
                CASE s.STEP_SUB_PORT
                    WHEN 'STEP_MOR' THEN
                        CASE
                            WHEN r.cnt_bulk > 0 THEN 'MIX'
                            WHEN r.cnt_insured > 0 AND r.cnt_uninsured > 0 THEN 'MIX'
                            WHEN r.cnt_uninsured = r.total_accts THEN 'UNINSURED'
                            WHEN r.cnt_insured = r.total_accts THEN 'INSURED'
                            ELSE NULL
                        END
                    WHEN 'STEP_HELOC' THEN
                        CASE
                            WHEN r.cnt_uninsured = r.total_accts THEN 'UNINSURED'
                            ELSE NULL
                        END
                    WHEN 'STEP_MIX' THEN
                        CASE
                            WHEN r.cnt_bulk > 0 THEN 'MIX'
                            WHEN r.cnt_insured > 0 AND r.cnt_uninsured > 0 THEN 'MIX'
                            WHEN r.cnt_uninsured = r.total_accts THEN 'UNINSURED'
                            ELSE NULL
                        END
                    ELSE NULL
                END AS STEP_INSURANCE
            FROM features.STEP_SUB_PORT s
            JOIN non_excl ON s.BASEL_ACCT_ID = non_excl.BASEL_ACCT_ID
            JOIN step_rollup r ON TRIM(s.STEP_PLN_AGRMNT_NUM) = r.STEP_PLN_AGRMNT_NUM
            WHERE s.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
        )
    """,
):
    pass


