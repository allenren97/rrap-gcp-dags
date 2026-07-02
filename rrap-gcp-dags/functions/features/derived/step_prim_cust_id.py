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
    "features.TREATMENT_F"
]
DOWNSTREAM_ASSET = "features.STEP_PRIM_CUST_ID"
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
                (
                --MOR
                SELECT BASEL_ACCT_ID FROM features.TOTAL_BALANCE
                WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AND TOTAL_BALANCE > 0
                UNION
                --KS
                SELECT BASEL_ACCT_ID FROM features.TOT_NEW_BAL_AMT
                WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AND TOT_NEW_BAL_AMT > 0
                UNION
                --SPL
                SELECT BASEL_ACCT_ID FROM features.OS_BAL_AMT_V2
                WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AND OS_BAL_AMT_V2 > 0
                UNION
                -- general
                SELECT BASEL_ACCT_ID FROM ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT
                WHERE MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}} AND CR_LMT_AMT > 0
                )
            ),
            mort AS (
                SELECT STEP_PLN_AGRMNT_NUM, PRIM_BASEL_CUST_ID AS MORT_PRIM_BASEL_CUST_ID
                FROM (
                    SELECT
                        TRIM(ss.STEP_PLN_AGRMNT_NUM) AS STEP_PLN_AGRMNT_NUM,
                        ss.PRIM_BASEL_CUST_ID,
                        ss.BASEL_ACCT_ID,
                        ROW_NUMBER() OVER (
                            PARTITION BY TRIM(ss.STEP_PLN_AGRMNT_NUM)
                            ORDER BY ss.BASEL_ACCT_ID ASC, ss.PRIM_BASEL_CUST_ID ASC
                        ) AS rn
                    FROM ingestion.MORT_MTH_SNAPSHOT ss
                    JOIN non_excl ON ss.BASEL_ACCT_ID = non_excl.BASEL_ACCT_ID
                    WHERE ss.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
                    AND ss.PRIM_BASEL_CUST_ID <> -1
                    AND ss.STEP_PLN_AGRMNT_NUM IS NOT NULL
                    AND TRIM(ss.STEP_PLN_AGRMNT_NUM) <> ''
                ) s
                WHERE rn = 1
            ),
            revl AS (
                SELECT STEP_PLN_AGRMNT_NUM, PRIM_BASEL_CUST_ID AS REVL_PRIM_BASEL_CUST_ID
                FROM (
                    SELECT
                        TRIM(ss.STEP_PLN_AGRMNT_NUM) AS STEP_PLN_AGRMNT_NUM,
                        ss.PRIM_BASEL_CUST_ID,
                        ss.BASEL_ACCT_ID,
                        ROW_NUMBER() OVER (
                            PARTITION BY TRIM(ss.STEP_PLN_AGRMNT_NUM)
                            ORDER BY ss.BASEL_ACCT_ID ASC, ss.PRIM_BASEL_CUST_ID ASC
                        ) AS rn
                    FROM ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT ss
                    JOIN non_excl ON ss.BASEL_ACCT_ID = non_excl.BASEL_ACCT_ID
                    WHERE ss.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
                    AND ss.PRIM_BASEL_CUST_ID <> -1
                    AND ss.STEP_PLN_AGRMNT_NUM IS NOT NULL
                    AND TRIM(ss.STEP_PLN_AGRMNT_NUM) <> ''
                ) s
                WHERE rn = 1
            ),
            psnl AS (
                SELECT STEP_PLN_AGRMNT_NUM, PRIM_BASEL_CUST_ID AS PSNL_PRIM_BASEL_CUST_ID
                FROM (
                    SELECT
                        TRIM(ss.STEP_PLN_AGRMNT_NUM) AS STEP_PLN_AGRMNT_NUM,
                        ss.PRIM_BASEL_CUST_ID,
                        ss.BASEL_ACCT_ID,
                        ROW_NUMBER() OVER (
                            PARTITION BY TRIM(ss.STEP_PLN_AGRMNT_NUM)
                            ORDER BY ss.BASEL_ACCT_ID ASC, ss.PRIM_BASEL_CUST_ID ASC
                        ) AS rn
                    FROM ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT ss
                    JOIN non_excl ON ss.BASEL_ACCT_ID = non_excl.BASEL_ACCT_ID
                    WHERE ss.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
                    AND ss.PRIM_BASEL_CUST_ID <> -1
                    AND ss.STEP_PLN_AGRMNT_NUM IS NOT NULL
                    AND TRIM(ss.STEP_PLN_AGRMNT_NUM) <> ''
                ) s
                WHERE rn = 1
            ),
            keys AS (
                SELECT STEP_PLN_AGRMNT_NUM FROM mort
                UNION
                SELECT STEP_PLN_AGRMNT_NUM FROM revl
                UNION
                SELECT STEP_PLN_AGRMNT_NUM FROM psnl
            )
            SELECT
                '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
                k.STEP_PLN_AGRMNT_NUM,
                COALESCE(
                    m.MORT_PRIM_BASEL_CUST_ID,
                    r.REVL_PRIM_BASEL_CUST_ID,
                    p.PSNL_PRIM_BASEL_CUST_ID
                ) AS STEP_PRIM_CUST_ID
            FROM keys k
            LEFT JOIN mort m USING (STEP_PLN_AGRMNT_NUM)
            LEFT JOIN revl r USING (STEP_PLN_AGRMNT_NUM)
            LEFT JOIN psnl p USING (STEP_PLN_AGRMNT_NUM)
        )
    """,
):
    pass


