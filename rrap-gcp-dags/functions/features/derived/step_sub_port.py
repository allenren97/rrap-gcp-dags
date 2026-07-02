from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)


UPSTREAM_ASSET = [
    "ingestion.MORT_MTH_SNAPSHOT",
    "ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT",
    "ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT",
    "features.HELOC_F",
    "features.PRD_ID",
    "features.WRITTEN_OUT_F",
    "features.MODEL_EXCL_F",
    "features.TREATMENT_F",
    "features.TOTAL_BALANCE",
    "features.TOT_NEW_BAL_AMT",
    "features.CR_LMT_AMT",
    "features.OS_BAL_AMT_V2",
    "features.PIT_STATUS_STEP",
]
DOWNSTREAM_ASSET = "features.STEP_SUB_PORT"
DEPENDENCIES = {
    "duckdb_start": ["export_mor","export_ks","export_dtl"],
    "export_mor": ["export_all"],
    "export_ks": ["export_all"],
    "export_dtl": ["export_all"],
    "export_all": ["duckdb_delete"],
    "duckdb_delete": ["duckdb_load"],
}


def duckdb_start(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    SELECT 'START'
    """,
):
    pass


def export_mor(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    SELECT
        nullif(trim(s.STEP_PLN_AGRMNT_NUM), '') STEP_PLN_AGRMNT_NUM,
        s.BASEL_ACCT_ID,
        CASE WHEN TRIM(COALESCE(s.STEP_PLN_AGRMNT_NUM,''))='' THEN 'Standalone_MOR' ELSE 'STEP_MOR' END AS STEP_SUB_PORT,
        'MOR' AS STEP_PRODUCT
    FROM {UPSTREAM_ASSET[0]} s
    WHERE
        s.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    """,
):
    pass


def export_ks(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    SELECT
        nullif(trim(s.STEP_PLN_AGRMNT_NUM), '') STEP_PLN_AGRMNT_NUM,
        s.BASEL_ACCT_ID,
        CASE WHEN TRIM(COALESCE(s.STEP_PLN_AGRMNT_NUM,''))='' THEN 'Standalone_HELOC' ELSE 'STEP_HELOC' END AS STEP_SUB_PORT,
        'HELOC' AS STEP_PRODUCT
    FROM {UPSTREAM_ASSET[1]} s
    INNER JOIN {UPSTREAM_ASSET[3]} f ON
        s.BASEL_ACCT_ID = f.BASEL_ACCT_ID
        AND TRIM(UPPER(f.HELOC_F)) = 'Y'
        AND f.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    WHERE
        s.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    """,
):
    pass


def export_dtl(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    SELECT
        s.STEP_PLN_AGRMNT_NUM,
        s.BASEL_ACCT_ID,
        'STEP_MIX' AS STEP_SUB_PORT,
        'DTL' AS STEP_PRODUCT
    FROM {UPSTREAM_ASSET[2]} s
    INNER JOIN {UPSTREAM_ASSET[4]} pid ON
        s.BASEL_ACCT_ID = pid.BASEL_ACCT_ID
        AND pid.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    WHERE
        s.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
        AND TRIM(COALESCE(s.STEP_PLN_AGRMNT_NUM,'')) != ''
    """,
):
    pass


def export_all(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        WITH
        accts AS (
            SELECT
                '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' as OBSN_DT,
                STEP_PLN_AGRMNT_NUM,
                BASEL_ACCT_ID,
                STEP_SUB_PORT,
                STEP_PRODUCT
            FROM read_parquet([
                '{{{{ task_instance.xcom_pull(task_ids="derived__step_sub_port.export_mor", key="parquet") }}}}',
                '{{{{ task_instance.xcom_pull(task_ids="derived__step_sub_port.export_ks", key="parquet") }}}}',
                '{{{{ task_instance.xcom_pull(task_ids="derived__step_sub_port.export_dtl", key="parquet") }}}}'
                ], union_by_name = true)
        )
        , flags AS (
            SELECT
                a.OBSN_DT,
                a.STEP_PLN_AGRMNT_NUM,
                a.BASEL_ACCT_ID,
                a.STEP_SUB_PORT,
                a.STEP_PRODUCT,
                t1.WRITTEN_OUT_F,
                t2.MODEL_EXCL_F,
                t3.TREATMENT_F,
                t4.TOTAL_BALANCE,
                t5.TOT_NEW_BAL_AMT,
                t6.CR_LMT_AMT,
                t7.OS_BAL_AMT_V2,
                t8.PIT_STATUS_STEP,
                CASE
                    WHEN t1.WRITTEN_OUT_F = 'N' AND t2.MODEL_EXCL_F = 'N' THEN
                        CASE
                            WHEN
                                (STEP_PRODUCT = 'MOR' AND t4.TOTAL_BALANCE > 0)
                                OR (STEP_PRODUCT = 'HELOC' AND (t5.TOT_NEW_BAL_AMT > 0 OR t6.CR_LMT_AMT > 0))
                                OR (STEP_PRODUCT = 'DTL' AND t7.OS_BAL_AMT_V2 > 0) THEN 'INCLUDE'
                            ELSE 'EXCLUDE'
                        END
                    ELSE 'EXCLUDE'
                END AS SUPER_FLAG
            FROM accts a
            INNER JOIN {UPSTREAM_ASSET[5]} t1 ON
                a.OBSN_DT = t1.OBSN_DT
                AND a.BASEL_ACCT_ID = t1.BASEL_ACCT_ID
            INNER JOIN {UPSTREAM_ASSET[6]} t2 ON
                a.OBSN_DT = t2.OBSN_DT
                AND a.BASEL_ACCT_ID = t2.BASEL_ACCT_ID
            LEFT OUTER JOIN (
                SELECT OBSN_DT, BASEL_ACCT_ID, TREATMENT_F
                FROM {UPSTREAM_ASSET[7]}
                WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
            ) t3 ON
                a.OBSN_DT = t3.OBSN_DT
                AND a.BASEL_ACCT_ID = t3.BASEL_ACCT_ID
            LEFT OUTER JOIN (
                SELECT OBSN_DT, BASEL_ACCT_ID, TOTAL_BALANCE
                FROM {UPSTREAM_ASSET[8]}
                WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
            ) t4 ON
                a.OBSN_DT = t4.OBSN_DT
                AND a.BASEL_ACCT_ID = t4.BASEL_ACCT_ID
            LEFT OUTER JOIN (
                SELECT OBSN_DT, BASEL_ACCT_ID, TOT_NEW_BAL_AMT
                FROM {UPSTREAM_ASSET[9]}
                WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
            ) t5 ON
                a.OBSN_DT = t5.OBSN_DT
                AND a.BASEL_ACCT_ID = t5.BASEL_ACCT_ID
            LEFT OUTER JOIN (
                SELECT OBSN_DT, BASEL_ACCT_ID, CR_LMT_AMT
                FROM {UPSTREAM_ASSET[10]}
                WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
            ) t6 ON
                a.OBSN_DT = t6.OBSN_DT
                AND a.BASEL_ACCT_ID = t6.BASEL_ACCT_ID
            LEFT OUTER JOIN (
                SELECT OBSN_DT, BASEL_ACCT_ID, OS_BAL_AMT_V2
                FROM {UPSTREAM_ASSET[11]}
                WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
            ) t7 ON
                a.OBSN_DT = t7.OBSN_DT
                AND a.BASEL_ACCT_ID = t7.BASEL_ACCT_ID
            LEFT OUTER JOIN (
                SELECT OBSN_DT, BASEL_ACCT_ID, PIT_STATUS_STEP
                FROM {UPSTREAM_ASSET[12]}
                WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
            ) t8 ON
                a.OBSN_DT = t8.OBSN_DT
                AND a.BASEL_ACCT_ID = t8.BASEL_ACCT_ID
        )
        SELECT
            OBSN_DT
            , STEP_PLN_AGRMNT_NUM
            , BASEL_ACCT_ID
            , STEP_SUB_PORT
            , STEP_PRODUCT
            , WRITTEN_OUT_F
            , MODEL_EXCL_F
            , TREATMENT_F
            , TOTAL_BALANCE
            , TOT_NEW_BAL_AMT
            , CR_LMT_AMT
            , OS_BAL_AMT_V2
            , PIT_STATUS_STEP
            , SUPER_FLAG
            , COUNT (DISTINCT STEP_PRODUCT) OVER (PARTITION BY STEP_PLN_AGRMNT_NUM, SUPER_FLAG) AS COMPOSITION_COUNT
            , CASE WHEN COUNT (DISTINCT STEP_PRODUCT) OVER (PARTITION BY STEP_PLN_AGRMNT_NUM, SUPER_FLAG) > 1 THEN 'STEP_MIX' ELSE STEP_SUB_PORT END AS FINAL_STEP_SUB_PORT
        FROM flags
        WHERE
            STEP_PLN_AGRMNT_NUM IS NOT NULL
            AND TREATMENT_F = 'A'
            AND PIT_STATUS_STEP != 'CLO'
        UNION ALL
        SELECT
            OBSN_DT
            , STEP_PLN_AGRMNT_NUM
            , BASEL_ACCT_ID
            , STEP_SUB_PORT
            , STEP_PRODUCT
            , WRITTEN_OUT_F
            , MODEL_EXCL_F
            , TREATMENT_F
            , TOTAL_BALANCE
            , TOT_NEW_BAL_AMT
            , CR_LMT_AMT
            , OS_BAL_AMT_V2
            , PIT_STATUS_STEP
            , SUPER_FLAG
            , null
            , STEP_SUB_PORT
        FROM flags
        WHERE
            STEP_PLN_AGRMNT_NUM IS NULL
            OR
            PIT_STATUS_STEP = 'CLO'
            OR
            TREATMENT_F != 'A'
    """,
):
    pass


def duckdb_delete(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    delete from {DOWNSTREAM_ASSET} where OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """,
):
    pass


def duckdb_load(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} by name
    FROM (
        SELECT
            OBSN_DT,
            STEP_PLN_AGRMNT_NUM,
            BASEL_ACCT_ID,
            FINAL_STEP_SUB_PORT AS STEP_SUB_PORT
        FROM '{{{{ task_instance.xcom_pull(task_ids="derived__step_sub_port.export_all", key="parquet") }}}}'
    )
    """,
):
    pass
