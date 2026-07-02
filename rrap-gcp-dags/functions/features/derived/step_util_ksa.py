from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

UPSTREAM_ASSET = [
    "ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT", 
    "features.MODEL_EXCL_F",
    "features.WRITTEN_OUT_F",
    "features.TREATMENT_F",
    "features.CR_LMT_AMT",
    "features.TOT_NEW_BAL_AMT",
    "features.PIT_STATUS_STEP",
]
DOWNSTREAM_ASSET = "features.STEP_UTIL_KSA"
DEPENDENCIES = {
    "export_revl": ["export_included_accts"],
    "export_included_accts": ["export_step_agg"],
    "export_step_agg": ["duckdb_delete"],
    "duckdb_delete": ["duckdb_load"],
}


def export_revl(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        SELECT 
            BASEL_ACCT_ID,
            TRIM(STEP_PLN_AGRMNT_NUM) AS STEP_PLN_AGRMNT_NUM,
            MTH_TM_ID 
        FROM {UPSTREAM_ASSET[0]}  -- ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT
        WHERE MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
        AND STEP_PLN_AGRMNT_NUM IS NOT NULL
        AND TRIM(STEP_PLN_AGRMNT_NUM) <> ''
    """
):
    pass


def export_included_accts(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        SELECT 
            ss.BASEL_ACCT_ID
        FROM
            '{{{{ task_instance.xcom_pull(task_ids="derived__step_util_ksa.export_revl", key="parquet") }}}}' ss
        INNER JOIN 
            (SELECT * FROM {UPSTREAM_ASSET[1]} WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}') a -- features.MODEL_EXCL_F
            ON ss.BASEL_ACCT_ID = a.BASEL_ACCT_ID
        INNER JOIN
            (SELECT * FROM {UPSTREAM_ASSET[2]} WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}') b -- features.WRITTEN_OUT_F
            ON a.BASEL_ACCT_ID = b.BASEL_ACCT_ID
        INNER JOIN
            (SELECT * FROM {UPSTREAM_ASSET[3]} WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}') c -- features.TREATMENT_F
            ON b.BASEL_ACCT_ID = c.BASEL_ACCT_ID
        INNER JOIN
            (SELECT * FROM {UPSTREAM_ASSET[4]} WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}') d -- features.CR_LMT_AMT
            ON c.BASEL_ACCT_ID = d.BASEL_ACCT_ID
        INNER JOIN
            (SELECT * FROM {UPSTREAM_ASSET[5]} WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}') e -- features.TOT_NEW_BAL_AMT
            ON d.BASEL_ACCT_ID = e.BASEL_ACCT_ID
        INNER JOIN
            (SELECT * FROM {UPSTREAM_ASSET[6]} WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}') f -- features.PIT_STATUS_STEP
            ON e.BASEL_ACCT_ID = f.BASEL_ACCT_ID
        WHERE UPPER(TRIM(a.MODEL_EXCL_F)) <> 'Y'
        AND UPPER(TRIM(b.WRITTEN_OUT_F)) <> 'Y'
        AND UPPER(TRIM(c.TREATMENT_F)) = 'A'
        AND (d.CR_LMT_AMT > 0 OR e.TOT_NEW_BAL_AMT > 0)
        AND UPPER(TRIM(f.PIT_STATUS_STEP)) = 'CUR' 
    """
):
    pass


def export_step_agg(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        SELECT
            TRIM(STEP_PLN_AGRMNT_NUM) AS STEP_PLN_AGRMNT_NUM,
            SUM(CR_LMT_AMT) AS STEP_CR_LMT_AMT,
            SUM(TOT_NEW_BAL_AMT) AS STEP_TOT_NEW_BAL_AMT
        FROM
            {UPSTREAM_ASSET[0]} RSN
        INNER JOIN
            '{{{{ task_instance.xcom_pull(task_ids="derived__step_util_ksa.export_included_accts", key="parquet") }}}}' a
        ON rsn.BASEL_ACCT_ID = a.BASEL_ACCT_ID
        WHERE
            RSN.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
            AND RSN.STEP_PLN_AGRMNT_NUM IS NOT NULL AND TRIM(RSN.STEP_PLN_AGRMNT_NUM) <> ''
        GROUP BY TRIM(STEP_PLN_AGRMNT_NUM)
    """
):
    pass


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
            SELECT 
                '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
                STEP_PLN_AGRMNT_NUM,
                (
                    CASE WHEN STEP_CR_LMT_AMT = 0 AND STEP_TOT_NEW_BAL_AMT > 0 THEN 1
                    WHEN STEP_TOT_NEW_BAL_AMT < 0 THEN 0
                    WHEN STEP_CR_LMT_AMT <> 0 THEN STEP_TOT_NEW_BAL_AMT / STEP_CR_LMT_AMT
                    ELSE -1
                END) AS STEP_UTIL_KSA
            FROM '{{{{ task_instance.xcom_pull(task_ids="derived__step_util_ksa.export_step_agg", key="parquet") }}}}'
        )
    """,
):
    pass

