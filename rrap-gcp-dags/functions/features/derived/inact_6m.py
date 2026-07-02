from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

UPSTREAM_ASSET = [
    "ingestion.TM_DIM",
    "ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT",
    "features.PIT_STATUS_CROSS_DEFAULT_ORIG",
]
DOWNSTREAM_ASSET = "features.INACT_6M"
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
            WITH sm AS (
                SELECT
                    CASE
                        WHEN MAX(LAST_PRCH_DT) IS NULL AND MAX(LAST_PYMT_DT) IS NOT NULL THEN DATE '1800-01-01'
                        ELSE DATE_TRUNC('month', MAX(LAST_PRCH_DT))
                    END AS LLAST_PRCH_DT,
                    CASE
                        WHEN MAX(LAST_PYMT_DT) IS NULL AND MAX(LAST_PRCH_DT) IS NOT NULL THEN DATE '1800-01-01'
                        ELSE DATE_TRUNC('month', MAX(LAST_PYMT_DT))
                    END AS LLAST_PYMT_DT,
                    COUNT(BASEL_ACCT_ID) as CNT_BASEL_ACCT_ID ,
                    ss.BASEL_ACCT_ID
                FROM ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT ss
                WHERE MTH_TM_ID BETWEEN {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}} - 200
                    AND {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
                GROUP BY BASEL_ACCT_ID
            )
            SELECT
                '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
                ss.BASEL_ACCT_ID,
                CASE
                    WHEN PIT_STATUS_CROSS_DEFAULT_ORIG = 'CUR' AND LLAST_PRCH_DT IS NULL AND LLAST_PYMT_DT IS NULL 
                        AND (CNT_BASEL_ACCT_ID >= 6 
                            OR DATE_DIFF('month', ss.ACCT_OPND_DT, (SELECT TM_LVL_END_DT FROM ingestion.TM_DIM WHERE TRIM(TM_LVL) = 'Month' 
                                AND TM_LVL_END_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}')) >= 6) THEN 'Y'
                    WHEN PIT_STATUS_CROSS_DEFAULT_ORIG = 'CUR' 
                        AND DATE_DIFF( -- number of months since the most recent activity
                            'month',
                            CASE
                                WHEN LLAST_PRCH_DT < LLAST_PYMT_DT THEN LLAST_PYMT_DT
                                WHEN LLAST_PRCH_DT = LLAST_PYMT_DT THEN LLAST_PYMT_DT
                                WHEN LLAST_PRCH_DT > LLAST_PYMT_DT THEN LLAST_PRCH_DT
                                ELSE NULL
                            END,
                            (SELECT TM_LVL_ST_DT FROM ingestion.TM_DIM WHERE TRIM(TM_LVL) = 'Month' 
                                AND TM_LVL_END_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}')
                        ) >= 6 THEN 'Y'
                    ELSE 'N'
                END AS INACT_6M
            FROM ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT ss
            JOIN features.PIT_STATUS_CROSS_DEFAULT_ORIG pit ON ss.BASEL_ACCT_ID = pit.BASEL_ACCT_ID
            LEFT JOIN sm ON ss.BASEL_ACCT_ID = sm.BASEL_ACCT_ID
            WHERE ss.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
                AND pit.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
        )
    """,
):
    pass


