from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)


UPSTREAM_ASSET = [
    "ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT",
    "reference.SRC_PRD_STDNT_LOAN_LKP",
    "reference.SRC_PRD_LKP",
]
DOWNSTREAM_ASSET = "features.BASEL_PRD_CD"
DEPENDENCIES = {
    "duckdb_delete": ["duckdb_load"],
}
# reference tables
SRC_PRD_STDNT_LOAN_LKP = "reference.SRC_PRD_STDNT_LOAN_LKP"
SRC_PRD_LKP = "reference.SRC_PRD_LKP"


def duckdb_delete(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    delete from {DOWNSTREAM_ASSET} where obsn_dt = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """,
):
    pass


def duckdb_load(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} by name
    FROM (
        SELECT
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            BASEL_ACCT_ID,
            CASE 
                WHEN TRIM(SP.SRC_PRD_CD) = 'SSL' THEN TRIM(STD.BASEL_PRD_CD) 
                ELSE TRIM(SP.BASEL_PRD_CD) 
            END AS BASEL_PRD_CD,
            CASE 
                WHEN TRIM(SP.SRC_PRD_CD) = 'SSL' THEN TRIM(STD.BASEL_PRD_DESC)
                ELSE TRIM(SP.BASEL_PRD_DESC) 
            END AS BASEL_PRD_DESC
        FROM {UPSTREAM_ASSET[0]} A
        LEFT JOIN (
            SELECT * 
            FROM {SRC_PRD_LKP}
            WHERE strftime('%Y%m', DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}') BETWEEN EFF_FROM_YR_MTH AND EFF_TO_YR_MTH
        ) SP
        ON TRIM(A.PRD_CD) = TRIM(SP.SRC_PRD_CD) AND TRIM(A.SUB_PRD_CD) = TRIM(SP.SRC_SUB_PRD_CD)
        LEFT JOIN (
            SELECT *
            FROM {SRC_PRD_STDNT_LOAN_LKP}
            WHERE strftime('%Y%m', DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}') BETWEEN EFF_FROM_YR_MTH AND EFF_TO_YR_MTH
        ) STD
        ON TRIM(A.PRD_CD) = TRIM(STD.SRC_PRD_CD)
        AND TRIM(A.SUB_PRD_CD) = TRIM(STD.SRC_SUB_PRD_CD)
        AND SUBSTR(TRIM(CRNT_BILL_CD), 3, 1) = BILL_CD_CHAR
        WHERE A.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
        )
    """,
):
    pass


