from airflow.sdk import get_current_context, Param

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

DAG_PARAMS = {
    "mth_tm_id": Param("", type=["null", "string"], description="MTH_TM_ID"),
    "rundate": Param("", type=["null", "string"], description="RUNDATE"),
}


UPSTREAM_ASSET = [
    "ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT",
    "features.PIT_STATUS_CROSS_DEFAULT_ORIG",
    "reference.BLOCK_RECL_LKP",
    "reference.BLOCK_RECL_CLS_RSN_LKP",
    "reference.SRC_PRD_LKP",
]
DOWNSTREAM_ASSET = "features.CONSM_SCORECRD_EXCLSN_F"
DEPENDENCIES = {
    "duckdb_delete": ["duckdb_load"],
}


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
            WHEN TRIM(CSEF_CONDITION_1) = 'Y' THEN 'Y'
            WHEN TRIM(CSEF_CONDITION_2) = 'Y' THEN 'Y'
            WHEN TRIM(CSEF_CONDITION_3) = 'Y' THEN 'Y'
            WHEN (TOT_NEW_BAL_AMT <= 0 AND CR_LMT_AMT <= 0) THEN 'Y'
            WHEN TOT_NEW_BAL_AMT <= 0 AND (SUBSTR(TRIM(BLOCK_RECL_CD), 1, 1) = 'V' OR TRIM(BLOCK_RECL_CD) = 'FX') THEN 'Y'
            WHEN PIT_STATUS_CROSS_DEFAULT_ORIG = 'CHG' THEN 'Y'
            ELSE 'N'
        END AS CONSM_SCORECRD_EXCLSN_F
        FROM (
        SELECT 
            A.MTH_TM_ID,
            A.BASEL_ACCT_ID,
            A.PRIM_BASEL_CUST_ID AS BASEL_CUST_ID,
            TRIM(A.BLOCK_RECL_CD) AS BLOCK_RECL_CD,
            CR_LMT_AMT,
            TOT_NEW_BAL_AMT,
            CSEF_CONDITION_1,
            CSEF_CONDITION_2,
            CSEF_CONDITION_3,
            PIT.PIT_STATUS_CROSS_DEFAULT_ORIG
        FROM {UPSTREAM_ASSET[0]} A
        LEFT JOIN (
            SELECT 
                CONSM_SCORECRD_EXCLSN_F AS CSEF_CONDITION_1, 
                TRIM(BLOCK_RECL_CD) AS BLOCK_RECL_CD
            FROM reference.BLOCK_RECL_LKP
            WHERE strftime('%Y%m', DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}') BETWEEN EFF_FROM_YR_MTH AND EFF_TO_YR_MTH
            AND BLOCK_RECL_CD IS NOT NULL
        ) CC1 ON TRIM(A.BLOCK_RECL_CD) = CC1.BLOCK_RECL_CD
        LEFT JOIN (
            SELECT 
                CONSM_SCORECRD_EXCLSN_F AS CSEF_CONDITION_2, 
                TRIM(CLS_RSN_CD) AS CLS_RSN_CD, 
                TRIM(BLOCK_RECL_CD) AS BLOCK_RECL_CD
            FROM reference.BLOCK_RECL_CLS_RSN_LKP
            WHERE strftime('%Y%m', DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}') BETWEEN EFF_FROM_YR_MTH AND EFF_TO_YR_MTH
            AND CLS_RSN_CD IS NOT NULL
            AND BLOCK_RECL_CD IS NOT NULL
        ) CC2 ON TRIM(A.BLOCK_RECL_CD) = CC2.BLOCK_RECL_CD AND TRIM(A.acct_CLS_RSN_CD) = CC2.CLS_RSN_CD
        LEFT JOIN (
            SELECT 
                CONSM_SCORECRD_EXCLSN_F AS CSEF_CONDITION_3, 
                TRIM(SRC_PRD_CD) AS SRC_PRD_CD, 
                TRIM(SRC_SUB_PRD_CD) AS SRC_SUB_PRD_CD
            FROM reference.SRC_PRD_LKP
            WHERE strftime('%Y%m', DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}') BETWEEN EFF_FROM_YR_MTH AND EFF_TO_YR_MTH
            AND TRIM(PRD_SYS_CD) = 'KS'
        ) SP ON TRIM(A.PRD_CD) = SP.SRC_PRD_CD AND TRIM(A.SUB_PRD_CD) = SP.SRC_SUB_PRD_CD
        LEFT JOIN features.PIT_STATUS_CROSS_DEFAULT_ORIG PIT
            ON A.BASEL_ACCT_ID = PIT.BASEL_ACCT_ID and PIT.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
        WHERE A.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
        )
    )
    """,
):
    pass


