from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)


UPSTREAM_ASSET = [
    "ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT",
    "features.PIT_STATUS_CROSS_DEFAULT_ORIG",
    "features.OS_BAL_AMT_V2",
    "features.PRD_ID",
    "features.MONTH_DEF",
]
DOWNSTREAM_ASSET = "features.MODEL_EXCL_F_V2"
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
            ss.BASEL_ACCT_ID,
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' as OBSN_DT,
            (CASE
                WHEN 
                    (UPPER(pit.PIT_STATUS_CROSS_DEFAULT_ORIG) = 'CUR' AND (os_bal.OS_BAL_AMT_V2 < 100 OR ss.TOT_CRNT_BAL_AMT <= 0))
                    OR (UPPER(pit.PIT_STATUS_CROSS_DEFAULT_ORIG) = 'DEF' AND (os_bal.OS_BAL_AMT_V2 < 1 OR ss.TOT_CRNT_BAL_AMT <= 0))    -- small balance exclusions
                    OR prd_id.PRD_ID = 'S11'    -- home improvement loan exclusions
                    OR prd_id.PRD_ID = 'S14'    -- other indirect loan exclusions
                    OR ss.COMM_LOAN_CD IN (1, 2)    -- small business exclusion loans
                    OR ss.CRNT_BR_LOCTN_TRNST IN (18192, 99432)    -- invalid cab exclusions
                    OR month_def.MONTH_DEF > 24    -- longer than 24 months in default
                THEN 'Y'
                ELSE 'N'
            END) AS MODEL_EXCL_F_V2
        FROM ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT ss
        LEFT JOIN (
            SELECT * FROM features.PIT_STATUS_CROSS_DEFAULT_ORIG WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
        ) pit
        ON ss.BASEL_ACCT_ID = pit.BASEL_ACCT_ID
        LEFT JOIN (
            SELECT * FROM features.OS_BAL_AMT_V2 WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
        ) os_bal
        ON pit.BASEL_ACCT_ID = os_bal.BASEL_ACCT_ID
        LEFT JOIN (
            SELECT * FROM features.PRD_ID WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
        ) prd_id
        ON os_bal.BASEL_ACCT_ID = prd_id.BASEL_ACCT_ID
        LEFT JOIN (
            SELECT * FROM features.MONTH_DEF WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AND SRC_SYS_CD = 'SPL'
        ) month_def
        ON prd_id.BASEL_ACCT_ID = month_def.BASEL_ACCT_ID
        WHERE ss.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
        AND ss.RECD_STAT_CD IN (4,5,6,7,8)
    )
    """,
):
    pass


