from airflow.sdk import get_current_context
 
from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)
 
 
UPSTREAM_ASSET = [
    "ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT",
    "ingestion.MORT_MTH_SNAPSHOT",
    "ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT",
    "ingestion.BASEL_STEP_PLN_MTH_SNAPSHOT",
    "features.CONSM_PRD_TREATMNT_CD",
    "features.SML_BUS_F",
]
DOWNSTREAM_ASSET = "features.STEP_MORT_ACCT_CNT"
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
            WITH SQ_BASEL_STEP_PLN_MTH_SNAPSHOT AS (
                SELECT DISTINCT  
                    A.STEP_PLN_SNAPSHOT_ID,
                    A.MTH_TM_ID,
                    TRIM(A.STEP_PLN_AGRMNT_NUM) AS STEP_PLN_AGRMNT_NUM
                FROM ingestion.BASEL_STEP_PLN_MTH_SNAPSHOT A 
                WHERE A.STEP_PLN_AGRMNT_NUM IS NOT NULL 
                AND A.STEP_PLN_AGRMNT_NUM <> -1
                AND A.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
                AND A.STEP_PLN_SNAPSHOT_ID IN (
                    SELECT ss.STEP_PLN_SNAPSHOT_ID
                    FROM ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT ss
                    JOIN features.CONSM_PRD_TREATMNT_CD trt
                        ON ss.BASEL_ACCT_ID = trt.BASEL_ACCT_ID
                    JOIN features.SML_BUS_F sml
                        ON ss.BASEL_ACCT_ID = sml.BASEL_ACCT_ID
                    WHERE ss.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
                        AND trt.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
                        AND sml.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
                        AND ss.STEP_PLN_SNAPSHOT_ID <> -1
                        AND trt.CONSM_PRD_TREATMNT_CD = 'A'
                        AND sml.SML_BUS_F = 'N'
                    
                    UNION ALL
                    
                    SELECT ss.STEP_PLN_SNAPSHOT_ID
                    FROM ingestion.MORT_MTH_SNAPSHOT ss
                    JOIN features.CONSM_PRD_TREATMNT_CD trt
                        ON ss.BASEL_ACCT_ID = trt.BASEL_ACCT_ID
                    WHERE ss.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
                        AND trt.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
                        AND TRIM(trt.CONSM_PRD_TREATMNT_CD) = 'A'
                        AND ss.STEP_PLN_SNAPSHOT_ID <> -1
                    
                    UNION ALL
                    
                    SELECT ss.STEP_PLN_SNAPSHOT_ID
                    FROM ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT ss
                    JOIN features.CONSM_PRD_TREATMNT_CD trt
                        ON ss.BASEL_ACCT_ID = trt.BASEL_ACCT_ID
                    WHERE ss.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
                        AND trt.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
                        AND TRIM(trt.CONSM_PRD_TREATMNT_CD) = 'A'
                        AND ss.STEP_PLN_SNAPSHOT_ID <> -1
                )
            ),
            STEP_DRVD_VARS_CNT_MO_F AS (
                SELECT
                    COUNT(ss.BASEL_ACCT_ID) AS STEP_MORT_ACCT_CNT,
                    ss.STEP_PLN_SNAPSHOT_ID,
                    TRIM(ss.STEP_PLN_AGRMNT_NUM) AS STEP_PLN_AGRMNT_NUM
                FROM ingestion.MORT_MTH_SNAPSHOT ss
                LEFT JOIN features.CONSM_PRD_TREATMNT_CD trt
                    ON ss.BASEL_ACCT_ID = trt.BASEL_ACCT_ID
                WHERE ss.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
                    AND trt.OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
                    AND trt.CONSM_PRD_TREATMNT_CD = 'A'
                    AND ss.STEP_PLN_SNAPSHOT_ID <> -1
                GROUP BY ss.STEP_PLN_AGRMNT_NUM, ss.STEP_PLN_SNAPSHOT_ID
            )
            SELECT
                '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
                b.STEP_PLN_SNAPSHOT_ID,
                COALESCE(c.STEP_MORT_ACCT_CNT, 0) AS STEP_MORT_ACCT_CNT,
            FROM SQ_BASEL_STEP_PLN_MTH_SNAPSHOT b
            LEFT JOIN STEP_DRVD_VARS_CNT_MO_F c 
                ON TRIM(c.STEP_PLN_AGRMNT_NUM) = TRIM(b.STEP_PLN_AGRMNT_NUM)
        )
    """,
):
    pass
 
 
