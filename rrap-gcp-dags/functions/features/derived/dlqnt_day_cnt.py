from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)


UPSTREAM_ASSET = [
    "ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT",
    "ingestion.MORT_MTH_SNAPSHOT",
    "ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT",
    "ingestion.TM_DIM"
]
DOWNSTREAM_ASSET = "features.DLQNT_DAY_CNT"
DEPENDENCIES = {
    "duckdb_delete": ["export_dlqnt_day_cnt_ks", "export_dlqnt_day_cnt_mor", "export_dlqnt_day_cnt_spl"],
    "export_dlqnt_day_cnt_ks": ["duckdb_load"],
    "export_dlqnt_day_cnt_mor": ["duckdb_load"],
    "export_dlqnt_day_cnt_spl": ["duckdb_load"],
}


def duckdb_delete(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        DELETE FROM {DOWNSTREAM_ASSET}
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """,
):
    pass


def export_dlqnt_day_cnt_ks(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        SELECT
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            BASEL_ACCT_ID,
            (
                CASE
                    WHEN TRIM(CHRG_OFF_CD) <> '1'
                    THEN (CASE WHEN BNS_DLQNT_DAY >= 30 THEN BNS_DLQNT_DAY - 30 ELSE 0 END)
                ELSE NULL
                END
            ) AS DLQNT_DAY_CNT,
            'KS' AS SRC_SYS_CD
        FROM { UPSTREAM_ASSET[0] } -- ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT
        where MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
        and prim_basel_cust_id is not null and prim_basel_cust_id <> -1 
        and (TOT_NEW_BAL_AMT > 0 or CR_LMT_AMT > 0) and
        TRIM(PRD_CD) not in ('BLV') and
        TRIM(SUB_PRD_CD) not in ('CC') and 
        TRIM(PRD_CD) in ('VIC','SCL','VLR','VGD','VCL','LOC','SSL','')
    """,
):
    pass


def export_dlqnt_day_cnt_mor(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        WITH last_business_day AS (
            SELECT MAX(DAY_DT) AS LAST_BUSINESS_DAY
            FROM { UPSTREAM_ASSET[3] } -- ingestion.TM_DIM
            WHERE TRIM(TM_LVL) = 'Day' AND CLNDR_YR IN (SELECT CLNDR_YR FROM ingestion.TM_DIM WHERE TRIM(TM_LVL) = 'Month' AND TM_LVL_END_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}')
                AND MTH_CLNDR_CD IN (SELECT MTH_CLNDR_CD FROM ingestion.TM_DIM WHERE TRIM(TM_LVL) = 'Month' AND TM_LVL_END_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}')
            AND TRIM(DAY_OF_WK_DESC) IN ('Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday')
        )
        SELECT 
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            BASEL_ACCT_ID,
            CASE
                WHEN PD_OFF_DT IS NOT NULL OR TRIM(PD_OFF_F) = 'Y' THEN 0
                WHEN TRIM(FLOAT_CD) IN ('W', 'B', 'S') AND WK_FRST_UNPAID_DT IS NOT NULL THEN 
                    CASE 
                        WHEN LAST_BUSINESS_DAY - WK_FRST_UNPAID_DT < 0 THEN 0
                        ELSE LAST_BUSINESS_DAY - WK_FRST_UNPAID_DT
                    END
                WHEN FRST_UNPAID_DT IS NOT NULL THEN 
                    CASE 
                        WHEN LAST_BUSINESS_DAY - FRST_UNPAID_DT < 0 THEN 0
                        ELSE LAST_BUSINESS_DAY - FRST_UNPAID_DT
                    END
                ELSE NULL
            END AS DLQNT_DAY_CNT,
            'MO' AS SRC_SYS_CD
        FROM { UPSTREAM_ASSET[1] } -- ingestion.MORT_MTH_SNAPSHOT
        CROSS JOIN last_business_day
        WHERE MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    """,
):
    pass


def export_dlqnt_day_cnt_spl(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        SELECT
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            BASEL_ACCT_ID,
            (
                CASE
                    WHEN RECD_STAT_CD IN (4,5) THEN DAY_ODUE ELSE NULL
                END 
            ) AS DLQNT_DAY_CNT,
            'SPL' AS SRC_SYS_CD
        FROM { UPSTREAM_ASSET[2] }  -- ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT
        WHERE MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
        AND PRIM_BASEL_CUST_ID IS NOT NULL
        AND PRIM_BASEL_CUST_ID <> -1
        AND RECD_STAT_CD IN (4,5,6,7,8)
    """,
):
    pass


def duckdb_load(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} by name
    FROM (
        select * 
        from read_parquet([
            '{{{{ task_instance.xcom_pull(task_ids="derived__dlqnt_day_cnt.export_dlqnt_day_cnt_ks", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__dlqnt_day_cnt.export_dlqnt_day_cnt_mor", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="derived__dlqnt_day_cnt.export_dlqnt_day_cnt_spl", key="parquet") }}}}'
        ], union_by_name = true)
    )
    """,
):
    pass

