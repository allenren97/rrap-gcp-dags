from airflow.sdk import get_current_context
from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)
# Upstream assets (table names with index references)
UPSTREAM_ASSET = ["ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT",
                  "ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT",
                  "ingestion.MORT_MTH_SNAPSHOT",
                  ]

DOWNSTREAM_ASSET = "features.STEP_MONTHLY_MAX_DELQ"
DEPENDENCIES = {
    "export_all": ["duckdb_delete"],
    "duckdb_delete": ["duckdb_load"],
}
SRC_PRD_LKP = "reference.SRC_PRD_LKP"


def export_all(
    duckdb_conn_id="duckdb-conn",
    sql=rf"""
        SELECT 
            CAST(NULLIF(TRIM(STEP_PLN_AGRMNT_NUM), '') AS BIGINT) AS STEP_PLN_AGRMNT_NUM,
            CAST(DAY_ODUE AS INTEGER) AS STEP_DELQ,
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        FROM { UPSTREAM_ASSET[0] } 
        WHERE MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
        AND PRIM_BASEL_CUST_ID is not null 
        AND PRIM_BASEL_CUST_ID <> -1 

        UNION
        SELECT  
            CAST(NULLIF(TRIM(STEP_PLN_AGRMNT_NUM), '') AS BIGINT) AS STEP_PLN_AGRMNT_NUM,
            CAST(BNS_DLQNT_DAY AS INTEGER) AS STEP_DELQ, 
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        FROM { UPSTREAM_ASSET[1] } 
        WHERE MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
        AND PRIM_BASEL_CUST_ID is not null 
        AND PRIM_BASEL_CUST_ID <> -1 

        UNION
        SELECT 
            CAST(NULLIF(TRIM(STEP_PLN_AGRMNT_NUM), '') AS BIGINT) AS STEP_PLN_AGRMNT_NUM,
            DLQNT_DAY AS STEP_DELQ,
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        FROM { UPSTREAM_ASSET[2] } 
        WHERE MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
        AND UPPER(TRIM(COMM_TP))='RESIDENTIAL'
        AND CRNT_BAL_AMT > 0
        AND TRIM(PD_OFF_F)='N'
    """,
):
    pass

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
            OBSN_DT, 
            STEP_PLN_AGRMNT_NUM, 
            MAX(STEP_DELQ) AS STEP_MONTHLY_MAX_DELQ
        FROM '{{{{ task_instance.xcom_pull(task_ids="derived__step_monthly_max_delq.export_all", key="parquet") }}}}'
        GROUP BY OBSN_DT, STEP_PLN_AGRMNT_NUM
    )
    """,
):
    pass


# def export_spl(
#     duckdb_conn_id="duckdb-conn",
#     sql=f"""
#         SELECT 
#             BASEL_ACCT_ID, 
#             CAST(DAY_ODUE AS INTEGER) AS DAY_ODUE, 
#             PRIM_BASEL_CUST_ID AS BASEL_CUST_ID,
#             '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
#         FROM { UPSTREAM_ASSET[0] } 
#         WHERE MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
#         AND PRIM_BASEL_CUST_ID is not null 
#         AND PRIM_BASEL_CUST_ID <> -1 
#     """,
# ):
#     pass

# def export_ks(
#     duckdb_conn_id="duckdb-conn",
#     sql=f"""
#         SELECT 
#             BASEL_ACCT_ID, 
#             CAST(BNS_DLQNT_DAY AS INTEGER) AS BNS_DLQNT_DAY, 
#             PRIM_BASEL_CUST_ID AS BASEL_CUST_ID,
#             '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
#         FROM { UPSTREAM_ASSET[1] } 
#         WHERE MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
#         AND PRIM_BASEL_CUST_ID is not null 
#         AND PRIM_BASEL_CUST_ID <> -1 
#     """,
# ):
#     pass

# def export_mor(
#     duckdb_conn_id="duckdb-conn",
#     sql=rf"""
#         SELECT 
#             BASEL_ACCT_ID, 
#             DLQNT_DAY,
#             MORT_NUM, 
#             PRIM_BASEL_CUST_ID AS BASEL_CUST_ID,
#             '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
#         FROM { UPSTREAM_ASSET[2] } 
#         WHERE MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
#         AND UPPER(TRIM(COMM_TP))='RESIDENTIAL'
#         AND CRNT_BAL_AMT > 0
#         AND TRIM(PD_OFF_F)='N'
#     """,
# ):
#     pass