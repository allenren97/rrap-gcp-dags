from airflow.sdk import get_current_context
from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)
# Upstream assets (table names with index references)
UPSTREAM_ASSET = [
    "ingestion.BASEL_CUST_ACCT_RLTNP_SNAPSHOT",   # [0]
    "ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT",    # [1]
    "ingestion.MORT_MTH_SNAPSHOT",                # [2]
    "ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT",     # [3]
    "features.DLQNT_DAY_CNT",                     # [4]
    "ingestion.TM_DIM",                           # [5]
]
DOWNSTREAM_ASSET = "features.CUST13"
DEPENDENCIES = {
    "export_ks": ["duckdb_delete"],
    "export_mor": ["duckdb_delete"],
    "export_spl": ["duckdb_delete"],
    "duckdb_delete": ["duckdb_load"],
}
SRC_PRD_LKP = "reference.SRC_PRD_LKP"


def export_ks(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        SELECT MAX(c.DLQNT_DAY_CNT) AS cust13, a.BASEL_CUST_ID,
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        FROM { UPSTREAM_ASSET[0] } a  -- ingestion.BASEL_CUST_ACCT_RLTNP_SNAPSHOT
        INNER JOIN { UPSTREAM_ASSET[1] } b  -- ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT
        ON a.BASEL_ACCT_ID = b.BASEL_ACCT_ID
        AND a.MTH_TM_ID = b.MTH_TM_ID
        INNER JOIN (
            SELECT * FROM { UPSTREAM_ASSET[4] } 
            WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
        ) c -- features.DLQNT_DAY_CNT
        ON b.BASEL_ACCT_ID = c.BASEL_ACCT_ID
        where a.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
        and b.prim_basel_cust_id is not null and b.prim_basel_cust_id <> -1 
        and (b.TOT_NEW_BAL_AMT > 0 or b.CR_LMT_AMT > 0) and
        TRIM(b.PRD_CD) not in ('BLV') and
        TRIM(b.SUB_PRD_CD) not in ('CC') and 
        TRIM(b.PRD_CD) in ('VIC','SCL','VLR','VGD','VCL','LOC','SSL','')
        GROUP BY a.BASEL_CUST_ID
    """,
):
    pass

def export_mor(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        SELECT MAX(c.DLQNT_DAY_CNT) AS cust13, a.BASEL_CUST_ID,
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        FROM { UPSTREAM_ASSET[0] } a  -- ingestion.BASEL_CUST_ACCT_RLTNP_SNAPSHOT
        INNER JOIN { UPSTREAM_ASSET[2] } b  -- ingestion.MORT_MTH_SNAPSHOT
        ON a.BASEL_ACCT_ID = b.BASEL_ACCT_ID
        AND a.MTH_TM_ID = b.MTH_TM_ID
        INNER JOIN (
            SELECT * FROM { UPSTREAM_ASSET[4] } WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' 
        ) c  -- features.DLQNT_DAY_CNT
        ON b.BASEL_ACCT_ID = c.BASEL_ACCT_ID
        WHERE b.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
        AND b.PRIM_BASEL_CUST_ID IS NOT NULL
        AND b.PRIM_BASEL_CUST_ID <> -1
        AND b.CRNT_BAL_AMT > 0
        AND b.PD_OFF_F = 'N'
        AND SUBSTR(b.SCRTY_TP_2, 1, 1) <> '6'
        AND (CASE WHEN TRIM(SCRTY_TP_2) IN ('', '00 0') THEN 100 ELSE CAST(SUBSTR(TRIM(SCRTY_TP_2), LENGTH(TRIM(SCRTY_TP_2)) - 2, 3) AS INTEGER) END) < 5
        GROUP BY a.BASEL_CUST_ID
    """,
):
    pass

def export_spl(
    duckdb_conn_id="duckdb-conn",
    sql=rf"""
        SELECT MAX(c.DLQNT_DAY_CNT) AS cust13, a.BASEL_CUST_ID,
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        FROM { UPSTREAM_ASSET[0] } a  -- ingestion.BASEL_CUST_ACCT_RLTNP_SNAPSHOT
        INNER JOIN { UPSTREAM_ASSET[3] } b  -- ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT
        ON a.BASEL_ACCT_ID = b.BASEL_ACCT_ID
        AND a.MTH_TM_ID = b.MTH_TM_ID
        INNER JOIN (
            SELECT * FROM { UPSTREAM_ASSET[4] } WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
        ) c -- features.DLQNT_DAY_CNT
        ON b.BASEL_ACCT_ID = c.BASEL_ACCT_ID
        INNER JOIN { UPSTREAM_ASSET[5] } d -- ingestion.TM_DIM
        ON a.MTH_TM_ID = d.TM_ID
        AND d.TM_LVL_END_DT = c.OBSN_DT
        WHERE b.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
        AND b.PRIM_BASEL_CUST_ID IS NOT NULL
        AND b.PRIM_BASEL_CUST_ID <> -1
        AND b.RECD_STAT_CD IN (4,5,6,7,8)
        GROUP BY a.BASEL_CUST_ID
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
        select OBSN_DT, BASEL_CUST_ID, MAX(cust13) AS cust13 
        from read_parquet(['{{{{ task_instance.xcom_pull(task_ids="derived__cust13.export_ks", key="parquet") }}}}','{{{{ task_instance.xcom_pull(task_ids="derived__cust13.export_mor", key="parquet") }}}}','{{{{ task_instance.xcom_pull(task_ids="derived__cust13.export_spl", key="parquet") }}}}'], union_by_name = true)
        group by OBSN_DT, BASEL_CUST_ID
    )
    """,
):
    pass

