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
    "features.COMM_TP_CD",                        # [5]
    "ingestion.TM_DIM",                           # [6]
]
DOWNSTREAM_ASSET = "features.CUST18"
DEPENDENCIES = {
    "export_cust_list": ['duckdb_delete'],
    "export_ks": ["duckdb_delete"],
    "export_mor": ["duckdb_delete"],
    "export_spl": ["duckdb_delete"],
    "duckdb_delete": ["duckdb_load"],
}
SRC_PRD_LKP = "reference.SRC_PRD_LKP"

def export_cust_list(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    SELECT distinct m.prim_basel_cust_id as basel_cust_id
    from {UPSTREAM_ASSET[3]} as m
    where (m.mth_tm_id = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}) and 
        m.prim_basel_cust_id is not null and m.prim_basel_cust_id <> -1 and
        m.RECD_STAT_CD in (4,5,6,7,8)
    
    UNION

    select distinct m.prim_basel_cust_id as basel_cust_id
    from {UPSTREAM_ASSET[2]} as m,
    where (m.mth_tm_id = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}) and
        m.prim_basel_cust_id is not null and m.prim_basel_cust_id <> -1 and
	    m.CRNT_BAL_AMT <> 0 and
        TRIM(m.PD_OFF_F) ='N' and
        UPPER(TRIM(m.COMM_TP))='RESIDENTIAL'

    UNION

    select distinct m.prim_basel_cust_id as basel_cust_id
    from {UPSTREAM_ASSET[1]} as m
    where (m.mth_tm_id = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}) and
        m.prim_basel_cust_id is not null and m.prim_basel_cust_id <> -1 and
	    (m.TOT_NEW_BAL_AMT > 0 or m.CR_LMT_AMT > 0) and
        TRIM(m.PRD_CD) not in ('BLV') and
        TRIM(m.SUB_PRD_CD) not in ('CC') and 
        TRIM(m.PRD_CD) in ('VIC','SCL','VLR','VGD','VCL','LOC','SSL','')
    """
):
    pass

def export_ks(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        SELECT SUM(
            CASE
                WHEN TRIM(CHRG_OFF_CD) IS DISTINCT FROM '1' AND BNS_DLQNT_DAY < 30 AND TRIM(BLOCK_RECL_CD) <> 'B5' AND TRIM(CHRG_OFF_CD) NOT IN ('N', 'Q') THEN 1
                ELSE 0
            END
        )
         AS cust18, a.BASEL_CUST_ID
        FROM { UPSTREAM_ASSET[0] } a  -- ingestion.BASEL_CUST_ACCT_RLTNP_SNAPSHOT
        INNER JOIN { UPSTREAM_ASSET[1] } b  -- ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT
        ON a.BASEL_ACCT_ID = b.BASEL_ACCT_ID
        AND a.MTH_TM_ID = b.MTH_TM_ID
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
        SELECT SUM(CASE WHEN c.DLQNT_DAY_CNT = 0 THEN 1 ELSE 0 END) AS cust18, a.BASEL_CUST_ID
        FROM { UPSTREAM_ASSET[0] } a  -- ingestion.BASEL_CUST_ACCT_RLTNP_SNAPSHOT
        INNER JOIN { UPSTREAM_ASSET[2] } b  -- ingestion.MORT_MTH_SNAPSHOT
        ON a.BASEL_ACCT_ID = b.BASEL_ACCT_ID
        AND a.MTH_TM_ID = b.MTH_TM_ID
        INNER JOIN (
            select * from { UPSTREAM_ASSET[4] }  -- features.DLQNT_DAY_CNT
            where obsn_dt = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
            and src_sys_cd = 'MO'
        ) c
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
        SELECT SUM(CASE WHEN RECD_STAT_CD = 4 AND DAY_ODUE = 0 THEN 1 ELSE 0 END) AS cust18, a.BASEL_CUST_ID
        FROM { UPSTREAM_ASSET[0] } a  -- ingestion.BASEL_CUST_ACCT_RLTNP_SNAPSHOT
        INNER JOIN { UPSTREAM_ASSET[3] } b  -- ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT
        ON a.BASEL_ACCT_ID = b.BASEL_ACCT_ID
        AND a.MTH_TM_ID = b.MTH_TM_ID
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
        with derived_inputs as (
            select BASEL_CUST_ID, CUST18
            from read_parquet(['{{{{ task_instance.xcom_pull(task_ids="derived__cust18.export_ks", key="parquet") }}}}','{{{{ task_instance.xcom_pull(task_ids="derived__cust18.export_mor", key="parquet") }}}}','{{{{ task_instance.xcom_pull(task_ids="derived__cust18.export_spl", key="parquet") }}}}'], union_by_name = true)
        )
        select
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            scrd_list.BASEL_CUST_ID,
            SUM(derived_inputs.CUST18) AS CUST18
        from '{{{{ task_instance.xcom_pull(task_ids="derived__cust18.export_cust_list", key="parquet") }}}}' scrd_list
        inner join derived_inputs
        on scrd_list.BASEL_CUST_ID = derived_inputs.BASEL_CUST_ID
        group by scrd_list.BASEL_CUST_ID
    )
    """,
):
    pass

