from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

UPSTREAM_ASSET = [
    "ingestion.BASEL_CUST_ACCT_RLTNP_SNAPSHOT",
    "ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT",
    "ingestion.MORT_MTH_SNAPSHOT",
    "ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT",
    "features.CR_LMT_AMT",
    "features.TOT_NEW_BAL_AMT"
]
DOWNSTREAM_ASSET = "features.CUST24"
DEPENDENCIES = {
    "export_cust_list": ["duckdb_delete"],
    "export_derived_list": ["duckdb_delete"],
    "export_cust24": ["duckdb_delete"],
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
    
    UNION ALL

    select distinct m.prim_basel_cust_id as basel_cust_id
    from {UPSTREAM_ASSET[2]} as m,
    where (m.mth_tm_id = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}) and
        m.prim_basel_cust_id is not null and m.prim_basel_cust_id <> -1 and
	    m.CRNT_BAL_AMT <> 0 and
        TRIM(m.PD_OFF_F) ='N' and
        UPPER(TRIM(m.COMM_TP))='RESIDENTIAL'

    UNION ALL

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

def export_derived_list(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    SELECT
        a.BASEL_CUST_ID AS PRIM_BASEL_CUST_ID,
        a.MTH_TM_ID
        FROM {UPSTREAM_ASSET[0]} a
        INNER JOIN {UPSTREAM_ASSET[3]} b
            ON a.BASEL_ACCT_ID = b.BASEL_ACCT_ID
            AND a.MTH_TM_ID = b.MTH_TM_ID
        WHERE b.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
            AND b.PRIM_BASEL_CUST_ID IS NOT NULL
            AND b.PRIM_BASEL_CUST_ID <> -1
            AND b.RECD_STAT_CD IN (4,5,6,7,8)
        GROUP BY a.BASEL_CUST_ID, a.MTH_TM_ID

    UNION ALL

    SELECT 
        a.BASEL_CUST_ID AS PRIM_BASEL_CUST_ID,
        a.MTH_TM_ID
        FROM {UPSTREAM_ASSET[0]} a
        INNER JOIN {UPSTREAM_ASSET[1]} b
            ON a.BASEL_ACCT_ID = b.BASEL_ACCT_ID
            AND a.MTH_TM_ID = b.MTH_TM_ID
        where a.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
            and b.PRIM_BASEL_CUST_ID is not null and b.PRIM_BASEL_CUST_ID <> -1 
            and (b.TOT_NEW_BAL_AMT > 0 or b.CR_LMT_AMT > 0) and
            TRIM(b.PRD_CD) not in ('BLV') and
            TRIM(b.SUB_PRD_CD) not in ('CC') and 
            TRIM(b.PRD_CD) in ('VIC','SCL','VLR','VGD','VCL','LOC','SSL','')
        GROUP BY a.BASEL_CUST_ID, a.MTH_TM_ID

    UNION ALL

    SELECT 
        a.BASEL_CUST_ID AS PRIM_BASEL_CUST_ID,
        a.MTH_TM_ID
        FROM {UPSTREAM_ASSET[0]} a
        INNER JOIN {UPSTREAM_ASSET[2]} b
            ON a.BASEL_ACCT_ID = b.BASEL_ACCT_ID
            AND a.MTH_TM_ID = b.MTH_TM_ID
        WHERE b.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
            AND b.PRIM_BASEL_CUST_ID IS NOT NULL
            AND b.PRIM_BASEL_CUST_ID <> -1
            AND b.CRNT_BAL_AMT <> 0
            AND TRIM(b.PD_OFF_F) = 'N'
            AND UPPER(TRIM(b.COMM_TP))='RESIDENTIAL'
        GROUP BY a.BASEL_CUST_ID, a.MTH_TM_ID
    """
):
    pass

def export_cust24(
    duckdb_conn_id="duckdb-conn",
    sql=rf"""
        WITH cust24 AS (
            SELECT
                a.MTH_TM_ID,
                a.BASEL_CUST_ID,
                SUM(CASE 
                        WHEN cr.CR_LMT_AMT <=0 AND new_bal.TOT_NEW_BAL_AMT < 0 
                        THEN 0 ELSE new_bal.TOT_NEW_BAL_AMT 
                    END) 
                AS SUM_TOT_NEW_BAL_AMT,
                SUM(cr.CR_LMT_AMT) AS SUM_CR_LMT_AMT,
                CASE 
                    WHEN SUM_TOT_NEW_BAL_AMT IS NULL OR SUM_CR_LMT_AMT IS NULL THEN NULL
                    WHEN SUM_CR_LMT_AMT = 0 THEN 0
                ELSE TRUNC(SUM_TOT_NEW_BAL_AMT / SUM_CR_LMT_AMT, 4)
                END AS CUST24
            FROM {UPSTREAM_ASSET[0]} AS a
            LEFT JOIN {UPSTREAM_ASSET[1]} AS b ON
                a.MTH_TM_ID = b.MTH_TM_ID AND
                a.BASEL_ACCT_ID = b.BASEL_ACCT_ID AND
                TRIM(b.PRD_CD) <> 'BLV' AND 
                TRIM(b.SUB_PRD_CD) NOT IN ('CC') AND 
                TRIM(b.PRD_CD) in ('VIC','SCL','VLR','VGD','VCL','LOC','SSL','')
            LEFT JOIN (SELECT * FROM {UPSTREAM_ASSET[4]} WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}') AS cr ON
                b.BASEL_ACCT_ID = cr.BASEL_ACCT_ID
            LEFT JOIN (SELECT * FROM {UPSTREAM_ASSET[5]} WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}') AS new_bal ON
                b.BASEL_ACCT_ID = new_bal.BASEL_ACCT_ID
            WHERE 
                a.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}} AND
                a.BASEL_CUST_ID IS NOT NULL AND a.BASEL_CUST_ID <> -1 AND
                (b.TOT_NEW_BAL_AMT > 0 or b.CR_LMT_AMT > 0)
            GROUP BY  
                a.BASEL_CUST_ID,
                a.MTH_TM_ID
        )
        SELECT
            MTH_TM_ID,
            BASEL_CUST_ID,
            CUST24
        FROM cust24
    """,
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
        SELECT DISTINCT
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            scrd.BASEL_CUST_ID,
            cust24.CUST24
        FROM read_parquet('{{{{ task_instance.xcom_pull(task_ids="derived__cust24.export_cust_list", key="parquet") }}}}') scrd
        INNER JOIN read_parquet('{{{{ task_instance.xcom_pull(task_ids="derived__cust24.export_derived_list", key="parquet") }}}}') derived ON
            scrd.BASEL_CUST_ID = derived.PRIM_BASEL_CUST_ID
        LEFT JOIN read_parquet('{{{{ task_instance.xcom_pull(task_ids="derived__cust24.export_cust24", key="parquet") }}}}') cust24 ON
            derived.PRIM_BASEL_CUST_ID = cust24.BASEL_CUST_ID
            AND derived.MTH_TM_ID = cust24.MTH_TM_ID
        WHERE derived.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    )
    """,
):
    pass

