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
    "features.DLQNT_DAY_CNT",
]
DOWNSTREAM_ASSET = "features.CUST22_V2"
DEPENDENCIES = {
    "export_ks": ["duckdb_delete"],
    "export_mor": ["duckdb_delete"],
    "export_spl": ["duckdb_delete"],
    "duckdb_delete": ["duckdb_load"],
}


def export_ks(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        SELECT SUM(
            CASE
                WHEN TRIM(CHRG_OFF_CD) <> '1' and (TRIM(BLOCK_RECL_CD)='B5' or TRIM(CHRG_OFF_CD) in ('N','Q') or BNS_DLQNT_DAY >= 30)
                THEN b.TOT_NEW_BAL_AMT
                ELSE 0
            END
        ) AS CUST22_V2, a.BASEL_CUST_ID,
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        FROM { UPSTREAM_ASSET[0] } a
        INNER JOIN { UPSTREAM_ASSET[1] } b
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
        SELECT SUM(
            CASE
                -- last business day
                WHEN FRCLSR_F = 'Y' OR 
                    (
                        CAST(FUND_CD AS INTEGER) BETWEEN 2000 AND 2199 OR
                        CAST(FUND_CD AS INTEGER) BETWEEN 2202 AND 2249 OR
                        CAST(FUND_CD AS INTEGER) BETWEEN 6490 AND 6499
                    )
                THEN 0
                WHEN c.DLQNT_DAY_CNT >= 90 AND (FRCLSR_F <> 'Y' OR FRCLSR_F IS NULL) AND NOT
                    (
                        CAST(FUND_CD AS INTEGER) BETWEEN 2000 AND 2199 OR
                        CAST(FUND_CD AS INTEGER) BETWEEN 2202 AND 2249 OR
                        CAST(FUND_CD AS INTEGER) BETWEEN 6490 AND 6499
                    )
                THEN CRNT_BAL_AMT+INTR_ACCR_AMT
                WHEN c.DLQNT_DAY_CNT > 0 AND c.DLQNT_DAY_CNT < 90 AND (
                        (FRCLSR_F <> 'Y' OR FRCLSR_F IS NULL) OR NOT
                        (
                            CAST(FUND_CD AS INTEGER) BETWEEN 2000 AND 2199 OR
                            CAST(FUND_CD AS INTEGER) BETWEEN 2202 AND 2249 OR
                            CAST(FUND_CD AS INTEGER) BETWEEN 6490 AND 6499
                        )
                    )
                THEN CRNT_BAL_AMT+INTR_ACCR_AMT
                ELSE 0
            END
        ) AS CUST22_V2, a.BASEL_CUST_ID,
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        FROM { UPSTREAM_ASSET[0] } a
        INNER JOIN { UPSTREAM_ASSET[2] } b
        ON a.BASEL_ACCT_ID = b.BASEL_ACCT_ID
        AND a.MTH_TM_ID = b.MTH_TM_ID
        INNER JOIN (
            SELECT * FROM { UPSTREAM_ASSET[4] }
            WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
            AND SRC_SYS_CD = 'MO'
        ) c -- features.DLQNT_DAY_CNT
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
        SELECT SUM(
            CASE
                WHEN 
                    RECD_STAT_CD = 5 
                    OR (RECD_STAT_CD = 4 AND DAY_ODUE >= 90)
                    OR (RECD_STAT_CD IN (4,5) AND DAY_ODUE > 0)
                THEN round(TOT_CRNT_BAL_AMT + ADD_ON_BAL_AMT + ACCR_INTR,3)
                ELSE 0
            END
        ) AS CUST22_V2, a.BASEL_CUST_ID,
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        FROM { UPSTREAM_ASSET[0] } a
        INNER JOIN { UPSTREAM_ASSET[3] } b
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
        select OBSN_DT, BASEL_CUST_ID, SUM(CUST22_V2) AS CUST22_V2
        from read_parquet(['{{{{ task_instance.xcom_pull(task_ids="derived__cust22_v2.export_ks", key="parquet") }}}}','{{{{ task_instance.xcom_pull(task_ids="derived__cust22_v2.export_mor", key="parquet") }}}}','{{{{ task_instance.xcom_pull(task_ids="derived__cust22_v2.export_spl", key="parquet") }}}}'], union_by_name = true)
        group by OBSN_DT, BASEL_CUST_ID
    )
    """,
):
    pass
