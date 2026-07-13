from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)


UPSTREAM_ASSET = ["ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT",
                  "ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT",
                  "ingestion.MORT_MTH_SNAPSHOT"
    
]
DOWNSTREAM_ASSET = "features.HGST_DLQNT_DAY_COMB_MAX24M"
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
    INSERT INTO {DOWNSTREAM_ASSET} 
    WITH KS_accts_pre_1 AS (
        SELECT a.basel_acct_id, a.prim_basel_cust_id 
        FROM  {UPSTREAM_ASSET[0]} a
        WHERE
            a.mth_tm_id = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
            and prim_basel_cust_id > 0
    ),
    ks_accts_pre_2 AS (
            SELECT
                    BASEL_ACCT_ID,
                    PRIM_BASEL_CUST_ID,
                    COALESCE(
                    (
                            CASE WHEN BNS_DLQNT_DAY < 0 THEN 0
                            WHEN BNS_DLQNT_DAY >= 0
                            AND BNS_DLQNT_DAY<31 THEN 0
                            WHEN BNS_DLQNT_DAY >= 31 THEN BNS_DLQNT_DAY - 30
                    END),
                            0) AS BNS_DLQNT_DAY
        FROM {UPSTREAM_ASSET[0]}
            WHERE mth_tm_id <= {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}} 
            and mth_tm_id >= {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}} -920
        and (basel_acct_id, prim_basel_cust_id) in (SELECT * FROM ks_accts_pre_1)
    ),
    ks_accts AS (
        SELECT
            a.basel_acct_id,
            a.PRIM_BASEL_CUST_ID,
            MAX(a.BNS_DLQNT_DAY) AS max_dlqnt_ks
            FROM ks_accts_pre_2 a
        GROUP BY a.basel_acct_id, a.PRIM_BASEL_CUST_ID
    ),

    coal_max_dlqnt_cc AS (
        SELECT 
            basel_acct_id,
            PRIM_BASEL_CUST_ID,
            COALESCE(max_dlqnt_ks, 0) AS max_dlqnt_day_24m_ks
        FROM ks_accts
    ),
    spl_accts_pre AS (
            SELECT basel_acct_id, prim_basel_cust_id
            from {UPSTREAM_ASSET[1]}
            where mth_tm_id = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}} 
            and TOT_CRNT_BAL_AMT > 0
    ),
    spl_accts AS (
        SELECT
            a.basel_acct_id,
            a.PRIM_BASEL_CUST_ID,
            CASE
                    WHEN (a.basel_acct_id, a.prim_basel_cust_id) IN (SELECT * FROM spl_accts_pre)
                    THEN MAX(a.DAY_ODUE)
                    ELSE 0
            END as max_dlqnt_spl
        FROM {UPSTREAM_ASSET[1]} a
        WHERE a.mth_tm_id <= {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}} 
        and a.mth_tm_id >= {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}} -920
        and prim_basel_cust_id > 0
        GROUP BY a.basel_acct_id, a.PRIM_BASEL_CUST_ID
    ),
    coal_max_dlqnt_spl AS (
        SELECT 
            basel_acct_id,
            PRIM_BASEL_CUST_ID,
            COALESCE(max_dlqnt_spl, 0) AS max_dlqnt_day_24m_spl
        FROM spl_accts
    ),
    mort_accts_pre AS (
            SELECT basel_acct_id, prim_basel_cust_id
            from {UPSTREAM_ASSET[2]} a
            where a.mth_tm_id = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
            and UPPER(TRIM(a.COMM_TP))= 'RESIDENTIAL'
            and a.CRNT_BAL_AMT > 0
            AND TRIM(a.PD_OFF_F)= 'N'
    ),
    mort_accts AS (
        SELECT
            a.basel_acct_id,
            a.PRIM_BASEL_CUST_ID,
            CASE
                    WHEN (a.basel_acct_id, a.prim_basel_cust_id) in (select * from mort_accts_pre)
                    THEN MAX(a.dlqnt_day)
                    ELSE 0
            END AS max_dlqnt_mort
        FROM {UPSTREAM_ASSET[2]} a
        WHERE a.mth_tm_id <= {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}} 
        and a.mth_tm_id >= {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}} -920
        and prim_basel_cust_id > 0
        GROUP BY a.basel_acct_id, a.PRIM_BASEL_CUST_ID
    ),

    coal_max_dlqnt_mort AS (
        SELECT 
            basel_acct_id,
            PRIM_BASEL_CUST_ID,
            COALESCE(max_dlqnt_mort, 0) AS max_dlqnt_day_24m_mort
        FROM mort_accts
    ),
    hgst_dlqnt_day_combo AS (
        SELECT 
            COALESCE(a.basel_acct_id, b.basel_acct_id, c.basel_acct_id) AS basel_acct_id,
            COALESCE(a.PRIM_BASEL_CUST_ID, b.PRIM_BASEL_CUST_ID, c.PRIM_BASEL_CUST_ID) AS PRIM_BASEL_CUST_ID,
            GREATEST(
                CAST(a.max_dlqnt_day_24m_ks AS DECIMAL(17,3)),
                CAST(b.max_dlqnt_day_24m_spl AS DECIMAL(17,3)),
                CAST(c.max_dlqnt_day_24m_mort AS DECIMAL(17,3))
            ) AS dlqnt_day_24m
        FROM coal_max_dlqnt_cc a
        FULL OUTER JOIN coal_max_dlqnt_spl b ON a.basel_acct_id = b.basel_acct_id
        FULL OUTER JOIN coal_max_dlqnt_mort c ON COALESCE(a.basel_acct_id, b.basel_acct_id) = c.basel_acct_id
    ),
    twenty_four_month_max AS (
        SELECT 
            PRIM_BASEL_CUST_ID,
            MAX(dlqnt_day_24m) AS max_dlqnt_day_24m
        FROM hgst_dlqnt_day_combo
        GROUP BY PRIM_BASEL_CUST_ID
    )
    SELECT 
        PRIM_BASEL_CUST_ID AS basel_cust_id,
        DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS obsn_dt,
        max_dlqnt_day_24m AS hgst_dlqnt_day_comb_max24m
    FROM twenty_four_month_max

    """,
):
    pass


