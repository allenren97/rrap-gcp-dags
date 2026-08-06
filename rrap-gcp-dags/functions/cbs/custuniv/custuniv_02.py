UPSTREAM_ASSET = [
    "cbs.CIS_DATA_POP_02",
    "ingestion.CR_BUREAU_DELI_MTH_SNAPSHOT",
    "emulated.CBS_MDM_FLAGS",
]

DOWNSTREAM_ASSET = "cbs.CUST_BASE_05"

_TASK_GROUP = "custuniv__custuniv_02"

DEPENDENCIES = {
    "export_gather": ["export_result"],
    "export_result": ["duckdb_load"],
    "duckdb_delete": ["duckdb_load"],
}


def duckdb_delete(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    DELETE FROM {DOWNSTREAM_ASSET}
    WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
      AND STREAM = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}'
    """,
):
    pass


def export_gather(
    duckdb_conn_id="duckdb-conn",
    resource_tier="HIGH",
    pool_slots=96,
    sql=f"""
    WITH
        cust_base_01 AS (
            SELECT
                cid,
                cid_num,
                mth_tm_id,
                process_date,
                COUNT(1)                                                               AS num_prods,
                SUM(lend_prods_CUR)                                                    AS num_lend_prods_CUR,
                SUM(lend_prods_CLO)                                                    AS num_lend_prods_CLO,
                SUM(lend_prods_BNK)                                                    AS num_lend_prods_BNK,
                SUM(lend_prods_DEF)                                                    AS num_lend_prods_DEF,
                SUM(lend_prods_CHG)                                                    AS num_lend_prods_CHG,
                SUM(lend_prods_WO)                                                     AS num_lend_prods_WO,
                SUM(lend_prods_COMM)                                                   AS num_lend_prods_COMM,
                SUM(lend_prods_COMM_CUR)                                               AS num_lend_prods_COMM_CUR,
                SUM(lend_prods_COMM_CLO)                                               AS num_lend_prods_COMM_CLO,
                SUM(lend_prods_COMM_CHG)                                               AS num_lend_prods_COMM_CHG,
                SUM(lend_prods_COMM_DEF)                                               AS num_lend_prods_COMM_DEF,
                SUM(lend_prods_COMM_WO)                                                AS num_lend_prods_COMM_WO,
                SUM(PRIVATE_BANK_IND)                                                  AS PRIVATE_BANK_IND,
                SUM(mor_ind)                                                           AS num_mor,
                SUM(spl_ind)                                                           AS num_spl,
                SUM(rev_ind)                                                           AS num_rev,
                SUM(ssl_ind)                                                           AS num_ssl,
                SUM(lend_prods_CUR) + SUM(lend_prods_DEF)
                    + SUM(lend_prods_COMM_CUR) + SUM(lend_prods_COMM_DEF)             AS num_lend_prods,
                MAX(days_dlq)                                                          AS worst_dlq_days,
                MAX(CASE WHEN PRIMARY_FLAG = 'Y' THEN 1 ELSE 0 END)                     AS prime_ind,
                MAX(CASE WHEN PRIMARY_FLAG = 'N' THEN 1 ELSE 0 END)                     AS secondary_ind,
                MAX(CASE WHEN PRIMARY_FLAG = 'Y' AND lend_prods = 1 THEN 1 ELSE 0 END)  AS prime_ind_lend,
                MAX(CASE WHEN PRIMARY_FLAG = 'N' AND lend_prods = 1 THEN 1 ELSE 0 END)  AS secondary_ind_lend
            FROM cbs.CIS_DATA_POP_02
            WHERE OBSN_DT = DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
              AND STREAM = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}'
            GROUP BY cid, cid_num, mth_tm_id, process_date
        ),
        cust_base_02_raw AS (
            SELECT
                a.*,
                c.basel_cust_id,
                c.cust_cid,
                c.HIT_NOHIT_EDIT_REJCT_CD,
                c.FICO_08_SCORE,
                c.FICO_08_EXCLSN_CD,
                CASE WHEN c.cust_cid IS NOT NULL THEN 1 ELSE 0 END AS bureau_exist,
                ROW_NUMBER() OVER (
                    PARTITION BY a.cid ORDER BY c.SCORE_LAST_RECVD_DT DESC
                ) AS row_num
            FROM cust_base_01 a
            LEFT JOIN ingestion.CR_BUREAU_DELI_MTH_SNAPSHOT c
                ON a.cid = c.cust_cid
               AND a.mth_tm_id = c.mth_tm_id
               AND c.mth_tm_id = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
        ),
        cust_base_02 AS (
            SELECT * EXCLUDE (row_num) FROM cust_base_02_raw WHERE row_num = 1
        ),
        cust_base_04 AS (
            SELECT DISTINCT
                a.*,
                b.cust_type,
                b.cust_status,
                b.deceased_ind,
                b.BNKRPTCY_FLAG,
                b.UNDER_18_FLAG,
                CASE WHEN b.cust_type = 'Retail' THEN 1 ELSE NULL END AS retail_ind
            FROM cust_base_02 a
            LEFT JOIN emulated.CBS_MDM_FLAGS b
                ON TRY_CAST(b.party_id AS BIGINT) IS NOT NULL
               AND TRY_CAST(a.cid AS BIGINT) = TRY_CAST(b.party_id AS BIGINT)
               AND b.EFF_DT = DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
        )
    SELECT * FROM cust_base_04
    """,
):
    pass


def export_result(
    duckdb_conn_id="duckdb-conn",
    resource_tier="HIGH",
    pool_slots=96,
    sql=f"""
    WITH
        g AS (
            SELECT *
            FROM read_parquet(
                '{{{{ task_instance.xcom_pull(task_ids="{_TASK_GROUP}.export_gather", key="parquet") }}}}'
            )
        )
    SELECT
        DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}' AS STREAM,
        *
    FROM g
    """,
):
    pass


def duckdb_load(
    duckdb_conn_id="duckdb-conn",
    resource_tier="HIGH",
    pool_slots=96,
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} BY NAME
    SELECT *
    FROM read_parquet(
        '{{{{ task_instance.xcom_pull(task_ids="{_TASK_GROUP}.export_result", key="parquet") }}}}'
    )
    """,
):
    pass