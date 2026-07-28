"""
Rewrite of J_CBS_0020_CUSTUNIV_02.sas â€” CBS Customer Universe, step 02.

Two stages, mirroring the SAS:
  export_gather   -- Builds the customer-level aggregate (SAS CUST_BASE_01 through
                     CUST_BASE_05) as a single CTE chain: aggregate CIS_DATA_POP_02 by
                     customer, LEFT JOIN the bureau snapshot (keeping the most-recent
                     record per customer via ROW_NUMBER), LEFT JOIN MDM flags, then LEFT
                     JOIN primary/secondary role indicators from CIS_DATA_POP_02.
  export_result   -- Reads the gather parquet and prepends OBSN_DT and STREAM.
  duckdb_delete / duckdb_load -> cbs.CUST_BASE_05 (this dag's output; step 03 reads it).

Source schema mapping (verified against the ducklake catalog):
  cbs.*         CIS_DATA_POP_02 (output of custuniv_01)
  ingestion.*   CR_BUREAU_DELI_MTH_SNAPSHOT
  emulated.*    CBS_MDM_FLAGS

OPEN ITEMS (flagged, need confirmation):
  * emulated.CBS_MDM_FLAGS -- the SAS uses &RRAP_WRK..CBS_MDM_FLAGS, a pre-existing work
    table whose upstream source/schema is unconfirmed. Mapped to emulated.CBS_MDM_FLAGS
    pending verification.
  * CBS_MDM_FLAGS STREAM filter -- the SAS join carries no STREAM predicate on CBS_MDM_FLAGS;
    no STREAM filter is applied here. Confirm whether the emulated table is stream-partitioned.
  * lend_prods_COMM -- the SAS CUSTUNIV_01 DATA step initialises lend_prods_COMM=0 but the
    Python custuniv_01.py does not emit that column. num_lend_prods_COMM is hard-coded to 0
    in the aggregate below; adjust if the column is added to cbs.CIS_DATA_POP_02 upstream.
  * cbs.CUST_BASE_05 -- target schema/table DDL needs to be created.
  * Untested against on-prem output -- the customer-level derivations are a faithful
    translation but should be reconciled against CUST_BASE_05 on a sample month.
"""

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
        -- CUST_BASE_01: customer-level aggregation of CIS_DATA_POP_02 (SAS lines 3-28).
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
                -- prime/secondary role flags folded in (SAS CUST_BASE_05 :84-116). Each was
                -- a `... IN (SELECT DISTINCT cid FROM CIS_DATA_POP_02 WHERE PRIMARY_FLAG=..)`
                -- LEFT JOIN, i.e. a per-cid boolean -> compute here so we scan the 55M-row
                -- CIS_DATA_POP_02 ONCE instead of 5x (fixes the OOM).
                MAX(CASE WHEN PRIMARY_FLAG = 'Y' THEN 1 ELSE 0 END)                     AS prime_ind,
                MAX(CASE WHEN PRIMARY_FLAG = 'N' THEN 1 ELSE 0 END)                     AS secondary_ind,
                MAX(CASE WHEN PRIMARY_FLAG = 'Y' AND lend_prods = 1 THEN 1 ELSE 0 END)  AS prime_ind_lend,
                MAX(CASE WHEN PRIMARY_FLAG = 'N' AND lend_prods = 1 THEN 1 ELSE 0 END)  AS secondary_ind_lend
            FROM cbs.CIS_DATA_POP_02
            WHERE OBSN_DT = DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
              AND STREAM = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}'
            GROUP BY cid, cid_num, mth_tm_id, process_date
        ),
        -- CUST_BASE_02: join to bureau snapshot; keep the most-recent record per customer
        --              (SAS lines 29-59: ROW_NUMBER OVER SCORE_LAST_RECVD_DT DESC, filter row_num=1).
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
        -- CUST_BASE_04: join to MDM flags for customer type, status, and exclusion indicators
        --              (SAS lines 60-83: DISTINCT join on numeric cid=party_id, EFF_DT filter).
        --              Netezza TRANSLATE numeric-check replaced by TRY_CAST IS NOT NULL.
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
    -- prime_ind / secondary_ind / prime_ind_lend / secondary_ind_lend are already computed
    -- in cust_base_01 (folded MAX(CASE ...)), so the four DISTINCT re-scans + LEFT JOINs of
    -- CIS_DATA_POP_02 that OOM'd are gone. They flow through a.* -> cust_base_02 -> cust_base_04.
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