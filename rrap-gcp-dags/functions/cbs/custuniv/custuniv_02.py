"""
Rewrite of J_CBS_0020_CUSTUNIV_02.sas — CBS Customer Universe, step 02.

Four SAS work tables collapsed into one CTE chain (CUST_BASE_01 -> 02 -> 04 -> 05):
  cust_base_01  -- customer-level aggregation of CIS_DATA_POP_02 (lend-product counts)
  cust_base_02  -- + credit-bureau snapshot (latest score row per cid)
  cust_base_04  -- + CBS_MDM_FLAGS (cust type/status/deceased/bankruptcy/under-18)
  cust_base_05  -- + primary/secondary indicators from CIS_DATA_POP_02
Output -> cbs.CUST_BASE_05 (read by steps 03 and 04).

Reads:
  cbs.CIS_DATA_POP_02                      (step 01 output; grouped + primary flags)
  ingestion.CR_BUREAU_DELI_MTH_SNAPSHOT    (bureau; keyed by MTH_TM_ID, CUST_CID)
  emulated.CBS_MDM_FLAGS                    (MDM; keyed by EFF_DT, STREAM, PARTY_ID)

NOTE (flagged): CBS_MDM_FLAGS has DATE_TYPE in its grain; the SAS join does not pin
it and relies on SELECT DISTINCT. If a party has >1 DATE_TYPE row per (EFF_DT, STREAM)
with differing CUST_TYPE/STATUS, that fans out CUST_BASE_04 — confirm whether
DATE_TYPE should be pinned. Untested against on-prem CUST_BASE_05.
"""

UPSTREAM_ASSET = [
    "cbs.CIS_DATA_POP_02",
    "ingestion.CR_BUREAU_DELI_MTH_SNAPSHOT",
    "emulated.CBS_MDM_FLAGS",
]

DOWNSTREAM_ASSET = "cbs.CUST_BASE_05"

_TASK_GROUP = "custuniv__custuniv_02"

DEPENDENCIES = {
    "duckdb_delete": ["duckdb_load"],
    "export_result": ["duckdb_load"],
}

_RUNDATE = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
_STREAM = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
_MTH_TM_ID = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}'


def duckdb_delete(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    DELETE FROM {DOWNSTREAM_ASSET}
    WHERE OBSN_DT = '{_RUNDATE}'
      AND STREAM = '{_STREAM}'
    """,
):
    pass


def export_result(
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
                COUNT(1) AS num_prods,
                SUM(lend_prods_CUR) AS num_lend_prods_CUR,
                SUM(lend_prods_CLO) AS num_lend_prods_CLO,
                SUM(lend_prods_BNK) AS num_lend_prods_bnk,
                SUM(lend_prods_DEF) AS num_lend_prods_DEF,
                SUM(lend_prods_CHG) AS num_lend_prods_CHG,
                SUM(lend_prods_WO) AS num_lend_prods_WO,
                SUM(lend_prods_COMM_CUR) AS num_lend_prods_COMM_CUR,
                SUM(lend_prods_COMM_CLO) AS num_lend_prods_COMM_CLO,
                SUM(lend_prods_COMM_CHG) AS num_lend_prods_COMM_CHG,
                SUM(lend_prods_COMM_DEF) AS num_lend_prods_COMM_DEF,
                SUM(lend_prods_COMM_WO) AS num_lend_prods_COMM_WO,
                SUM(PRIVATE_BANK_IND) AS PRIVATE_BANK_IND,
                SUM(mor_ind) AS num_mor,
                SUM(spl_ind) AS num_spl,
                SUM(rev_ind) AS num_rev,
                SUM(ssl_ind) AS num_ssl,
                SUM(lend_prods_CUR) + SUM(lend_prods_DEF)
                    + SUM(lend_prods_COMM_CUR) + SUM(lend_prods_COMM_DEF) AS num_lend_prods,
                MAX(days_dlq) AS worst_dlq_days
            FROM cbs.CIS_DATA_POP_02
            WHERE OBSN_DT = '{_RUNDATE}'
              AND STREAM = '{_STREAM}'
            GROUP BY cid, cid_num, mth_tm_id, process_date
        ),
        cust_base_02 AS (
            SELECT
                a.*,
                c.BASEL_CUST_ID AS basel_cust_id,
                c.CUST_CID AS cust_cid,
                c.HIT_NOHIT_EDIT_REJCT_CD,
                c.FICO_08_SCORE,
                c.FICO_08_EXCLSN_CD,
                CASE WHEN c.CUST_CID IS NOT NULL THEN 1 ELSE 0 END AS bureau_exist
            FROM cust_base_01 a
            LEFT JOIN ingestion.CR_BUREAU_DELI_MTH_SNAPSHOT c
                ON a.cid = c.CUST_CID
               AND a.mth_tm_id = c.MTH_TM_ID
               AND c.MTH_TM_ID = {_MTH_TM_ID}
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY a.cid ORDER BY c.SCORE_LAST_RECVD_DT DESC
            ) = 1
        ),
        cust_base_04 AS (
            SELECT DISTINCT
                a.*,
                b.CUST_TYPE AS cust_type,
                b.CUST_STATUS AS cust_status,
                b.DECEASED_IND AS deceased_ind,
                b.BNKRPTCY_FLAG,
                b.UNDER_18_FLAG,
                CASE WHEN b.CUST_TYPE = 'Retail' THEN 1 ELSE NULL END AS retail_ind
            FROM cust_base_02 a
            LEFT JOIN emulated.CBS_MDM_FLAGS b
                ON TRY_CAST(TRIM(b.PARTY_ID) AS BIGINT) IS NOT NULL   -- party_id numeric
               AND TRY_CAST(a.cid AS BIGINT) = TRY_CAST(b.PARTY_ID AS BIGINT)
               AND b.EFF_DT = DATE '{_RUNDATE}'
               AND b.STREAM = '{_STREAM}'
        ),
        prim_y AS (
            SELECT DISTINCT cid, PRIMARY_FLAG FROM cbs.CIS_DATA_POP_02
            WHERE OBSN_DT = '{_RUNDATE}' AND STREAM = '{_STREAM}' AND PRIMARY_FLAG = 'Y'
        ),
        prim_n AS (
            SELECT DISTINCT cid, PRIMARY_FLAG FROM cbs.CIS_DATA_POP_02
            WHERE OBSN_DT = '{_RUNDATE}' AND STREAM = '{_STREAM}' AND PRIMARY_FLAG = 'N'
        ),
        prim_y_lend AS (
            SELECT DISTINCT cid, PRIMARY_FLAG FROM cbs.CIS_DATA_POP_02
            WHERE OBSN_DT = '{_RUNDATE}' AND STREAM = '{_STREAM}' AND PRIMARY_FLAG = 'Y' AND LEND_PRODS = 1
        ),
        prim_n_lend AS (
            SELECT DISTINCT cid, PRIMARY_FLAG FROM cbs.CIS_DATA_POP_02
            WHERE OBSN_DT = '{_RUNDATE}' AND STREAM = '{_STREAM}' AND PRIMARY_FLAG = 'N' AND LEND_PRODS = 1
        ),
        cust_base_05 AS (
            SELECT
                a.*,
                CASE WHEN b.PRIMARY_FLAG = 'Y' THEN 1 ELSE 0 END AS prime_ind,
                CASE WHEN c.PRIMARY_FLAG = 'N' THEN 1 ELSE 0 END AS secondary_ind,
                CASE WHEN d.PRIMARY_FLAG = 'Y' THEN 1 ELSE 0 END AS prime_ind_lend,
                CASE WHEN e.PRIMARY_FLAG = 'N' THEN 1 ELSE 0 END AS secondary_ind_lend
            FROM cust_base_04 a
            LEFT JOIN prim_y b ON a.cid = b.cid
            LEFT JOIN prim_n c ON a.cid = c.cid
            LEFT JOIN prim_y_lend d ON a.cid = d.cid
            LEFT JOIN prim_n_lend e ON a.cid = e.cid
        )
    SELECT
        DATE '{_RUNDATE}' AS OBSN_DT,
        '{_STREAM}' AS STREAM,
        *
    FROM cust_base_05
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
