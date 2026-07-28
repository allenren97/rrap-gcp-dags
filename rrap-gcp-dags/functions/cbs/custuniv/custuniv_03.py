"""
Rewrite of J_CBS_0030_CUSTUNIV_03.sas â€” CBS Customer Universe, step 03.

Two stages, mirroring the SAS:
  export_gather   -- Retail account list: distinct accounts from CIS_DATA_POP_02 for
                     customers identified as 'Retail' in CUST_BASE_05, LEFT JOINed to
                     the account cross-reference and CBS customer-account tables, with a
                     ROW_NUMBER() deduplication key per account.
                     NOTE: the SAS routes data through a CBS DB2 staging table
                     (sas.Z_CBS_ACCNTS) before querying back via a pass-through
                     connection (cbsdb2). That cross-system round-trip is eliminated
                     here â€” the joins to ingestion.ACCT_XREF and ingestion.IWF_CUST_ACCT
                     are resolved directly in DuckDB.
  export_result   -- Filters to row_num = 1 per account (mirroring the SAS
                     rownumber() PARTITION BY account), prepends OBSN_DT and STREAM,
                     and selects the final column set.
  duckdb_delete / duckdb_load -> cbs.CBS_ACCT_STATUS (this dag's output).

Source schema mapping (verified against the ducklake catalog):
  cbs.*         CIS_DATA_POP_02, CUST_BASE_05 (outputs of custuniv_01 / custuniv_02)
  ingestion.*   ACCT_XREF, IWF_CUST_ACCT

OPEN ITEMS (flagged, need confirmation):
  * ingestion.ACCT_XREF     -- the SAS uses cbsdb2.owtact.acct_xref; DuckDB schema/table
    name and ingestion status need to be confirmed.
  * ingestion.IWF_CUST_ACCT -- the SAS uses cbsdb2.OWSTAR.IWF_CUST_ACCT; DuckDB schema/
    table name and ingestion status need to be confirmed.
  * cbs.CBS_ACCT_STATUS -- the SAS target is NZWRK.STATUS; the DuckDB name is provisional.
    Confirm final table name and DDL before deployment.
  * Untested against on-prem output -- the account-level derivations are a faithful
    translation but should be reconciled against NZWRK.STATUS on a sample month.
"""

UPSTREAM_ASSET = [
    "cbs.CIS_DATA_POP_02",
    "cbs.CUST_BASE_05",
    "ingestion.ACCT_XREF",
    "ingestion.IWF_CUST_ACCT",
]

DOWNSTREAM_ASSET = "cbs.CBS_ACCT_STATUS"

_TASK_GROUP = "custuniv__custuniv_03"

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
        -- Retail account list: distinct account-product pairs for retail customers
        -- (SAS lines 2-11: acct_list query joining CIS_DATA_POP_02 to CUST_BASE_05
        --  filtered to cust_type='Retail', LPAD account to 18 chars after numeric cast).
        acct_list AS (
            SELECT DISTINCT
                LPAD(TRY_CAST(a.account AS BIGINT)::VARCHAR, 18, '0') AS acct_id,
                a.account,
                a.product
            FROM cbs.CIS_DATA_POP_02 a
            LEFT JOIN cbs.CUST_BASE_05 b
                ON a.cid = b.cid
               AND b.OBSN_DT = DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
               AND b.STREAM = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}'
            WHERE a.OBSN_DT = DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
              AND a.STREAM = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}'
              AND b.cust_type = 'Retail'
        )
    -- Account cross-reference and CBS customer-account enrichment with deduplication key
    -- (SAS lines 24-36: Z_CBS_ACCNTS LEFT JOIN owtact.acct_xref LEFT JOIN OWSTAR.IWF_CUST_ACCT,
    --  rownumber() PARTITION BY account to resolve duplicate acct_xref matches).
    SELECT
        a.acct_id,
        a.account,
        a.product,
        b.ACCT_TYP,
        b.ACCT_BASE_KEY,
        c.acct_lcst,
        ROW_NUMBER() OVER (PARTITION BY a.account) AS row_num
    FROM acct_list a
    LEFT JOIN ingestion.ACCT_XREF b
        ON a.acct_id = b.acct_id
    LEFT JOIN ingestion.IWF_CUST_ACCT c
        ON b.ACCT_BASE_KEY = c.acct_base_key
       AND c.time_key = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
       AND c.PRIM_CUST_F = 'P'
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
        acct_id,
        account,
        product,
        ACCT_TYP,
        ACCT_BASE_KEY,
        acct_lcst
    FROM g
    WHERE row_num = 1
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