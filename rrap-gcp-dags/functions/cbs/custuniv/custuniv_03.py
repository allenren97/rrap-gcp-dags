"""
Rewrite of J_CBS_0030_CUSTUNIV_03.sas -- CBS account status, step 03.

Two SAS stages, collapsed into one query (the Z_CBS_ACCNTS staging table is just the
distinct retail account list, so it becomes a CTE):
  1. acct_list (:5-11): DISTINCT (lpad(cast(account as bigint),18,'0'), account, product)
     from CIS_DATA_POP_02 joined to CUST_BASE_05 where cust_type='Retail'.
  2. STATUS (:29-37): enrich each account with ACCT_TYP / ACCT_BASE_KEY (acct_xref) and
     ACCT_LCST (IWF_CUST_ACCT at time_key=&tm_id, PRIM_CUST_F='P'), one row per account.

  export_status -> parquet -> duckdb_load -> cbs.CBS_ACCT_STATUS.

NOTES / assumptions (flagged -- confirm against prod):
  * ACCT_XREF is NOT filtered by POPN_DT here; the one-row-per-account dedup keeps the
    latest xref (ORDER BY POPN_DT DESC). The SAS uses owtact.acct_xref (live, 1 row/acct)
    and rownumber() with no ORDER BY (arbitrary) -- adjust if a specific snapshot is meant.
  * cust_type filter compares b.CUST_TYPE = 'Retail' (from CUST_BASE_05).
  * Emulated tables are partitioned by OBSN_DT/STREAM; the two cbs.* joins are aligned on
    them. IWF_CUST_ACCT.TIME_KEY = &tm_id (the run month).
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
    "export_status": ["duckdb_load"],
    "duckdb_delete": ["duckdb_load"],
}

_RUNDATE = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
_STREAM = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
_TM = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}'


def duckdb_delete(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    DELETE FROM {DOWNSTREAM_ASSET}
    WHERE OBSN_DT = '{_RUNDATE}'
      AND STREAM = '{_STREAM}'
    """,
):
    pass


def export_status(
    duckdb_conn_id="duckdb-conn",
    resource_tier="HIGH",
    pool_slots=96,
    sql=f"""
    WITH acct_list AS (
        -- SAS :5-11: distinct retail accounts. LEFT JOIN + WHERE b.cust_type -> inner.
        SELECT DISTINCT
            LPAD(CAST(TRY_CAST(a.ACCOUNT AS BIGINT) AS VARCHAR), 18, '0') AS ACCT_ID,
            a.ACCOUNT,
            a.PRODUCT
        FROM cbs.CIS_DATA_POP_02 a
        JOIN cbs.CUST_BASE_05 b
            ON a.CID = b.CID
           AND b.OBSN_DT = DATE '{_RUNDATE}'
           AND b.STREAM = '{_STREAM}'
        WHERE a.OBSN_DT = DATE '{_RUNDATE}'
          AND a.STREAM = '{_STREAM}'
          AND b.CUST_TYPE = 'Retail'
    )
    SELECT
        DATE '{_RUNDATE}' AS OBSN_DT,
        '{_STREAM}' AS STREAM,
        al.ACCT_ID,
        al.ACCOUNT,
        al.PRODUCT,
        x.ACCT_TYP,
        x.ACCT_BASE_KEY,
        i.ACCT_LCST
    FROM acct_list al
    LEFT JOIN ingestion.ACCT_XREF x
        ON x.ACCT_ID = al.ACCT_ID
    LEFT JOIN ingestion.IWF_CUST_ACCT i
        ON i.ACCT_BASE_KEY = x.ACCT_BASE_KEY
       AND i.TIME_KEY = {_TM}
       AND i.PRIM_CUST_F = 'P'
    -- SAS rownumber() over (partition by account) where rn=1 (one row per account)
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY al.ACCOUNT
        ORDER BY x.POPN_DT DESC NULLS LAST, x.ACCT_BASE_KEY NULLS LAST
    ) = 1
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
        '{{{{ task_instance.xcom_pull(task_ids="{_TASK_GROUP}.export_status", key="parquet") }}}}'
    )
    """,
):
    pass
