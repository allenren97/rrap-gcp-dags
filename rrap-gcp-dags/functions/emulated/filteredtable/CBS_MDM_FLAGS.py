"""
Rewrite of J_CBS_0000_MDMFLAGS_CHECK.sas + ETL/Python/cbs_mdm_flags.py.

SAS job:
  1. Abort if crz.cbs_mdm_flags has no rows for mth_end_dt
  2. Delete + append month partition into nzwrk.cbs_mdm_flags

GCP job builds emulated.CBS_MDM_FLAGS from ingestion OS TOS snapshots:
  duckdb_delete  — clear partition for (EFF_DT, STREAM)
  export_result  — latest person/org MDM snapshot as-of rundate
  duckdb_load    — insert parquet into DuckLake

Upstream: ingestion.OS_TOS_PERSON, ingestion.OS_TOS_ORGANIZATION
Downstream joins (e.g. J_CBS_0020_CUSTUNIV_02): PARTY_ID = CID, EFF_DT = process month
"""

UPSTREAM_ASSET = [
    "ingestion.OS_TOS_PERSON",
    "ingestion.OS_TOS_ORGANIZATION",
]

DOWNSTREAM_ASSET = "emulated.CBS_MDM_FLAGS"

_TASK_GROUP = "filteredtable__CBS_MDM_FLAGS"

DEPENDENCIES = {
    "duckdb_delete": ["export_result"],
    "export_result": ["duckdb_load"],
}


def duckdb_delete(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    DELETE FROM {DOWNSTREAM_ASSET}
    WHERE EFF_DT = DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
      AND STREAM = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}'
    """,
):
    pass


def export_result(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    WITH
        params AS (
            SELECT
                DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS eff_dt,
                {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}} AS mth_tm_id,
                '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}' AS stream
        ),
        person_asof AS (
            SELECT MAX(p.BUSINESS_EFFECTIVE_DATE) AS snap_dt
            FROM ingestion.OS_TOS_PERSON p
            CROSS JOIN params
            WHERE p.BUSINESS_EFFECTIVE_DATE <= params.eff_dt
        ),
        org_asof AS (
            SELECT MAX(o.BUSINESS_EFFECTIVE_DATE) AS snap_dt
            FROM ingestion.OS_TOS_ORGANIZATION o
            CROSS JOIN params
            WHERE o.BUSINESS_EFFECTIVE_DATE <= params.eff_dt
        ),
        person_src AS (
            SELECT
                TRIM(p.PARTY_ID) AS PARTY_ID,
                p.BIRTH_DT,
                NULL AS ORG_TYPE_CD,
                p.OCCUP_TYPE_CD,
                p.OCCUP_STAT_CD,
                p.OCCUP_CAT_CD,
                p.PERSON_ORG_CD,
                CAST(p.SINCE_DT AS DATE) AS SINCE_DT,
                CAST(p.BANKRUPTCY_DT AS DATE) AS BANKRUPTCY_DT,
                p.DOMICILE_TRANSIT_NUM,
                p.GENDER_CD,
                p.EMP_TYPE_CD,
                p.MARITAL_STAT_CD,
                p.PREF_LANG_CD,
                p.OCCUP_CD,
                p.DECEASED_DT,
                CAST(p.LEFT_DT AS DATE) AS LEFT_DT,
                p.END_DT,
                p.SENSITIVITY_CD
            FROM ingestion.OS_TOS_PERSON p
            CROSS JOIN person_asof pa
            WHERE p.BUSINESS_EFFECTIVE_DATE = pa.snap_dt
        ),
        org_src AS (
            SELECT
                TRIM(o.PARTY_ID) AS PARTY_ID,
                NULL AS BIRTH_DT,
                o.ORG_TYPE_CD,
                NULL AS OCCUP_TYPE_CD,
                NULL AS OCCUP_STAT_CD,
                NULL AS OCCUP_CAT_CD,
                o.PERSON_ORG_CD,
                CAST(o.SINCE_DT AS DATE) AS SINCE_DT,
                CAST(o.BANKRUPTCY_DT AS DATE) AS BANKRUPTCY_DT,
                o.DOMICILE_TRANSIT_NUM,
                NULL AS GENDER_CD,
                NULL AS EMP_TYPE_CD,
                NULL AS MARITAL_STAT_CD,
                o.PREF_LANG_CD,
                NULL AS OCCUP_CD,
                NULL AS DECEASED_DT,
                CAST(o.LEFT_DT AS DATE) AS LEFT_DT,
                NULL AS END_DT,
                o.SENSITIVITY_CD
            FROM ingestion.OS_TOS_ORGANIZATION o
            CROSS JOIN org_asof oa
            WHERE o.BUSINESS_EFFECTIVE_DATE = oa.snap_dt
        ),
        combined AS (
            SELECT * FROM person_src
            UNION ALL
            SELECT * FROM org_src
        )
    SELECT
        params.eff_dt AS EFF_DT,
        params.stream AS STREAM,
        params.mth_tm_id AS MTH_TM_ID,
        'Monthly' AS DATE_TYPE,
        c.PARTY_ID,
        c.PREF_LANG_CD AS PREF_LANG,
        c.GENDER_CD,
        c.MARITAL_STAT_CD AS MARITAL_STATUS,
        c.EMP_TYPE_CD,
        c.OCCUP_CD,
        c.OCCUP_TYPE_CD,
        c.OCCUP_STAT_CD,
        c.OCCUP_CAT_CD,
        c.DOMICILE_TRANSIT_NUM AS TRANSIT_NUM,
        c.SENSITIVITY_CD,
        CASE
            WHEN c.DECEASED_DT IS NOT NULL
                 OR c.DECEASED_DT <= params.eff_dt
            THEN 'Y'
            ELSE 'N'
        END AS DECEASED_IND,
        CASE
            WHEN c.LEFT_DT IS NOT NULL
                 OR c.LEFT_DT <= params.eff_dt
                 OR c.END_DT IS NOT NULL
                 OR c.END_DT <= params.eff_dt
            THEN 'Closed'
            ELSE 'Open'
        END AS CUST_STATUS,
        CASE
            WHEN c.BANKRUPTCY_DT IS NOT NULL
                 AND c.BANKRUPTCY_DT <= params.eff_dt
            THEN 'Y'
            ELSE 'N'
        END AS BNKRPTCY_FLAG,
        CASE
            WHEN TRIM(COALESCE(c.PERSON_ORG_CD, '')) = 'P'
                 AND c.BIRTH_DT IS NOT NULL
                 AND DATE_DIFF('month', c.BIRTH_DT, params.eff_dt) / 12.0 < 18
            THEN 'Y'
            ELSE 'N'
        END AS UNDER_18_FLAG,
        CASE
            WHEN TRIM(COALESCE(c.PERSON_ORG_CD, '')) = 'P' THEN 'Retail'
            WHEN SUBSTR(COALESCE(c.ORG_TYPE_CD, ''), 1, 3) = 'SMB' THEN 'Small Business'
            WHEN SUBSTR(COALESCE(c.ORG_TYPE_CD, ''), 1, 3) = 'COM' THEN 'Commercial'
            WHEN SUBSTR(COALESCE(c.ORG_TYPE_CD, ''), 1, 3) = 'COR' THEN 'Corporate'
            ELSE NULL
        END AS CUST_TYPE,
        CASE
            WHEN c.SINCE_DT IS NOT NULL
            THEN DATE_DIFF('month', c.SINCE_DT, params.eff_dt)
            ELSE NULL
        END AS TIME_ON_BOOKS,
        CASE
            WHEN c.BIRTH_DT IS NOT NULL
            THEN FLOOR(DATE_DIFF('month', c.BIRTH_DT, params.eff_dt) / 12.0)
            ELSE NULL
        END AS CUST_AGE,
        CURRENT_TIMESTAMP AS INSRT_PROCESS_TMSTMP,
        CURRENT_TIMESTAMP AS UPDT_PROCESS_TMSTMP
    FROM combined c
    CROSS JOIN params
    WHERE TRIM(COALESCE(c.PARTY_ID, '')) <> ''
    """,
):
    pass


def duckdb_load(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} BY NAME
    SELECT *
    FROM read_parquet(
        '{{{{ task_instance.xcom_pull(task_ids="{_TASK_GROUP}.export_result", key="parquet") }}}}'
    )
    """,
):
    pass
