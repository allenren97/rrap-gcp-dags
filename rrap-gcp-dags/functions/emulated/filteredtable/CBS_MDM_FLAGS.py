"""
Direct load of emulated.CBS_MDM_FLAGS from EDL-R Hive.

Legacy:
  - cbs_mdm_flags.py builds crz_cust_scorecard.cbs_mdm_flags in Hive
  - J_CBS_0000_MDMFLAGS_CHECK.sas validates + copies Hive -> Netezza

GCP:
  beeline_extract — SELECT from crz_cust_scorecard.cbs_mdm_flags -> parquet
  duckdb_delete   — clear partition for (EFF_DT, STREAM)
  duckdb_load     — map Hive columns into DuckLake

Downstream joins (e.g. J_CBS_0020_CUSTUNIV_02): PARTY_ID = CID, EFF_DT = process month
"""

DOWNSTREAM_ASSET = "emulated.CBS_MDM_FLAGS"

_TASK_GROUP = "filteredtable__CBS_MDM_FLAGS"

DEPENDENCIES = {
    "duckdb_delete": ["beeline_extract"],
    "beeline_extract": ["duckdb_load"],
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


def beeline_extract(
    beeline_conn_id="edlr-conn",
    sql="""
    USE crz_cust_scorecard;
    SELECT *
    FROM cbs_mdm_flags
    WHERE eff_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
    """,
    target="cbs_mdm_flags.parquet",
    rundir="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}",
    to_parquet=True,
    strings_can_be_null=True,
    tmpfileloc="/bns/rrap/data/tmp",
):
    pass


def duckdb_load(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} BY NAME
    SELECT
        CAST(eff_dt AS DATE) AS EFF_DT,
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}' AS STREAM,
        {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}} AS MTH_TM_ID,
        date_type AS DATE_TYPE,
        TRIM(CAST(party_id AS VARCHAR)) AS PARTY_ID,
        pref_lang AS PREF_LANG,
        gender_cd AS GENDER_CD,
        marital_status AS MARITAL_STATUS,
        emp_type_cd AS EMP_TYPE_CD,
        occup_cd AS OCCUP_CD,
        occup_type_cd AS OCCUP_TYPE_CD,
        occup_stat_cd AS OCCUP_STAT_CD,
        occup_cat_cd AS OCCUP_CAT_CD,
        transit_num AS TRANSIT_NUM,
        sensitivity_cd AS SENSITIVITY_CD,
        deceased_ind AS DECEASED_IND,
        cust_status AS CUST_STATUS,
        bnkrptcy_flag AS BNKRPTCY_FLAG,
        under_18_flag AS UNDER_18_FLAG,
        cust_type AS CUST_TYPE,
        time_on_books AS TIME_ON_BOOKS,
        CAST(cust_age AS INTEGER) AS CUST_AGE,
        COALESCE(
            TRY_CAST(insrt_process_tmstmp AS TIMESTAMP),
            CURRENT_TIMESTAMP
        ) AS INSRT_PROCESS_TMSTMP,
        CURRENT_TIMESTAMP AS UPDT_PROCESS_TMSTMP
    FROM read_parquet(
        '{{{{ task_instance.xcom_pull(task_ids="{_TASK_GROUP}.beeline_extract", key="parquet") }}}}'
    )
    """,
):
    pass
