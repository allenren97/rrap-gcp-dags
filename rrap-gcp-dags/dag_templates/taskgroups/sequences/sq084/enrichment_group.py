from airflow.sdk import task


@task.duckdb(
    task_id="delete_cbs_mdm_flags",
    duckdb_conn_id="duckdb-conn",
    sql="""
        DELETE FROM emulated.CBS_MDM_FLAGS
        WHERE EFF_DT = DATE '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="RUNDATE") }}'
          AND STREAM = 'CBS'
    """,
)
def delete_cbs_mdm_flags():
    """ Clear the (EFF_DT, STREAM) partition before reload (idempotent re-runs). """
    pass


@task.duckdb(
    task_id="load_cbs_mdm_flags",
    duckdb_conn_id="duckdb-conn",
    sql="""
        INSERT INTO emulated.CBS_MDM_FLAGS BY NAME
        SELECT
            CAST(eff_dt AS DATE) AS EFF_DT,
            'CBS' AS STREAM,
            {{ task_instance.xcom_pull(task_ids="handle_month_context", key="MTH_TM_ID") }} AS MTH_TM_ID,
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
            COALESCE(TRY_CAST(insrt_process_tmstmp AS TIMESTAMP), CURRENT_TIMESTAMP) AS INSRT_PROCESS_TMSTMP,
            CURRENT_TIMESTAMP AS UPDT_PROCESS_TMSTMP
        FROM '{{ task_instance.xcom_pull(task_ids="sq084.sq084_source.extract_cbs_mdm_flags", key="parquet") }}'
    """,
)
def load_cbs_mdm_flags():
    """ Load extracted MDM flags into emulated.CBS_MDM_FLAGS (drops op_field). """
    pass


""" TaskFlow function definitions """
delete_cbs_mdm_flags = delete_cbs_mdm_flags()
load_cbs_mdm_flags = load_cbs_mdm_flags()

""" Dependency chaining """
delete_cbs_mdm_flags >> load_cbs_mdm_flags
