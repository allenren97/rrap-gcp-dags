from airflow.sdk import task


@task.duckdb(
    task_id="delete_existing_records",
    duckdb_conn_id="duckdb-conn",
    sql="""
        DELETE FROM ingestion.BASELAYER_MOR
        WHERE MTH_END_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="MTH_END_DT") }}'
    """,
)
def delete_existing_records():
    """Delete existing records for the month being processed from the target table."""
    pass


@task.parquet(
    task_id="make_baselayer_mor",
    duckdb_conn_id="duckdb-conn",
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq023/baselayer_mor.parquet",
    sql="""
        SELECT
            MTH_END_DT,
            CASE 
                WHEN MORT_NUM IS NULL THEN 0
                ELSE MORT_NUM 
            END AS MORT_NUM,
            CASE 
                WHEN unq_acct_id IS NULL THEN '0'
                ELSE unq_acct_id 
            END AS UNIQUEACCOUNTIDENTIFIER,
            NCR_EXPSR_SIZE_KEY_VAL AS NCR_EXPOSURE_SIZE,
            NCR_EXPSR_GEO_KEY_VAL AS NCR_GEOGRAPHY,
            NCR_RT_KEY_VAL AS NCR_RATE,
            NCR_DLQNT_BCKT_KEY_VAL AS NCR_DELINQUENCY_BUCKET,
            CAST(acct_opn_dt AS DATE) AS ACCOUNTOPENDATE,
            TRNST_NUM AS TRANSIT,
            ADVNC_AMT AS ADVANCEDAMOUNT,
            AUTH_AMT AS AUTHORIZEDAMOUNT,
            GL_BAL_ADJUSTING_AMT AS GL_BALANCING_ADJUSTMENT,
            DLQNT_DAYS_CNT AS DAYSDELINQUENT,
            BEFR_ZERO_NET_DRAWN_AMT AS BF_ZERO_NET_DRAWN_AMT,
            BEFR_ZERO_NET_UNDRAWN_AMT AS BF_ZERO_NET_UNDRAWN_AMT,
            ORIG_PRPTY_VAL_AMT AS ORIGINALPROPERTYVALUE,
            CCAR_EXPSR_CLSS_NM AS CCAR_EXPOSURE_CLASS,
            CONSM_PRD_TREATMNT_CD AS TREATMENT_FLAG,
            CASE
                WHEN PRIM_CUST_CID IS NULL THEN NULL
                ELSE LEFT(PRIM_CUST_CID, LENGTH(PRIM_CUST_CID) - 5)
            END AS PRIMARY_CUSTOMER_CID,
            CASE 
                WHEN RGNL_OFFC_CD IS NULL THEN '-1'
                ELSE RGNL_OFFC_CD 
            END AS TNIF_REGION_CODE,
            DLQNT_STG AS DELINQ_STAGE,
            BASEL_CIF_KEY AS BASEL_CIF_KEY,
            CAB_TRNST_NUM AS CAB,
            LEGAL_ENTITY,
            CAST(mat_dt AS DATE) AS MATURITY_DATE,
            SCOTIA_TOT_EQTY_PLN_F AS STEP_FLAG,
            RESIDUAL_MAT AS RESIDUAL_MATURITY,
            TRNST_EXCLSN_F AS TRANSIT_EXCLUSION_FLAG,
            AF_ZERO_NET_DRAWN_AMT,
            AF_ZERO_NET_UNDRAWN_AMT
        FROM '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="RUNDIR") }}/sq023/make_airb_baselayer_mort.parquet'
    """,
    export_params={},
    clear_before_write=True,
)
def make_baselayer_mor():
    """Generate the BASELAYER_MOR parquet file for the month being processed."""
    pass


@task.duckdb(
    task_id="insert_new_records",
    duckdb_conn_id="duckdb-conn",
    sql="""
        INSERT INTO ingestion.BASELAYER_MOR BY NAME
        SELECT * FROM read_parquet('{{ task_instance.xcom_pull(task_ids="handle_month_context", key="RUNDIR") }}/sq023/baselayer_mor.parquet')
    """,
)
def insert_new_records():
    """Insert new records into the target table from the generated parquet file."""
    pass


""" TaskFlow function definitons """
delete_existing_records = delete_existing_records()
make_baselayer_mor = make_baselayer_mor()
insert_new_records = insert_new_records()

""" Dependency chaining """
[ 
    delete_existing_records,
    make_baselayer_mor
] >> insert_new_records
