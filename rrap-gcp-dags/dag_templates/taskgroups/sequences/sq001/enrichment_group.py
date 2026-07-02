from airflow.sdk import task


@task.duckdb(
    task_id="load_airb_mort_mth_snapshot",
    duckdb_conn_id="duckdb-conn",
    sql="""
        INSERT INTO ingestion.AIRB_MORT_MTH_SNAPSHOT BY NAME
        SELECT 
            INSRT_PROCESS_TMSTMP,
            OP_FIELD,
            TM_ID,
            MORT_NUM,
            AMORT,
            LEND_VALUE,
            PROPERTY_ADDR_1,
            PROPERTY_ADDR_2,
            PROPERTY_ADDR_3,
            PROP_PROV,
            PREPAY_YTD,
            AUTH_AMT,
            CAB,
            DLQNT_MTH,
            EFF_TMSTMP,
            FLOAT_IND,
            INEREST_ACCR_AMT,
            INTR_ADJ_DT,
            MADE_DT,
            MAT_DT,
            PD_OFF_F,
            PRIM_CUST_ID,
            STEP_F,
            TOT_ADVNC_AMT,
            TRNST,
            UNPAID_MTH_PAY_DT,
            UNPAID_WKLY_PAY_DT,
            CLASS,
            FUND_CD,
            SECURITY_TYPE,
            CRNT_BAL,
            FRCLSR_F,
            PD_OFF_DT,
            TOT_SUSP_BAL,
            TNIF_RGN_CD,
            COMM_TP,
            INSUR_GRP,
            LRA_STAT,
            DLQNT_DAY,
            GL_ACCT_NUM,
            GL_TRNST_NUM,
            PRD_GRP_CD,
            UNIT_CNT,
            CRNCY_CD,
            MTH_END_DT
        FROM '{{ task_instance.xcom_pull(task_ids='sq001.sq001_source.airb_mort_mth_snapshot', key='parquet') }}'
    """,
)
def load_airb_mort_mth_snapshot():
    """
    Task to load the final AIRB_MORT_MTH_SNAPSHOT parquet file into the duckdb table.
    """
    pass


load_airb_mort_mth_snapshot = load_airb_mort_mth_snapshot()
