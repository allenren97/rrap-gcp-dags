from airflow.sdk import task


@task.parquet(
    task_id="make_tng_acct_mo",
    duckdb_conn_id="duckdb-conn",
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq034/tng_acct_mo.parquet",
    sql="""
        SELECT
            cast(ACCT_ID as varchar) as ACCOUNT_ID,
            cast(MTH_ST_DT as DATE) as MONTH_START_DT,
            cast(MTH_END_DT as DATE) as MONTH_END_DT,
            cast(MAT_DT as DATE) as MATURITY_DT,
            cast(OPN_DT as DATE) as OPEN_DT,
            cast(COMMTTD_AMT as DECIMAL(12,2)) as COMMITTED_AMT,
            cast(INTR_ARRS_AMT as DECIMAL(12,2)) as INTEREST_ARREARS_AMT,
            cast(PRNCPL_ARRS_AMT as DECIMAL(12,2)) as PRINCIPAL_ARREARS_AMT,
            cast(ESCROW_ARRS_AMT as DECIMAL(12,2)) as ESCROW_ARREARS_AMT,
            cast(TOT_ARRS_AMT as DECIMAL(12,2)) as TOT_ARREARS_AMT,
            cast(NOMNL_INTR_RT as DECIMAL(12,2)) as NOMINAL_INTEREST_RATE,
            cast(CRNT_LOAN_TO_VAL_RTO as DECIMAL(12,2)) as LOAN_TO_VALUE_RATIO,
            cast(RMNG_TERM as DECIMAL(12,0)) as REMAINING_TERM,
            cast(NON_PRFRMNG_IND as varchar) as NON_PERFORMING_IND,
            cast(NON_ACCRL_IND as varchar) as NON_ACCRUAL_IND,
            cast(DAYS_ARRS_CNT as DECIMAL(12,0)) as DAYS_ARREARS_CNT,
            cast(PYMTS_ARRS_CNT as DECIMAL(12,0)) as PYMTS_ARREARS_CNT,
            cast(NSF_CNT as DECIMAL(12,0)) as NSF_CNT,
            cast(NSF_YTD_CNT as DECIMAL(12,0)) as NSF_YTD_CNT,
            cast(NSF_LIFE_CNT as DECIMAL(12,0)) as NSF_LIFE_CNT,
            cast(ST_PRNCPL_BAL_AMT as DECIMAL(12,2)) as START_PRINCIPAL_BALANCE,
            cast(END_PRNCPL_BAL_AMT as DECIMAL(12,2)) as END_PRINCIPAL_BALANCE,
            cast(SUNDRY_BAL_AMT as DECIMAL(12,2)) as SUNDRY_BALANCE,
            cast(ESCROW_BAL_AMT as DECIMAL(12,2)) as ESCROW_BALANCE,
            cast(TOT_ADVNC_AMT as DECIMAL(12,2)) as TOT_ADVANCED_AMT,
            cast(RMNG_AMORT as DECIMAL(12,0)) as REMAIN_AMORT,
            cast(ACCT_KEY as DECIMAL(12,0)) as ACCOUNT_KEY,
            cast(MORT_APP_KEY as DECIMAL(12,0)) as MTG_APPLICATION_KEY,
            cast(POOL_KEY as DECIMAL(12,0)) as POOL_KEY,
            cast(CUST_KEY as DECIMAL(12,0)) as CUSTOMER_KEY,
            cast(AMORT_DT as DATE) as AMORT_MATURITY_DT,
            cast(LAST_NSF_PYMT_RTN_DT as DATE) as LST_NSF_PYMT_RTN_DT,
            cast(LAST_PYMT_DT as DATE) as LST_PAYMENT_DT,
            cast(NON_PRFRMNG_DT as DATE) as NON_PERFM_DT,
            cast(LATEST_90_DT_PD_DT as DATE) as LATEST_90_DT,
            cast(TOT_SCHED_PYMT_AMT as DECIMAL(12,2)) as TOT_SCH_PYMT,
            cast(LAST_KNOWN_COVER_PCTG as DECIMAL(12,2)) as LST_KNWN_COVER_PCT,
            cast(EVER_ARRS_CNT as DECIMAL(12,0)) as EVER_ARREARS_CNT,
            cast(EVER_30_CNT_PD_CNT as DECIMAL(12,0)) as EVER_30_CNT,
            cast(EVER_60_CNT_PD_CNT as DECIMAL(12,0)) as EVER_60_CNT,
            cast(EVER_90_CNT_PD_CNT as DECIMAL(12,0)) as EVER_90_CNT,
            cast(DFT_TP_CD as varchar) as DEFAULT_TYPE_CODE,
            cast(MORT_ORGNTN_KEY as DECIMAL(12,0)) as MTG_ORIGINATION_KEY,
            cast(ORIG_TERM as varchar) as TERM_DESC,
            cast(RT_TP_DESC as varchar) as RATE_TYPE_DESC,
            cast(ACCELERATED_PYMT_IND as varchar) as ACCELERATED_PMNT_IND,
            cast(ANUL_FACTR as DECIMAL(12,0)) as ANNUAL_FACTOR,
            cast(MORT_PROVIDER_DESC as varchar) as MTG_PROVIDER_DESC,
            cast(PRPTY_TP_DESC as varchar) as PROPERTY_TYPE_DESC,
            cast(TENURE_DESC as varchar) as TENURE_DESC,
            cast(STAT_DESC as varchar) as STATUS_DESC,
            cast(INSURER_DESC as varchar) as INSURER_DESC,
            cast(POOL_DESC as varchar) as POOL_DESC,
            cast(PRPS_DESC as varchar) as PURPOSE_DESC,
            cast(OCPNY_TP_DESC as varchar) as OCCUPANCY_TYPE_DESC,
            cast(DWELLING_TP_CD as varchar) as DWELLING_TYPE,
            NULL as BIRTH_DT,
            nullif(cast(OCP_INDSTR_CD as varchar), '') as OCCUPATION_INDSTRY_CODE,
            NULL as GENDER,
            cast(EARLY_RNEW_IND as varchar) as EARLY_RNWL_IND,
            cast(ORIG_ADJUSTED_BUREAU_SCORE as DECIMAL(12,0)) as ORG_ADJ_BUREAU_SCR,
            cast(SUBMIT_DT as DATE) as SUBMIT_DT,
            cast(ORIG_GDSR as DECIMAL(12,2)) as ORG_GDSR,
            cast(ORIG_TDSR as DECIMAL(12,2)) as ORG_TDSR,
            cast(FRST_PYMT_RTN_DT as DATE) as FRST_PYMT_RTN_DT,
            cast(FRST_90_DAYS_PD_DT as DATE) as FIRST_90_DT,
            cast(GRNT_IND as varchar) as GURNTR_IND,
            cast(ORIG_LOAN_TO_VAL_RTO as DECIMAL(12,2)) as ORG_LTV_RATIO,
            cast(AMORT_PRD as DECIMAL(12,0)) as AMORT_PERIOD,
            cast(ORIG_ADVNC_AMT as DECIMAL(12,2)) as ORG_ADVANCED_AMT,
            cast(DRC_IND as varchar) as DIRECT_IND,
            cast(FRST_DFT_DT as DATE) as FIRST_DEFAULT_DT,
            cast(FRST_ADVNC_DT as DATE) as FRST_ADVANCE_DT,
            cast(LTV_RTO_AT_FRST_DFT as DECIMAL(12,2)) as LOAN_TO_VALUE_AT_FIRST_DEFAULT,
            cast(LATEST_INTR_ADJ_DT as DATE) as LATEST_INTEREST_ADJUST_DT,
            cast(ADVNC_EFF_DT as DATE) as ADVANCE_EFF_DT,
            cast(PRPTY_CITY as varchar) as PROP_CITY,
            cast(PRPTY_CNTRY_CD as varchar) as PROP_COUNTRY_CODE,
            cast(PRPTY_POST_CD as varchar) as PROP_POSTAL_CODE,
            cast(PRPTY_PROV_CD as varchar) as PROP_PROVINCE_CODE,
            cast(LAST_PRPTY_APPRSL_VAL as DECIMAL(12,2)) as LST_PROP_APPRAISAL_VAL,
            cast(FSA as varchar) as FSA,
            cast(LAST_PRPTY_APPRSL_DT as DATE) as LST_PROP_APPRAISAL_DT,
            cast(LIEN_PRIORITY_NUM as DECIMAL(12,0)) as LIEN_PRIORITY_NUM,
            cast(PRPTY_BUILDING_TP as varchar) as PROP_BUILDING_TYPE,
            cast(ORIG_COVER_EXECUTION_VAL as DECIMAL(12,2)) as ORG_COVER_EXEC_VAL,
            cast(PRPTY_USAGE_TP as varchar) as PROP_USAGE_TYPE,
            cast(SCRTY_OWN_TP as varchar) as SECURITY_OWNER_TYPE,
            cast(SCRTY_PROVIDER as varchar) as SECURITY_PROVIDER,
            cast(SCRTY_RGSTRN_BEGIN_DT as DATE) as SECURITY_REG_BEGIN_DT,
            cast(SCRTY_RGSTRN_END_DT as DATE) as SECURITY_REG_END_DT,
            cast(SCRTY_RGSTRN_NUM as varchar) as SECURITY_REG_NUM,
            cast(ASST_TP_DESC as varchar) as ASSET_TYPE,
            cast(ORIG_PRPTY_APPRSL_VAL as DECIMAL(12,0)) as ORIG_PROP_APPRAISAL_VAL,
            cast(ORIG_PRPTY_APPRSL_DT as DATE) as ORIG_PROP_APPRAISAL_DT,
            cast(PRPTY_PRCH_AMT as DECIMAL(12,0)) as PROP_PURCHASE_AMT,
            cast(PRPTY_PRCH_DT as DATE) as PROP_PURCHASE_DT,
            cast(ORIG_TOT_INCM as DECIMAL(18,2)) as ORG_TOTAL_INCOME,
            cast(PRNCPL_INTR_PYMT_AMT as DECIMAL(12,2)) as PI_PYMT_AMT,
            cast(NEXT_INTR_RESET_DT as DATE) as NEXT_INTEREST_RESET_DT,
            cast(RT_MODFR as DECIMAL(12,2)) as RATE_MODIFIER,
            nullif(cast(DN_PYMT_SRC_DESC as varchar), '') as DOWN_PYMT_SOURCE_DESC,
            cast(CLIENT_DT as DATE) as CLIENT_DT,
            cast(CIF_CREATD_ON_DT as DATE) as CIF_CREATED_ON_DT,
            cast(DFT_IND as varchar) as DEFAULT_IND,
            cast(STATED_INCM_IND as varchar) as STATED_INCOME_INDICATOR,
            cast(ACCR_INTR_AMT as DECIMAL(12,2)) as ACCRUED_INTEREST_AMT,
            cast(CLS_DT as DATE) as CLOSE_DT,
            cast(BULK_INSURER_DESC as varchar) as BULK_NSURER_DESC,
            cast(SECRTZTN_IND as varchar) as SECURITIZATION_INDICATOR,
            cast(CIF_TP_DESC as varchar) as CIF_TYPE_DESC,
            cast(CUST_TP_2 as varchar) as CUSTOMER_TYPE_2,
            cast(INTR_PYMTS_AMT as DECIMAL(12,2)) as INTEREST_PAYMENTS,
            cast(INTR_CMPNDNG_FREQ as DECIMAL(12,0)) as INTEREST_COMPOUNDING_FREQ,
            cast(OPN_CLSD_TERM as varchar) as OPEN_CLOSED_TERM,
            cast(PYT_RSN as varchar) as PAYOUT_REASON,
            cast(LAST_RNEW_DT as DATE) as LAST_RENEWAL_DT,
            cast(ACCT_NUM as varchar) as ACCOUNT_NUM
        from
            '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq034/airb_tangrn_mort_acct.parquet'
        where
            MTH_END_DT = '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='MTH_END_DT') }}'
    """,
    export_params={},
    clear_before_write=True,
)
def make_tng_acct_mo():
    """ Create TNG_ACCT_MO parquet file with proper schema and filters applied. """
    pass


@task.duckdb(
    task_id="delete_if_exists",
    duckdb_conn_id="duckdb-conn",
    sql="""
        DELETE FROM ingestion.TNG_ACCT_MO
        WHERE MONTH_END_DT = '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='MTH_END_DT') }}'
    """,
)
def delete_if_exists():
    """ Delete TNG_ACCT_MO table if it already exists in DuckDB to ensure idempotency. """
    pass


@task.duckdb(
    task_id="load_tng_acct_mo",
    duckdb_conn_id="duckdb-conn",
    sql="""
        INSERT INTO ingestion.TNG_ACCT_MO BY NAME
        SELECT * FROM '{{ task_instance.xcom_pull(task_ids='sq034.sq034_enrichment.make_tng_acct_mo', key='parquet') }}'
    """,
)
def load_tng_acct_mo():
    """ Load TNG_ACCT_MO parquet file into DuckLake. """
    pass


""" TaskFlow function definitions """
make_tng_acct_mo = make_tng_acct_mo()
delete_if_exists = delete_if_exists()
load_tng_acct_mo = load_tng_acct_mo()

""" Dependency chaining """
[
    delete_if_exists,
    make_tng_acct_mo
] >> load_tng_acct_mo
