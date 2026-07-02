import pyarrow as pa
import os
from airflow.sdk import task, get_current_context


"""kq_acct SQL"""
kq_acct_sql = """
USE {{ var.value.TSZ_SCHEMA }};
select 
    kq_tkq_account1.src_mnemonic_cd,
    kq_tkq_account1.businesseffectivedate,
    lpad(trim(kq_tkq_account1.bcm_account_num),23,'0') AS bcm_account_num,
    kq_tkq_account1.bcm_segment_id,
    kq_tkq_account1.bcm_curr_proc_dt,
    kq_tkq_account1.bcm_corporate_retail_ind,
    kq_tkq_account1.bcm_tot_unpaid_finance_chg,
    kq_tkq_account1.bcm_tot_recovery_int,
    kq_tkq_account1.bcm_mdl_compaction,
    kq_tkq_account1.bcm_mdl_citifone,
    kq_tkq_account1.bcm_mdl_code_3,
    kq_tkq_account1.bcm_mdl_code_4,
    kq_tkq_account1.bcm_prev_sub_product,
    kq_tkq_account2.bcm_prev_credit_score,
    kq_tkq_account2.bcm_campaign_status,
    kq_tkq_account2.bcm_campaign_notified,
    kq_tkq_account2.bcm_pad_other_fi_ind,
    kq_tkq_account2.bcm_ann_fee_ind,
    kq_tkq_account2.bcm_solicitation_code,
    kq_tkq_bill_statements.bcm_current_bill_code ,
    kq_tkq_bill_statements.bcm_rtrn_envlp,
    kq_tkq_bill_statements.bcm_oper_last_online_updt,
    kq_tkq_bill_statements.bcm_full_payment_ind ,
    kq_tkq_delinquency.bcm_dlq_history_13_24,
    kq_tkq_delinquency.bcm_dlq_history_01_12,
    kq_tkq_financial_history.bcm_last_year_credit_int_paid,
    kq_tkq_financial_history.bcm_tot_ytd_credit_int_paid ,
    kq_tkq_financial_history.bcm_bal_hist_purchases_1,
    kq_tkq_financial_history.bcm_bal_hist_new_balance_1,
    COALESCE(kq_tkq_financial1.bcm_purchase_curr_cyc_bal, 0),
    kq_tkq_financial1.bcm_purchase_1cyc_ago_bal ,
    kq_tkq_financial1.bcm_purchase_2cyc_ago_bal ,
    kq_tkq_financial1.bcm_ytd_purchases_num ,
    kq_tkq_financial1.bcm_ytd_purchase_int_chged ,
    kq_tkq_financial1.bcm_ytd_purchase_int_paid,
    kq_tkq_financial1.bcm_ytd_cash_adv_int_chged,
    kq_tkq_financial1.bcm_ytd_cash_adv_int_paid ,
    kq_tkq_financial1.bcm_purchase_recovery_int ,
    kq_tkq_financial1.bcm_amt_last_payment,
    COALESCE(kq_tkq_financial2.bcm_cash_adv_curr_cyc_bal, 0),
    COALESCE(kq_tkq_financial2.bcm_cash_adv_1cyc_ago_bal, 0),
    kq_tkq_financial2.bcm_cash_adv_2cyc_ago_bal,
    kq_tkq_financial2.bcm_cash_adv_recovery_int ,
    kq_tkq_financial2.bcm_fytd_purchases,
    kq_tkq_financial2.bcm_fytd_returns,
    kq_tkq_financial2.bcm_dividend_rebate_last_yr,
    kq_tkq_plastics.bcm_visa_plas_out,
    kq_tkq_plastics.bcm_chip_flag,
    cast('{{ task_instance.xcom_pull(task_ids="handle_month_context", key="RUNDATE") }}' as date) cal_end_date
    from  v_kq_tkq_account1_tsys as kq_tkq_account1
    left outer join v_kq_tkq_account2_tsys as kq_tkq_account2
        on kq_tkq_account1.bcm_curr_proc_dt=kq_tkq_account2.bcm_curr_proc_dt and kq_tkq_account1.bcm_account_num=kq_tkq_account2.bcm_account_num and kq_tkq_account1.businesseffectivedate=kq_tkq_account2.businesseffectivedate
    left outer join v_kq_tkq_bill_statements_tsys as kq_tkq_bill_statements
        on kq_tkq_account1.bcm_account_num=kq_tkq_bill_statements.bcm_account_num 
        and kq_tkq_account1.businesseffectivedate=kq_tkq_bill_statements.businesseffectivedate
    left outer join v_kq_tkq_delinquency_tsys as kq_tkq_delinquency
        on kq_tkq_account1.bcm_curr_proc_dt=kq_tkq_delinquency.bcm_curr_proc_dt 
        and kq_tkq_account1.bcm_account_num=kq_tkq_delinquency.bcm_account_num 
        and kq_tkq_account1.businesseffectivedate=kq_tkq_delinquency.businesseffectivedate
    left outer join v_kq_tkq_financial_history_tsys as kq_tkq_financial_history
        on kq_tkq_account1.bcm_curr_proc_dt=kq_tkq_financial_history.bcm_curr_proc_dt 
        and kq_tkq_account1.bcm_account_num=kq_tkq_financial_history.bcm_account_num 
        and kq_tkq_account1.businesseffectivedate=kq_tkq_financial_history.businesseffectivedate
    left outer join v_kq_tkq_financial1_tsys as kq_tkq_financial1
        on kq_tkq_account1.bcm_curr_proc_dt=kq_tkq_financial1.bcm_curr_proc_dt 
        and kq_tkq_account1.bcm_account_num=kq_tkq_financial1.bcm_account_num 
        and kq_tkq_account1.businesseffectivedate=kq_tkq_financial1.businesseffectivedate
    left outer join v_kq_tkq_financial2_tsys as kq_tkq_financial2
        on kq_tkq_account1.bcm_curr_proc_dt=kq_tkq_financial2.bcm_curr_proc_dt 
        and kq_tkq_account1.bcm_account_num=kq_tkq_financial2.bcm_account_num 
        and kq_tkq_account1.businesseffectivedate=kq_tkq_financial2.businesseffectivedate 
    left outer join v_kq_tkq_plastics_tsys as kq_tkq_plastics 
        on kq_tkq_account1.bcm_curr_proc_dt=kq_tkq_plastics.bcm_curr_proc_dt 
        and kq_tkq_account1.bcm_account_num=kq_tkq_plastics.bpi_account_num 
        and kq_tkq_account1.businesseffectivedate=kq_tkq_plastics.businesseffectivedate
    WHERE
        kq_tkq_account1.src_mnemonic_cd = 'TSYS' AND
        kq_tkq_account1.businesseffectivedate = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="RUNDATE") }}'
"""

SCHEMA_kq_acct = pa.schema([
    ('src_mnemonic_cd', pa.string()),
    ('businesseffectivedate', pa.date64()),
    ('bcm_account_num', pa.string()),
    ('bcm_segment_id', pa.int64()),
    ('bcm_curr_proc_dt', pa.date64()),
    ('bcm_corporate_retail_ind', pa.string()),
    ('bcm_tot_unpaid_finance_chg', pa.float64()),
    ('bcm_tot_recovery_int', pa.float64()),
    ('bcm_mdl_compaction', pa.string()),
    ('bcm_mdl_citifone', pa.string()),
    ('bcm_mdl_code_3', pa.string()),
    ('bcm_mdl_code_4', pa.string()),
    ('bcm_prev_sub_product', pa.string()),
    ('bcm_prev_credit_score', pa.string()),
    ('bcm_campaign_status', pa.string()),
    ('bcm_campaign_notified', pa.string()),
    ('bcm_pad_other_fi_ind', pa.string()),
    ('bcm_ann_fee_ind', pa.float64()),
    ('bcm_solicitation_code', pa.string()),
    ('bcm_current_bill_code', pa.string()),
    ('bcm_rtrn_envlp', pa.string()),
    ('bcm_oper_last_online_updt', pa.string()),
    ('bcm_full_payment_ind', pa.string()),
    ('bcm_dlq_history_13_24', pa.float64()),
    ('bcm_dlq_history_01_12', pa.float64()),
    ('bcm_last_year_credit_int_paid', pa.float64()),
    ('bcm_tot_ytd_credit_int_paid', pa.float64()),
    ('bcm_bal_hist_purchases_1', pa.float64()),
    ('bcm_bal_hist_new_balance_1', pa.float64()),
    ('bcm_purchase_curr_cyc_bal', pa.float64()),
    ('bcm_purchase_1cyc_ago_bal', pa.float64()),
    ('bcm_purchase_2cyc_ago_bal', pa.float64()),
    ('bcm_ytd_purchases_num', pa.float64()),
    ('bcm_ytd_purchase_int_chged', pa.float64()),
    ('bcm_ytd_purchase_int_paid', pa.float64()),
    ('bcm_ytd_cash_adv_int_chged', pa.float64()),
    ('bcm_ytd_cash_adv_int_paid', pa.float64()),
    ('bcm_purchase_recovery_int', pa.float64()),
    ('bcm_amt_last_payment', pa.float64()),
    ('bcm_cash_adv_curr_cyc_bal', pa.float64()),
    ('bcm_cash_adv_1cyc_ago_bal', pa.float64()),
    ('bcm_cash_adv_2cyc_ago_bal', pa.float64()),
    ('bcm_cash_adv_recovery_int', pa.float64()),
    ('bcm_fytd_purchases', pa.float64()),
    ('bcm_fytd_returns', pa.float64()),
    ('bcm_dividend_rebate_last_yr', pa.float64()),
    ('bcm_visa_plas_out', pa.float64()),
    ('bcm_chip_flag', pa.string()),
    ('cal_end_date', pa.date64()),
])


@task
def create_sq007_rundir():
    """
    Task to create RUNDIR for sequence sq007.
    RUNDIR is the directory where the extracted data for the sequence will be stored.
    """
    context = get_current_context()
    RUNDIR = context['ti'].xcom_pull(task_ids='handle_month_context', key='RUNDIR')
    sq007_rundir = f"{RUNDIR}/sq007"
    os.makedirs(sq007_rundir, exist_ok=True)


@task.beeline(
    task_id="odbc_tsz_kq_acct",
    beeline_conn_id="edlr-conn",
    sql=kq_acct_sql,
    schema=SCHEMA_kq_acct,
    target="kq_acct.parquet",
    rundir="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq007",
    to_parquet=True,
    tmpfileloc="/bns/rrap/data/tmp"
)
def odbc_tsz_kq_acct():
    """
    Task to extract kq_acct data from EDL using the defined SQL and schema.
    """
    pass


@task.beeline(
    task_id="odbc_tsz_bcname",
    beeline_conn_id="edlr-conn",
    sql="""
    SELECT 
        tsys.businesseffectivedate,
        tsys.bcm_account_num,
        tsys.bcm_segment_id,
        tsys.bcm_curr_proc_dt,
        tsys.bcm_last_addr_chge_dt_mdy,
        cast('{{ task_instance.xcom_pull(task_ids="handle_month_context", key="MTH_END_DT") }}' as date) cal_end_date
    from  {{ var.value.TSZ_SCHEMA }}.v_kq_tkq_bcname_tsys as tsys
    WHERE tsys.src_mnemonic_cd == 'TSYS' AND
        tsys.businesseffectivedate = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="MTH_END_DT") }}'
    """,
    schema=pa.schema([
        ('businesseffectivedate', pa.date64()),
        ('bcm_account_num', pa.string()),
        ('bcm_segment_id', pa.int64()),
        ('bcm_curr_proc_dt', pa.date64()),
        ('bcm_last_addr_chge_dt_mdy', pa.date64()),
        ('cal_end_date', pa.date64())
    ]),
    target="ODBC_tsz_bcname.parquet",
    rundir="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq007",
    to_parquet=True,
    tmpfileloc="/bns/rrap/data/tmp"
)
def odbc_tsz_bcname():
    """
    Task to extract TSYS account numbers/various attributes from TSZ's `v_kq_tkq_bcname_tsys` using `src_mnemonic_cd`
    as the delimiting filter column. Export file is 'ODBC_tsz_bcname.parquet'.
    """
    pass


@task.parquet(
    task_id="join_bcname",
    duckdb_conn_id="duckdb-conn",
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq007/join_bcname.parquet",
    sql="""
    SELECT 
        bc.*, kq.*
    FROM
        '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="RUNDIR") }}/sq007/odbc_tsz_bcname.parquet' bc 
    JOIN
        '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="RUNDIR") }}/sq007/kq_acct.parquet' kq 
    ON
        lpad(trim(bc.bcm_account_num), 23, '0') = lpad(trim(kq.bcm_account_num), 23, '0') 
        AND
        bc.cal_end_date = kq.cal_end_date
    """,
    export_params={},
    clear_before_write=True,
)
def join_bcname():
    """
    Task to join contents of 'odbc_tsz_bcname.parquet' and 'kq_acct.parquet' on trimmed and left-padded 
    BCM Account Numbers and Calendar End Date.
    This data is then written to 'join_bcname.parquet'.
    """
    pass


@task.parquet(
    task_id="join_bcname_nodup",
    duckdb_conn_id="duckdb-conn",
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq007/jb0071_join_bcname_nodup.parquet",
    sql="""
        SELECT
        -- MAX here acts to pick the non null value for a numeric column (ANY_VALUE is for strings)
        MAX(bc.bcm_last_addr_chge_dt_mdy )                                                                      AS bcm_last_addr_chge_dt_mdy,
        MAX(bc.businesseffectivedate )                                                                          AS businesseffectivedate,
        bc.bcm_account_num,
        MAX(bc.bcm_segment_id)                                                                                  AS bcm_segment_id,
        MAX(bc.bcm_curr_proc_dt)                                                                                AS bcm_curr_proc_dt,
        ANY_VALUE(CASE WHEN bc.bcm_corporate_retail_ind='' THEN NULL ELSE bc.bcm_corporate_retail_ind END )     AS bcm_corporate_retail_ind,
        MAX(bc.bcm_tot_unpaid_finance_chg)                                                                      AS bcm_tot_unpaid_finance_chg,
        MAX(bc.bcm_tot_recovery_int)                                                                            AS bcm_tot_recovery_int,
        ANY_VALUE(CASE WHEN bc.bcm_mdl_compaction='' THEN NULL ELSE bc.bcm_mdl_compaction END )                 AS bcm_mdl_compaction,
        ANY_VALUE(CASE WHEN bc.bcm_mdl_citifone='' THEN NULL ELSE bc.bcm_mdl_citifone END )                     AS bcm_mdl_citifone,
        ANY_VALUE(CASE WHEN bc.bcm_mdl_code_3='' THEN NULL ELSE bc.bcm_mdl_code_3 END )                         AS bcm_mdl_code_3,
        ANY_VALUE(CASE WHEN bc.bcm_mdl_code_4='' THEN NULL ELSE bc.bcm_mdl_code_4 END )                         AS bcm_mdl_code_4,
        ANY_VALUE(CASE WHEN bc.bcm_prev_sub_product='' THEN NULL ELSE bc.bcm_prev_sub_product END )             AS bcm_prev_sub_product,
        ANY_VALUE(CASE WHEN bc.bcm_prev_credit_score='' THEN NULL ELSE bc.bcm_prev_credit_score END )           AS bcm_prev_credit_score,
        ANY_VALUE(CASE WHEN bc.bcm_campaign_status='' THEN NULL ELSE bc.bcm_campaign_status END )               AS bcm_campaign_status,
        ANY_VALUE(CASE WHEN bc.bcm_campaign_notified='' THEN NULL ELSE bc.bcm_campaign_notified END )           AS bcm_campaign_notified,
        ANY_VALUE(CASE WHEN bc.bcm_pad_other_fi_ind='' THEN NULL ELSE bc.bcm_pad_other_fi_ind END )             AS bcm_pad_other_fi_ind,
        MAX(bc.bcm_ann_fee_ind )                                                                                AS bcm_ann_fee_ind,
        ANY_VALUE(CASE WHEN bc.bcm_solicitation_code='' THEN NULL ELSE bc.bcm_solicitation_code END )           AS bcm_solicitation_code,
        ANY_VALUE(CASE WHEN bc.bcm_current_bill_code='' THEN NULL ELSE bc.bcm_current_bill_code END )           AS bcm_current_bill_code,
        ANY_VALUE(CASE WHEN bc.bcm_rtrn_envlp='' THEN NULL ELSE bc.bcm_rtrn_envlp END )                         AS bcm_rtrn_envlp,
        ANY_VALUE(CASE WHEN bc.bcm_oper_last_online_updt='' THEN NULL ELSE bc.bcm_oper_last_online_updt END )   AS bcm_oper_last_online_updt,
        ANY_VALUE(CASE WHEN bc.bcm_full_payment_ind='' THEN NULL ELSE bc.bcm_full_payment_ind END )             AS bcm_full_payment_ind,

        MAX(bc.bcm_dlq_history_13_24 )                                                                          AS bcm_dlq_history_13_24,
        MAX(bc.bcm_dlq_history_01_12 )                                                                          AS bcm_dlq_history_01_12,
        MAX(bc.bcm_last_year_credit_int_paid )                                                                  AS bcm_last_year_credit_int_paid,
        MAX(bc.bcm_tot_ytd_credit_int_paid )                                                                    AS bcm_tot_ytd_credit_int_paid,
        MAX(bc.bcm_bal_hist_purchases_1 )                                                                       AS bcm_bal_hist_purchases_1,
        MAX(bc.bcm_bal_hist_new_balance_1 )                                                                     AS bcm_bal_hist_new_balance_1,
        MAX(bc.bcm_purchase_curr_cyc_bal )                                                                      AS bcm_purchase_curr_cyc_bal,
        MAX(bc.bcm_purchase_1cyc_ago_bal )                                                                      AS bcm_purchase_1cyc_ago_bal,
        MAX(bc.bcm_purchase_2cyc_ago_bal )                                                                      AS bcm_purchase_2cyc_ago_bal,
        MAX(bc.bcm_ytd_purchases_num )                                                                          AS bcm_ytd_purchases_num,
        MAX(bc.bcm_ytd_purchase_int_chged )                                                                     AS bcm_ytd_purchase_int_chged,
        MAX(bc.bcm_ytd_purchase_int_paid )                                                                      AS bcm_ytd_purchase_int_paid,
        MAX(bc.bcm_ytd_cash_adv_int_chged )                                                                     AS bcm_ytd_cash_adv_int_chged,
        MAX(bc.bcm_ytd_cash_adv_int_paid )                                                                      AS bcm_ytd_cash_adv_int_paid,
        MAX(bc.bcm_purchase_recovery_int )                                                                      AS bcm_purchase_recovery_int,
        MAX(bc.bcm_amt_last_payment )                                                                           AS bcm_amt_last_payment,
        MAX(bc.bcm_cash_adv_curr_cyc_bal )                                                                      AS bcm_cash_adv_curr_cyc_bal,
        MAX(bc.bcm_cash_adv_1cyc_ago_bal )                                                                      AS bcm_cash_adv_1cyc_ago_bal,
        MAX(bc.bcm_cash_adv_2cyc_ago_bal )                                                                      AS bcm_cash_adv_2cyc_ago_bal,
        MAX(bc.bcm_cash_adv_recovery_int )                                                                      AS bcm_cash_adv_recovery_int,
        MAX(bc.bcm_fytd_purchases )                                                                             AS bcm_fytd_purchases,
        MAX(bc.bcm_fytd_returns )                                                                               AS bcm_fytd_returns,
        MAX(bc.bcm_dividend_rebate_last_yr )                                                                    AS bcm_dividend_rebate_last_yr,
        MAX(bc.bcm_visa_plas_out )                                                                              AS bcm_visa_plas_out,

        ANY_VALUE(CASE WHEN bc.bcm_chip_flag = '' THEN NULL ELSE bc.bcm_chip_flag END )                         AS bcm_chip_flag,
        MAX(bc.cal_end_date )                                                                                   AS cal_end_date
    FROM 
        '{{ task_instance.xcom_pull(task_ids="sq007.sq007_source.join_bcname", key="parquet") }}' bc
    GROUP BY bc.bcm_account_num
    """,
    export_params={},
    clear_before_write=True,
)
def join_bcname_nodup():
    """
    This task groups contents of 'jb0071_join_bcname.parquet' by BCM Account Number to drop any duplicate accounts (seen in prior batches). 
    Non-null value, either via MAX() - for numeric cols - or ANY_VALUE() - for strings - is selected; 
    contents written to 'jb0071_join_bcname_nodup.parquet'.
    """
    pass


@task.parquet(
    task_id="xft_src_to_tgt",
    duckdb_conn_id="duckdb-conn",
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq007/xft_src_to_tgt.parquet",
    sql="""
    SELECT 
        businesseffectivedate,
        bcm_account_num                 AS acct_num,
        bcm_segment_id,
        bcm_curr_proc_dt,
        bcm_last_addr_chge_dt_mdy       AS last_addr_chng_dt,
        cal_end_date                    AS mth_end_dt,
        -- businesseffectivedate_1,     -- these are duplicate cols due to Join_bcname task
        -- bcm_account_num_1,           -- these are duplicate cols due to Join_bcname task
        -- bcm_segment_id_1,            -- these are duplicate cols due to Join_bcname task
        -- bcm_curr_proc_dt_1,          -- these are duplicate cols due to Join_bcname task
        bcm_corporate_retail_ind        AS corp_rtl_ind,
        bcm_tot_unpaid_finance_chg      AS tot_unpaid_fncl_chrg_amt,
        bcm_tot_recovery_int            AS tot_rcvry_intr_amt,
        bcm_mdl_compaction              AS mdl_compaction_cd,
        bcm_mdl_citifone                AS mdl_citifone_cd,
        bcm_mdl_code_3                  AS mdl_cd_3_cd,
        bcm_mdl_code_4                  AS mdl_cd_4_cd,
        bcm_prev_sub_product            AS prev_sub_prd_cd,
        CASE 
            WHEN trim(bcm_prev_credit_score) = '' THEN NULL 
            ELSE bcm_prev_credit_score 
        END                             AS prev_credit_score,
        bcm_campaign_status             AS cmpgn_stat_cd,
        bcm_campaign_notified           AS cmpgn_notified_cd,
        bcm_pad_other_fi_ind            AS pad_oth_fncl_instn_ind,
        bcm_ann_fee_ind                 AS annual_fee_ind,
        bcm_solicitation_code           AS solicitation_cd,
        bcm_current_bill_code           AS crnt_bill_cd,
        bcm_rtrn_envlp                  AS return_envolope_cd,
        bcm_oper_last_online_updt       AS oprtr_last_online_updt,
        bcm_full_payment_ind            AS full_pymt_ind,
        bcm_dlq_history_13_24           AS dlqnt_hist_13_to_24_day,
        bcm_dlq_history_01_12           AS dlqnt_hist_01_to_12_day,
        bcm_last_year_credit_int_paid   AS last_yr_credit_intr_paid_amt,
        bcm_tot_ytd_credit_int_paid     AS tot_ytd_credit_intr_paid_amt,
        bcm_bal_hist_purchases_1        AS prch_bal_hist_amt,
        bcm_bal_hist_new_balance_1      AS pymt_hist_amt,
        bcm_purchase_curr_cyc_bal       AS crnt_cycl_prch_bal_amt,
        bcm_purchase_1cyc_ago_bal       AS prev_1_cycl_prch_bal_amt,
        bcm_purchase_2cyc_ago_bal       AS prev_2_cycl_prch_bal_amt,
        bcm_ytd_purchases_num           AS ytd_prch_cnt,
        bcm_ytd_purchase_int_chged      AS ytd_prch_intr_chrgd_amt,
        bcm_ytd_purchase_int_paid       AS ytd_prch_intr_paid_amt,
        bcm_ytd_cash_adv_int_chged      AS ytd_csh_advnc_intr_chrgd_amt,
        bcm_ytd_cash_adv_int_paid       AS ytd_csh_advnc_intr_paid_amt,
        bcm_purchase_recovery_int       AS prch_rcvry_intr_amt,
        bcm_amt_last_payment            AS last_pymt_amt,
        bcm_cash_adv_curr_cyc_bal       AS crnt_cycl_csh_advnc_bal_amt,
        bcm_cash_adv_1cyc_ago_bal       AS prev_1_cycl_csh_advnc_bal_amt,
        bcm_cash_adv_2cyc_ago_bal       AS prev_2_cycl_csh_advnc_bal_amt,
        bcm_cash_adv_recovery_int       AS csh_advnc_rcvry_intr_amt,
        bcm_fytd_purchases              AS fncl_ytd_prch_amt,
        bcm_fytd_returns                AS fncl_ytd_return_amt,
        bcm_dividend_rebate_last_yr     AS last_yr_divdnd_rebate_amt,
        bcm_visa_plas_out               AS visa_plastic_out, -- already type double no need to transform
        CASE 
            WHEN trim(bcm_chip_flag) = '' THEN NULL 
            ELSE bcm_chip_flag 
        END                             AS chip_f,
        -- cal_end_date_1,    -- another duplicate due to join_bcname_nodup
        -- bcm_account_num_2, -- another duplicate due to join_bcname_nodup
        'KQ' AS src_sys_cd
    FROM
        '{{ task_instance.xcom_pull(task_ids="sq007.sq007_source.join_bcname_nodup", key="parquet") }}'
    """,
    export_params={},
    clear_before_write=True,
)
def xft_src_to_tgt():
    """
    This task performs various transformations and renaming of columns from 'jb0071_join_bcname_nodup.parquet' 
    and writes them to 'jb0071_xft_src_to_tgt.parquet'.
    """
    pass


@task.beeline(
    task_id="odbc_rcrr_revlvng_credit_mth_snapshot",
    beeline_conn_id="edlr-conn",
    sql="""
    use {{ var.value.RCRR_SCHEMA }};
    SELECT mth_end_dt,
       lpad(trim(acct_num),23,'0') AS acct_num,
       cast(case when src_sys_cd = 'KQ_TSYS' THEN 'KQ' ELSE src_sys_cd end as varchar(20)) src_sys_cd,
	   cast(acct1_sub_prd_cd as varchar(3)) acct1_sub_prd_cd,
	   cast(acct2_transit_number as varchar(5)) acct2_transit_number,
	   cast(acct1_prd_cd as varchar(3)) acct1_prd_cd,
	   cast(acct1_block_cd as varchar(1)) acct1_block_cd, 
	   cast(acct1_recl_cd as varchar(1)) acct1_recl_cd, 
       cast(acct1_acct_stat_cd as varchar(1)) acct1_acct_stat_cd,
	   acct1_open_dt,
	   acct1_src,
	   acct1_final_score,
	   acct1_last_prch_dt,
	   acct1_last_active_dt,
	   acct1_credit_lmt,
	   delq_mth_dlqnt,
	   delq_orig_chng_off_amt,
	   delq_chrg_off_dt,
	   cast(delq_chrg_off_cd as varchar(1)) delq_chrg_off_cd,
	   fin1_last_pymt_dt,
	   os_bal_coa_amt,
	   stud_risk_bal_rto,
	   acct2_first_use_dt,
	   fin2_nal_dt,
	   fin2_wor_dt,
	   cast(acct2_cls_reason_cd as varchar(2)) acct2_cls_reason_cd,
	   delq_bns_dlqnt_day,
	   acct1_last_blocked_dt,
	   cast(acct1_proc_type_cd as varchar(2)) acct1_proc_type_cd,
	   cast(acct1_scrd_ind as varchar(1)) acct1_scrd_ind,
	   cast(acct1_switch_ind as varchar(1)) acct1_switch_ind,
	   acct1_next_renew_fee_dt_mth_yr,
	   acct1_switch_xref,
	   acct1_switch_dt,
	   cast(acct2_cacs_crc as varchar(2)) acct2_cacs_crc,
	   cast(acct2_setup_transit as varchar(5)) acct2_setup_transit, 
	   acct1_sub_prd_tfr_dt,
	   cast(acct2_inact_ind as varchar(1)) acct2_inact_ind,
	   cast(acct1_scrty_type_cd as varchar(2)) acct1_scrty_type_cd,
	   acct1_last_online_updt_dt,
	   acct1_scrty_val,
	   cast(acct1_annual_fee_lnk_ind as varchar(1)) acct1_annual_fee_lnk_ind,
	   acct1_rqstd_credit_lmt,
	   cast(stud_spsp_ccli as varchar(1)) stud_spsp_ccli,
	   cast(gl_acct_num as varchar(7)) gl_acct_num,
	   cast(gl_acctng_transit as varchar(5)) gl_acctng_transit,
       cast(currency_cd as varchar(3)) currency_cd,
       COALESCE(tot_ctd_fncl_chrg, 0),
       COALESCE(bal_hist_csh_adv_amt, 0)
    FROM 
        tsys_revlvng_credit_mth_snapshot
    WHERE
        mth_end_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="MTH_END_DT") }}';
    """,
    schema=pa.schema([
        ('mth_end_dt', pa.date64()),
        ('acct_num', pa.string()),
        ('src_sys_cd', pa.string()), 
        ('acct1_sub_prd_cd', pa.string()), 
        ('acct2_transit_number', pa.int64()),  
        ('acct1_prd_cd', pa.string()), 
        ('acct1_block_cd', pa.string()), 
        ('acct1_recl_cd', pa.string()), 
        ('acct1_acct_stat_cd', pa.string()), 
        ('acct1_open_dt', pa.date64()), 
        ('acct1_src', pa.int64()),  
        ('acct1_final_score', pa.int64()),  
        ('acct1_last_prch_dt', pa.date64()), 
        ('acct1_last_active_dt', pa.date64()), 
        ('acct1_credit_lmt', pa.float64()),
        ('delq_mth_dlqnt', pa.int64()),  
        ('delq_orig_chng_off_amt', pa.float64()),
        ('delq_chrg_off_dt', pa.date64()), 
        ('delq_chrg_off_cd', pa.string()), 
        ('fin1_last_pymt_dt', pa.date64()), 
        ('os_bal_coa_amt', pa.float64()),
        ('stud_risk_bal_rto', pa.float64()),
        ('acct2_first_use_dt', pa.date64()), 
        ('fin2_nal_dt', pa.date64()), 
        ('fin2_wor_dt', pa.date64()), 
        ('acct2_cls_reason_cd', pa.string()), 
        ('delq_bns_dlqnt_day', pa.int64()),  
        ('acct1_last_blocked_dt', pa.date64()), 
        ('acct1_proc_type_cd', pa.int64()),  
        ('acct1_scrd_ind', pa.string()), 
        ('acct1_switch_ind', pa.string()), 
        ('acct1_next_renew_fee_dt_mth_yr', pa.int64()),  
        ('acct1_switch_xref', pa.int64()),  
        ('acct1_switch_dt', pa.date64()), 
        ('acct2_cacs_crc', pa.int64()),  
        ('acct2_setup_transit', pa.int64()),  
        ('acct1_sub_prd_tfr_dt', pa.date64()), 
        ('acct2_inact_ind', pa.string()), 
        ('acct1_scrty_type_cd', pa.int64()),  
        ('acct1_last_online_updt_dt', pa.date64()), 
        ('acct1_scrty_val', pa.int64()),  
        ('acct1_annual_fee_lnk_ind', pa.string()), 
        ('acct1_rqstd_credit_lmt', pa.int64()),  
        ('stud_spsp_ccli', pa.string()), 
        ('gl_acct_num', pa.int64()),  
        ('gl_acctng_transit', pa.int64()),
        ('currency_cd', pa.string()),
        ('tot_ctd_fncl_chrg', pa.float64()),
        ('bal_hist_csh_adv_amt',pa.float64())
    ]),
    target="odbc_rcrr_revlvng_credit_mth_snapshot.parquet",
    rundir="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq007",
    to_parquet=True,
    tmpfileloc="/bns/rrap/data/tmp"
)
def odbc_rcrr_revlvng_credit_mth_snapshot():
    """
    Task to extract data from `tsys_revlvng_credit_mth_snapshot` table in EDL, perform necessary transformations and type casting, 
    and write the output to 'odbc_rcrr_revlvng_credit_mth_snapshot.parquet'.
    """
    pass


@task.beeline(
    task_id="odbc_ez_step_acct_rltnp_snapshot",
    beeline_conn_id="edlr-conn",
    sql="""
    use {{ var.value.EZ1_SCHEMA }};
    select 
        acct_grp_id, lpad(trim(acct_num),23,'0') as acct_num, 
        cast(src_sys_cd as varchar(20))  src_sys_cd, cast(grp_stat_cd as varchar(1)) grp_stat_cd, mth_end_dt 
    from 
        step_acct_rltnp_snapshot where  (grp_stat_cd IS NULL or grp_stat_cd = 'F')  
        and  mth_end_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="MTH_END_DT") }}';
    """,
    schema=pa.schema([
        ('acct_grp_id', pa.string()),
        ('acct_num', pa.string()),
        ('src_sys_cd', pa.string()),
        ('grp_stat_cd', pa.string()),
        ('mth_end_dt', pa.date64())
    ]),
    target="odbc_ez_step_acct_rltnp_snapshot.parquet",
    rundir="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq007",
    to_parquet=True,
    tmpfileloc="/bns/rrap/data/tmp"
)
def odbc_ez_step_acct_rltnp_snapshot():
    """
    Task to extract Scotia Total Equity Plan (STEP) account attributes from 'ez1.step_acct_rltnp_snapshot' in EDL, perform necessary transformations and type casting, 
    and write the output to 'odbc_ez_step_acct_rltnp_snapshot.parquet'.
    Note: This task is currently commented out as EDL-R returns masked results which break the downstream join. 
    Once EDL-R supports unmasked results for this table, the SQL, schema, and other parameters can be defined and this task can be enabled.
    """
    pass


@task.parquet(
    task_id="odbc_airb_cust_acct_rltnp",
    duckdb_conn_id="duckdb-conn",
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq007/odbc_airb_cust_acct_rltnp.parquet",
    sql="""
    select
        mth_end_dt,
        cast(cust_id as varchar(40)) as cust_id,
        lpad(trim(acct_num),23,'0') as acct_num,
        cast(src_sys_cd as varchar(20)) as src_sys_cd,
        cast(cust_acct_rltnp_type_cd as varchar(10)) as rel_cd,
        cast(primary_acct_holder_f as varchar(1)) as prim_cust_f
    from
        '{{ task_instance.xcom_pull(task_ids="sq004.sq004_source.airb_cust_acct_rltnp", key="parquet") }}' --TODO: 'jb0042_AIRB_CUST_ACCT_RLTNP.parquet'
    where
        prim_cust_f = 'Y'
        and mth_end_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="MTH_END_DT") }}'
    """,
    export_params={},
    clear_before_write=True,
)
def odbc_airb_cust_acct_rltnp():
    """
    This task pulls customer and account relationship data generated during sequence 004 in 'airb_cust_acct_rltnp.parquet',
    where primary customer flag = 'Y'.
    """
    pass


@task.parquet(
    task_id="join_15",
    duckdb_conn_id="duckdb-conn",
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq007/join_15.parquet",
    sql="""
    select 
        -- TODO Make all columns explicit , since Joins in datastage can also do on the fly column renaming
        rcrr.*, airb.*, ez.*,
        rcrr.acct1_sub_prd_cd as sub_prd_cd,
        airb.cust_id as rltnp_prim_cust_id 
    FROM '{{ task_instance.xcom_pull(task_ids="sq004.sq004_source.rcrr_revlvng_credit_mth_snapshot", key="parquet") }}' rcrr 
    LEFT JOIN '{{ task_instance.xcom_pull(task_ids="sq007.sq007_source.odbc_ez_step_acct_rltnp_snapshot", key="parquet") }}' ez
        ON rcrr.mth_end_dt = ez.mth_end_dt AND
            rcrr.src_sys_cd = ez.src_sys_cd AND
            lpad(trim(rcrr.acct_num), 23, '0') = lpad(trim(ez.acct_num), 23, '0')
    LEFT JOIN '{{ task_instance.xcom_pull(task_ids="sq007.sq007_source.odbc_airb_cust_acct_rltnp", key="parquet") }}' airb  
        ON rcrr.mth_end_dt = airb.mth_end_dt AND 
            rcrr.src_sys_cd = airb.src_sys_cd AND 
            lpad(trim(rcrr.acct_num), 23, '0') = lpad(trim(airb.acct_num), 23, '0')
    """,
    export_params={},
    clear_before_write=True,
)
def join_15():
    """ 
    This task joins 'odbc_rcrr_revlvng_credit_mth_snapshot.parquet' and 'odbc_ez_step_acct_rltnp_snapshot.parquet' 
    on source system code, as well as 'odbc_airb_cust_acct_rltnp.parquet' on source system code and 
    trimmed + left-padded accounts numbers into 'join_15.parquet'.
    """
    pass


@task.parquet(
    task_id="join_25",
    duckdb_conn_id="duckdb-conn",
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq007/join_25.parquet",
    sql="""
    select 
        rev.*, j15.*
    FROM '{{ task_instance.xcom_pull(task_ids="sq007.sq007_source.xft_src_to_tgt", key="parquet") }}' rev
    INNER JOIN '{{ task_instance.xcom_pull(task_ids="sq007.sq007_source.join_15", key="parquet") }}' j15 
    ON
        rev.mth_end_dt = j15.mth_end_dt AND
        rev.src_sys_cd = j15.src_sys_cd AND
        lpad(trim(rev.acct_num), 23, '0') = lpad(trim(j15.acct_num), 23, '0')
    """,
    export_params={},
    clear_before_write=True,
)
def join_25():
    """
    This task inner joins 'jb0071_Xft_Src_To_Tgt.parquet' (TSZ-table's account attributes) with 'jb0071_Join_15.parquet' 
    (monthly snapshot + STEP account + customer attributes) on source system code and trimmed + left padded account numbers.
    This data is then written to 'jb0071_Join_25.parquet'.
    """
    pass


@task.beeline(
    task_id="odbc_v_ux_ux300u2",
    beeline_conn_id="edlr-conn",
    sql="""
    use {{ var.value.RCRR_SCHEMA }};
    with max_bus_eff_dt as
    (
    select max(businesseffectivedate) as max_bed 
    from {{ var.value.CRZ_AIRB_SCHEMA }}.v_ux_ux300u2 
        where businesseffectivedate<='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="MTH_END_DT") }}' 
    ) 
    select 
        a.agreement_no agreement_no , lpad(trim(a.account_no), 23, '0') acct_num,
        cast(b.bor_prd_src_sys_cd as varchar(20)) src_sys_cd, cast(last_day(a.businesseffectivedate) as date)  mth_end_dt 
    from
        {{ var.value.CRZ_AIRB_SCHEMA }}.v_ux_ux300u2 a,   
        v_NON_BOR_PRD_MAPPNG b,
        max_bus_eff_dt c
    where 
        a.product_cde=b.non_bor_prd_cd 
        and b.src_sys_cd='UX'
        and a.businesseffectivedate between b.eff_from_dt and b.eff_to_dt 
        and a.businesseffectivedate = c.max_bed; 
    """,
    schema=pa.schema([
        ('agreement_no', pa.int64()),
        ('acct_num', pa.string()),
        ('src_sys_cd', pa.string()),
        ('mth_end_dt', pa.date64())
    ]),
    target="odbc_v_ux_ux300u2.parquet",
    rundir="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq007",
    to_parquet=True,
    tmpfileloc="/bns/rrap/data/tmp"
)
def odbc_v_ux_ux300u2():
    """
    This task is currently commented out as the source table `v_ux_ux300u2` in EDL-R returns zero records for the
     relevant month-end date, which breaks the downstream join. Once EDL-R supports returning the expected records for this 
     table, the SQL, schema, and other parameters can be defined and this task can be enabled.
    """
    pass


@task.parquet(
    task_id="convert_xref_odbc_v_ux_ux300u2",
    duckdb_conn_id="duckdb-conn",
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq007/convert_xref_odbc_v_ux_ux300u2.parquet",
    sql="""
    SELECT 
        COALESCE(xref.bcm_acct_num, a.acct_num) as acct_num, 
        a.agreement_no,
        a.src_sys_cd,
        a.mth_end_dt
    FROM '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq007/odbc_v_ux_ux300u2.parquet' as a
    LEFT JOIN (
            SELECT bcm_acct_num, tsys_acct_id  
            FROM '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq004/kq_tkq_ks_tsys_xref.parquet' -- can use either the jb0043 or jb0031 version; both are identical
            WHERE end_of_chain_indicator='Y'
            GROUP BY bcm_acct_num, tsys_acct_id 
        ) as xref
    ON 
        lpad(trim(a.acct_num), 23, '0') = lpad(trim(xref.tsys_acct_id), 23, '0')
    """,
    export_params={},
    clear_before_write=True,
)
def convert_xref_odbc_v_ux_ux300u2():
    """
    This task is currently commented out as the source table `v_ux_ux300u2` in EDL-R returns zero records for the
     relevant month-end date, which breaks the downstream join. Once EDL-R supports returning the expected records for this 
     table, the SQL, schema, and other parameters can be defined and this task can be enabled.
    """
    pass


@task.parquet(
    task_id="join_57",
    duckdb_conn_id="duckdb-conn",
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq007/join_57.parquet",
    sql="""
    select
        j25.*, ux.*
    FROM '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq007/join_25.parquet' j25
    LEFT JOIN '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq007/odbc_v_ux_ux300u2.parquet' ux 
    ON
        j25.mth_end_dt = ux.mth_end_dt AND
        j25.src_sys_cd = ux.src_sys_cd AND
        lpad(trim(j25.acct_num), 23, '0') = lpad(trim(ux.acct_num), 23, '0')
    """,
    export_params={},
    clear_before_write=True,
)
def join_57():
    """
    This task is currently commented out as the source table `v_ux_ux300u2` in EDL-R returns zero records for the
     relevant month-end date, which breaks this join. Once EDL-R supports returning the expected records for this 
     table, the SQL, schema, and other parameters can be defined and this task can be enabled.
    """
    pass


@task.parquet(
    task_id="transformer_30",
    duckdb_conn_id="duckdb-conn",
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq007/transformer_30.parquet",
    sql="""
    SELECT
        current_timestamp as INSRT_PROCESS_TMSTMP,
        acct1_acct_stat_cd as acct_stat_cd,
        acct1_annual_fee_lnk_ind as anul_fee_lnk_f,
        acct1_credit_lmt as cr_lmt_amt,
        acct1_final_score as final_cr_score,
        acct1_prd_cd as prd_cd,
        acct1_proc_type_cd as proc_tp_cd,
        acct1_rqstd_credit_lmt as reqst_cr_lmt_amt,
        acct1_scrd_ind as scrd_tp_cd,
        acct1_scrty_type_cd as scrty_tp_cd,
        acct1_scrty_val as scrty_val_amt,
        acct1_src as src_cd,
        acct1_switch_ind as switch_cd,
        acct1_switch_xref as switch_xref,
        acct2_cacs_crc as colctn_ctr_cd,
        acct2_cls_reason_cd as acct_cls_rsn_cd,
        acct2_inact_ind as inact_cd,
        acct2_setup_transit as setp_trnst_num,
        -- We need to 0 pad these transits to avoid having them join to a spurious 4-character
        -- transit from ORG_UNIT_DIM (RRAP-151)
        lpad(acct2_transit_number::text, 5, '0') as trnst_num,
        annual_fee_ind as anul_fee_cd,
        corp_rtl_ind as corp_rtl_f,
        crnt_cycl_csh_advnc_bal_amt as csh_advnc_crnt_cycl_bal_amt,
        crnt_cycl_prch_bal_amt as prch_crnt_cycl_bal_amt,
        -- checking for NULL is done in ODBC_rcrr_revlvng_credit_mth_snapshot
        bal_hist_csh_adv_amt as bal_hist_csh_adv_amt,
        csh_advnc_rcvry_intr_amt as csh_advnc_rcvry_intr_amt,
        currency_cd as crncy_cd,
        delq_bns_dlqnt_day as bns_dlqnt_days,
        delq_chrg_off_cd as chrg_off_cd,
        delq_mth_dlqnt as mths_dlqnt_cnt,
        delq_orig_chng_off_amt as orig_chrg_off_amt,
        dlqnt_hist_01_to_12_day as dlqnt_hist_1_12,
        dlqnt_hist_13_to_24_day as dlqnt_hist_13_24,
        fncl_ytd_prch_amt as fscl_ytd_prchs_amt,
        fncl_ytd_return_amt as fscl_ytd_rtrns_amt,
        full_pymt_ind as full_pymt_cd,
        gl_acctng_transit as gl_trnst_num,
        last_pymt_amt as last_pymt_amt,
        last_yr_credit_intr_paid_amt as last_yr_cr_intr_pd_amt,
        last_yr_divdnd_rebate_amt as divdd_rbt_last_yr_amt,
        mdl_cd_3_cd as mdl_cd_3,
        mdl_cd_4_cd as mdl_cd_4,
        mdl_citifone_cd as mdl_citifone,
        mdl_compaction_cd as mdl_compaction,
        oprtr_last_online_updt as oprtr_last_onlne_updt,
        os_bal_coa_amt as tot_new_bal_amt,
        pad_oth_fncl_instn_ind as pad_oth_fncl_inst_f,
        prch_bal_hist_amt as bal_hist_prchs_amt,
        prch_rcvry_intr_amt as prch_rcvry_intr_amt,
        case when prev_1_cycl_csh_advnc_bal_amt is null then 0 else prev_1_cycl_csh_advnc_bal_amt end as csh_advnc_1_cycl_ago_bal_amt,
        prev_1_cycl_prch_bal_amt as prch_1_cycl_ago_bal_amt,
        prev_2_cycl_csh_advnc_bal_amt as csh_advnc_2_cycl_ago_bal_amt,
        prev_2_cycl_prch_bal_amt as prch_2_cycl_ago_bal_amt,
        prev_credit_score as cr_score,
        pymt_hist_amt as bal_hist_pymts_amt,
        return_envolope_cd as rtn_envelope,
        solicitation_cd as solctn_cd,
        stud_risk_bal_rto as risk_bal_rto,
        stud_spsp_ccli as spsp_ccli,
        tot_ctd_fncl_chrg as tot_cycl_to_dt_fncl_chrg_amt,
        tot_rcvry_intr_amt as tot_rcvry_intr_amt,
        tot_unpaid_fncl_chrg_amt as tot_unpaid_fncl_chrg_amt,
        tot_ytd_credit_intr_paid_amt as tot_ytd_cr_intr_pd_amt,
        visa_plastic_out as visa_plastic_out,
        ytd_csh_advnc_intr_chrgd_amt as ytd_csh_advnc_intr_chrgd_amt,
        ytd_csh_advnc_intr_paid_amt as ytd_csh_advnc_intr_pd,
        ytd_prch_cnt as ytd_prchs_cnt,
        ytd_prch_intr_chrgd_amt as ytd_prchs_intr_chrgd_amt,
        ytd_prch_intr_paid_amt as ytd_prchs_intr_pd_amt,
        -- # string_from_date columns we'll pass in directly since they're coming to us 
        -- # as string already. If the format is not correct we may need to do some additional 
        -- # transformation
        acct1_last_active_dt as last_acty_dt,
        acct1_last_blocked_dt as last_blocked_dt,
        acct1_last_online_updt_dt as last_onlne_updt_dt,
        acct1_last_prch_dt as last_prch_dt,
        acct1_open_dt as acct_opnd_dt,
        acct1_sub_prd_tfr_dt as sub_prd_tfr_dt,
        acct1_switch_dt as switch_dt,
        acct2_first_use_dt as frst_use_dt,
        delq_chrg_off_dt as chrg_off_dt,
        fin1_last_pymt_dt as last_pymt_dt,
        fin2_nal_dt as non_accrl_dt,
        fin2_wor_dt as write_off_dt,
        acct1_next_renew_fee_dt_mth_yr % 100 as yr2dig,
        floor(acct1_next_renew_fee_dt_mth_yr / 100)::int as mth,
        CASE WHEN yr2dig BETWEEN 80 and 99 THEN 1900 + yr2dig ELSE 2000 + yr2dig END as yr4dig,
        CASE WHEN acct1_next_renew_fee_dt_mth_yr > 0 
            THEN make_date(yr4dig, mth, 1) 
            ELSE make_date(9999, 12, 31) 
        END as next_rnew_fee_dt,
        CASE WHEN agreement_no=0 THEN '' ELSE agreement_no END as step_pln_agrmnt_num,
        businesseffectivedate,
        mth_end_dt,
        acct_num,
        -- Columns that did not appear in the transformation code extracted from XML. Odd
        chip_f,
        cmpgn_notified_cd,
        cmpgn_stat_cd,
        '' as non_responder_f,
        prev_sub_prd_cd,
        sub_prd_cd,
        crnt_bill_cd,
        rltnp_prim_cust_id as prim_cust_id,
        acct1_block_cd || acct1_recl_cd as block_recl_cd,
        last_addr_chng_dt,
        gl_acct_num
    FROM '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq007/join_57.parquet' as j57
    """,
    export_params={},
    clear_before_write=True,
)
def transformer_30():
    """
    This task is currently commented out as the source table `v_ux_ux300u2` in EDL-R returns zero records for the
     relevant month-end date, which breaks the downstream join. Once EDL-R supports returning the expected records for this 
     table, the SQL, schema, and other parameters can be defined and this task can be enabled.
    """
    pass


""" TaskFlow function calls """
odbc_tsz_kq_acct = odbc_tsz_kq_acct()
create_sq007_rundir = create_sq007_rundir()
odbc_tsz_bcname = odbc_tsz_bcname()
join_bcname = join_bcname()
join_bcname_nodup = join_bcname_nodup()
xft_src_to_tgt = xft_src_to_tgt()
odbc_rcrr_revlvng_credit_mth_snapshot = odbc_rcrr_revlvng_credit_mth_snapshot()
odbc_ez_step_acct_rltnp_snapshot = odbc_ez_step_acct_rltnp_snapshot()
odbc_airb_cust_acct_rltnp = odbc_airb_cust_acct_rltnp()
join_15 = join_15()
join_25 = join_25()
odbc_v_ux_ux300u2 = odbc_v_ux_ux300u2()
convert_xref_odbc_v_ux_ux300u2 = convert_xref_odbc_v_ux_ux300u2()
join_57 = join_57()
transformer_30 = transformer_30()

""" Dependency chaining """
create_sq007_rundir >> [
    odbc_tsz_kq_acct,
    odbc_tsz_bcname,
    odbc_v_ux_ux300u2,
    odbc_rcrr_revlvng_credit_mth_snapshot,
    odbc_ez_step_acct_rltnp_snapshot,
    odbc_airb_cust_acct_rltnp
]
odbc_v_ux_ux300u2 >> convert_xref_odbc_v_ux_ux300u2

[
    odbc_tsz_kq_acct,
    odbc_tsz_bcname,
] >> join_bcname
join_bcname >> join_bcname_nodup >> xft_src_to_tgt >> join_25

[
    odbc_rcrr_revlvng_credit_mth_snapshot,
    odbc_ez_step_acct_rltnp_snapshot,
    odbc_airb_cust_acct_rltnp
] >> join_15

[
    join_15,
    xft_src_to_tgt
] >> join_25

[
    convert_xref_odbc_v_ux_ux300u2,
    join_25
] >> join_57
join_57 >> transformer_30
