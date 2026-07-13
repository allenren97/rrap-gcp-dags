import os
import pyarrow as pa
from airflow.sdk import task, get_current_context


@task
def create_sq006_rundir():
    """
    Task to create RUNDIR for sequence sq006.
    RUNDIR is the directory where extracted data for the sequence is stored.
    """
    context = get_current_context()
    rundir = context["ti"].xcom_pull(task_ids="handle_month_context", key="RUNDIR")
    sq006_rundir = f"{rundir}/sq006"
    os.makedirs(sq006_rundir, exist_ok=True)


@task.parquet(
    task_id="extract_airb_cust_acct_rltnp",
    duckdb_conn_id="duckdb-conn",
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq006/airb_cust_acct_rltnp.parquet",
    sql="""
    select
        cust_id,
        trim(acct_num) as acct_num,
        trim(src_sys_cd) as src_sys_cd
    from
        '{{ task_instance.xcom_pull(task_ids="sq004.sq004_source.airb_cust_acct_rltnp", key="parquet") }}'
    where
        primary_acct_holder_f = 'Y'
        and mth_end_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="MTH_END_DT") }}'
    """,
    export_params={},
    clear_before_write=True,
)
def extract_airb_cust_acct_rltnp():
    pass


@task.beeline(
    task_id="extract_psnl_loan_mth_snapshot",
    beeline_conn_id="edlr-conn",
    sql="""
    use {{ var.value.RCRR_SCHEMA }};
    select
        SRC_SYS_CD as SRC_SYS_CD,
        MTH_END_DT as MTH_END_DT,
        TRIM(ACCT_NUM) as ACCT_NUM,
        BRANCH_LOCTN_TRANSIT as TRNST_NUM,
        LOAN_NUM as LOAN_NUM,
        RECD_STAT_CD as RECD_STAT_CD,
        RECD_STAT_DT as RECD_STAT_DT,
        CUST_RESIDENCE_CD as CUST_RSDNC_CD,
        TYPE_SRC_CD as TP_SRC_CD,
        LOAN_PURPOSE_CD as LOAN_PRPS_CD,
        SCRTY_CD as SCRTY_CD,
        RATING_CD as RT_CD,
        PROMISSORS_CNT as PROMISSORS_CNT,
        GUAR_CNT as GRNT_CNT,
        COMMERCIAL_LOAN_CD as COMM_LOAN_CD,
        NOTE_DT as NOTE_DT,
        FIRST_RGL_PYMT_DT as FRST_RGL_PYMT_DT,
        LAST_RGL_PYMT_DT as LAST_RGL_PYMT_DT,
        ORIG_LOAN_AMT as ORIG_LOAN_AMT,
        ADD_ON_BAL_AMT as ADD_ON_BAL_AMT,
        ADD_ON_INTR_AMT as ADD_ON_INTR_AMT,
        DAY_OVERDUE as DAYS_ODUE,
        ACCR_INTR_AMT as ACCR_INTR_AMT,
        EARLY_MATURITY_DT as EARLY_MAT_DT,
        LAST_PYMT_DT as LAST_PYMT_DT,
        PRNCPL_BAL_AMT as PRINCIPAL_BALANCE_AMT,
        case
            when SCRTY_VEHCL_VAL > 0
            then SCRTY_VEHCL_VAL
            else MARKETABLE_SCRTY_VAL
        end as MOTOR_VEHCL_VAL,
        SCRTY_HOUSEHOLD_CREDIT_SCORE as SECURITY_HOUSEHOLD_CR_SCORE,
        SCRTY_OTH_VAL as SCRTY_OTH_VAL,
        PLS_CREDIT_SCORE as PLS_CR_SCORE_OVRD_CD,
        ORIG_CAB_TRANSIT as BR_LOCTN_TRNST,
        EARNED_MTH_INTR_AMT as EARNED_MTH_INTR_AMT,
        ORIG_NOTE_DT as ORIG_NOTE_DT,
        CHRG_OFF_DT as CHRG_OFF_DT,
        CHRG_OFF_AMT as CHRG_OFF_AMT,
        SECURITIZATION_CD as SECRTZTN_CD,
        LOAN_TERM as LOAN_TERM,
        EARLY_MATURITY_TERM as EARLY_MAT_TERM,
        EARLY_MATURITY_STAT_CD as EARLY_MAT_STAT_CD,
        RGL_PYMT_AMT as RGL_PYMT_AMT,
        PRE_AUTH_DEBIT_PYMT_FREQ_CD as PRE_AUTHORIZED_DR_PYMT_FREQ_CD,
        INTR_RATE as INTR_RT,
        CIF_COMPANY_ID as CIF_COMPANY_ID,
        CIF_CUST_ID as CIF_CUST_ID,
        CIF_CUST_ID_TIE_BREAKER as CIF_CUST_ID_TIE_BRKR,
        BOOKED_AMT as BOOKED_AMT,
        GL_ACCT_NUM as GL_ACCT_NUM,
        GL_ACCTNG_TRANSIT as GL_TRNST_NUM,
        CURRENCY_CD as CRNCY_CD
    from
        psnl_loan_mth_snapshot as main
    where
        main.mth_end_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="MTH_END_DT") }}'
    ;
    """,
    schema=pa.schema([
        ("SRC_SYS_CD", pa.string()),
        ("MTH_END_DT", pa.date64()),
        ("ACCT_NUM", pa.string()),
        ("TRNST_NUM", pa.string()),
        ("LOAN_NUM", pa.string()),
        ("RECD_STAT_CD", pa.string()),
        ("RECD_STAT_DT", pa.string()),
        ("CUST_RSDNC_CD", pa.string()),
        ("TP_SRC_CD", pa.string()),
        ("LOAN_PRPS_CD", pa.string()),
        ("SCRTY_CD", pa.string()),
        ("RT_CD", pa.string()),
        ("PROMISSORS_CNT", pa.int64()),
        ("GRNT_CNT", pa.int64()),
        ("COMM_LOAN_CD", pa.string()),
        ("NOTE_DT", pa.string()),
        ("FRST_RGL_PYMT_DT", pa.string()),
        ("LAST_RGL_PYMT_DT", pa.string()),
        ("ORIG_LOAN_AMT", pa.float64()),
        ("ADD_ON_BAL_AMT", pa.float64()),
        ("ADD_ON_INTR_AMT", pa.float64()),
        ("DAYS_ODUE", pa.int64()),
        ("ACCR_INTR_AMT", pa.float64()),
        ("EARLY_MAT_DT", pa.string()),
        ("LAST_PYMT_DT", pa.string()),
        ("PRINCIPAL_BALANCE_AMT", pa.float64()),
        ("MOTOR_VEHCL_VAL", pa.float64()),
        ("SECURITY_HOUSEHOLD_CR_SCORE", pa.int64()),
        ("SCRTY_OTH_VAL", pa.float64()),
        ("PLS_CR_SCORE_OVRD_CD", pa.string()),
        ("BR_LOCTN_TRNST", pa.string()),
        ("EARNED_MTH_INTR_AMT", pa.float64()),
        ("ORIG_NOTE_DT", pa.string()),
        ("CHRG_OFF_DT", pa.string()),
        ("CHRG_OFF_AMT", pa.float64()),
        ("SECRTZTN_CD", pa.string()),
        ("LOAN_TERM", pa.int64()),
        ("EARLY_MAT_TERM", pa.int64()),
        ("EARLY_MAT_STAT_CD", pa.string()),
        ("RGL_PYMT_AMT", pa.float64()),
        ("PRE_AUTHORIZED_DR_PYMT_FREQ_CD", pa.string()),
        ("INTR_RT", pa.float64()),
        ("CIF_COMPANY_ID", pa.int64()),
        ("CIF_CUST_ID", pa.string()),
        ("CIF_CUST_ID_TIE_BRKR", pa.int64()),
        ("BOOKED_AMT", pa.float64()),
        ("GL_ACCT_NUM", pa.string()),
        ("GL_TRNST_NUM", pa.string()),
        ("CRNCY_CD", pa.string()),
    ]),
    target="psnl_loan_mth_snapshot.parquet",
    rundir="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq006",
    to_parquet=True,
    tmpfileloc="/bns/rrap/data/tmp",
)
def extract_psnl_loan_mth_snapshot():
    pass


@task.beeline(
    task_id="extract_v_ux_ux300u2",
    beeline_conn_id="edlr-conn",
    sql="""
    use {{ var.value.CRZ_AIRB_SCHEMA }};
    with max_bus_eff_dt as
    (
        select max(businesseffectivedate) as max_bed
        from v_ux_ux300u2
        where businesseffectivedate <= '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="MTH_END_DT") }}'
    )
    select
        agreement_no,
        substring(account_no, 7, 12) as account_no,
        cast('{{ task_instance.xcom_pull(task_ids="handle_month_context", key="MTH_END_DT") }}' as date) as businesseffectivedate,
        coalesce(v_non_bor_prd_mappng.bor_prd_src_sys_cd, product_cde) as src_sys_cd
    from v_ux_ux300u2 as u2, max_bus_eff_dt
    inner join {{ var.value.RCRR_SCHEMA }}.v_non_bor_prd_mappng
        on u2.product_cde = v_non_bor_prd_mappng.non_bor_prd_cd
        and v_non_bor_prd_mappng.src_sys_cd = 'UX'
    where
        u2.businesseffectivedate = max_bus_eff_dt.max_bed
        and (u2.businesseffectivedate between v_non_bor_prd_mappng.eff_from_dt and v_non_bor_prd_mappng.eff_to_dt)
    ;
    """,
    schema=pa.schema([
        ("AGREEMENT_NO", pa.float64()),
        ("ACCOUNT_NO", pa.string()),
        ("BUSINESSEFFECTIVEDATE", pa.date64()),
        ("SRC_SYS_CD", pa.string()),
    ]),
    target="v_ux_ux300u2.parquet",
    rundir="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq006",
    to_parquet=True,
    tmpfileloc="/bns/rrap/data/tmp",
)
def extract_v_ux_ux300u2():
    pass


@task.parquet(
    task_id="make_airb_psnl_loan_mth_snapshot",
    duckdb_conn_id="duckdb-conn",
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq006/airb_psnl_loan_mth_snapshot.parquet",
    sql="""
    select
        current_timestamp as insrt_process_tmstmp,
        main.*,
        airb.cust_id as prim_cust_id,
        cast(cast(u2.agreement_no as bigint) as varchar) as step_pln_agrmnt_num
    from
        '{{ task_instance.xcom_pull(task_ids='sq006.sq006_source.extract_psnl_loan_mth_snapshot', key='parquet') }}' as main
    left outer join
        '{{ task_instance.xcom_pull(task_ids='sq006.sq006_source.extract_airb_cust_acct_rltnp', key='parquet') }}' as airb
        on lpad(trim(main.acct_num), 23, '0') = lpad(trim(airb.acct_num), 23, '0')
        and main.src_sys_cd = airb.src_sys_cd
    left outer join
        '{{ task_instance.xcom_pull(task_ids='sq006.sq006_source.extract_v_ux_ux300u2', key='parquet') }}' as u2
        on lpad(trim(main.acct_num), 23, '0') = lpad(trim(u2.account_no), 23, '0')
        and main.src_sys_cd = u2.src_sys_cd
        and u2.businesseffectivedate = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="MTH_END_DT") }}'
    where
        main.mth_end_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="MTH_END_DT") }}'
    """,
    export_params={},
    clear_before_write=True,
)
def make_airb_psnl_loan_mth_snapshot():
    pass


create_sq006_rundir = create_sq006_rundir()
extract_airb_cust_acct_rltnp = extract_airb_cust_acct_rltnp()
extract_psnl_loan_mth_snapshot = extract_psnl_loan_mth_snapshot()
extract_v_ux_ux300u2 = extract_v_ux_ux300u2()
make_airb_psnl_loan_mth_snapshot = make_airb_psnl_loan_mth_snapshot()


create_sq006_rundir >> [
    extract_airb_cust_acct_rltnp,
    extract_psnl_loan_mth_snapshot,
    extract_v_ux_ux300u2,
]

[
    extract_airb_cust_acct_rltnp,
    extract_psnl_loan_mth_snapshot,
    extract_v_ux_ux300u2,
] >> make_airb_psnl_loan_mth_snapshot
