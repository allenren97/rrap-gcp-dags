import os
import pyarrow as pa
from airflow.sdk import task, get_current_context


@task
def create_sq008_rundir():
    """
    Task to create RUNDIR for sequence sq008.
    RUNDIR is the directory where extracted data for the sequence is stored.
    """
    context = get_current_context()
    rundir = context["ti"].xcom_pull(task_ids="handle_month_context", key="RUNDIR")
    sq008_rundir = f"{rundir}/sq008"
    os.makedirs(sq008_rundir, exist_ok=True)


@task.beeline(
    task_id="get_rcrr_mortgage_mth_snapshot",
    beeline_conn_id="edlr-conn",
    target="rcrr_mortgage_mth_snapshot.parquet",
    sql="""
    use {{ var.value.RCRR_SCHEMA }};
    SELECT
        mth_end_dt,
        src_sys_cd,
        mort_num,
        substring(STATE_DISTR_1_FREQ_NM, 1, 1) as float_cd,
        (case when acct_stat_cd = 'CL' then 'Y' else 'N' end) as pd_off_f,
        (case when FRCLS_FORECL_DT is not null then 'Y' else null end) as frclsr_f,
        (case when DELQ_DAY_CNT is null or DELQ_DAY_CNT <= 0 then 0 else floor(DELQ_DAY_CNT / 30) end) as mth_in_arrs_cnt,
        COND_ACCT_SUB_TYPE_CD as insur_cl_cd,
        (case when REF_PRPTY_TYPE_DESC is not null then substring(REF_PRPTY_TYPE_DESC, 1, 6) else null end) as PRPTY_CD,
        floor(GLMAP_INVESTOR_CD) as FUND_CD,
        STATE_ORIG_DISBURS_DT as INTR_ADJ_DT,
        OS_BAL_COA_AMT as crnt_bal_amt,
        COND_OWNSHP_BRANCH_TRANSIT as SERV_BR_TRNST_NUM,
        COND_ORIG_BRANCH_TRANSIT as PROC_BR_TRNST_NUM,
        STATE_LOAN_AUTH_DT as MORT_AUTH_DT,
        CLS_ACCT_CLS_DT as PD_OFF_DT,
        STATE_ACCT_MTUR_DT as CRNT_TERM_MAT_DT,
        RNWL_DT as LAST_RNEW_DT,
        COND_ORIG_LOAN_AMT as AUTH_AMT,
        INT_TOT_INT_DUE_AMT as INTR_DUE_AMT,
        COLL_COLL_VAL_AMT as LND_VAL,
        ESCR_TOT_ESCR_BAL_AMT as TAX_CRNT_BAL_AMT,
        INT_ACCR_INT_AMT as INTR_ACCR_AMT,
        STATE_TOT_DISBURS_AMT as TOT_ADVNC_AMT,
        (case when trim(COND_PROD_GRP_CD) = 'COM' then 'NP' else 'P' end) as brwer_cd,
        COLL_BUILDNG_VAL_AMT as APRSD_LAST_BUILDING_VAL,
        COLL_LAND_VAL_AMT as APRSD_LAND_VAL,
        null as APRSD_LAST_LAND_VAL,
        null as APRSD_BUILDING_VAL,
        COLL_ORIG_APPR_LAND_VAL_AMT as APRSD_ORIG_LAND_VAL,
        COLL_ORIG_APPR_BUILDNG_VAL_AMT as APRSD_ORIG_BUILDING_VAL,
        STATE_LAST_LOAN_ADV_DT as FINAL_ADVNC_DT,
        null as LIFE_INSUR_CD,
        (case when substring(STATE_DISTR_1_FREQ_NM, 1, 1) in ('W','B','S') then STATE_DISTR_1_NEXT_DUE_DT else null end) as WK_FRST_UNPAID_DT,
        COND_BUS_SRC_CD as BUS_SRC_CD,
        concat(case
            when COLL_PRPTY_TYPE_CD = 165 then '66'
            when COLL_PRPTY_TYPE_CD = 211 then '31'
            when COLL_PRPTY_TYPE_CD = 900 then '09'
            when COLL_PRPTY_TYPE_CD = 101 then (case when COLL_PRPTY_AGE_VAL = 0 then '05' else '01' end)
            when COLL_PRPTY_TYPE_CD = 111 then (case when COLL_PRPTY_AGE_VAL = 0 then '15' else '11' end)
            when COLL_PRPTY_TYPE_CD = 191 then (case when COLL_PRPTY_AGE_VAL = 0 then '95' else '91' end)
            when COLL_PRPTY_TYPE_CD = 221 then (case when COLL_PRPTY_AGE_VAL = 0 then '25' else '21' end)
            when COLL_PRPTY_TYPE_CD in (160,162,163,164,167,168) then cast(COLL_PRPTY_TYPE_CD % 100 as varchar(2))
            when COLL_PRPTY_TYPE_CD in (261,361,461,561,661,761)
                then (case when COLL_PRPTY_AGE_VAL = 0 then '65' else cast(COLL_PRPTY_TYPE_CD % 100 as varchar(2)) end)
            else '00' end, lpad(COLL_UNIT_CNT, 3, '0')) as SCRTY_TP_2,
        (case when substring(STATE_DISTR_1_FREQ_NM, 1, 1) = 'M' then STATE_DISTR_1_NEXT_DUE_DT else null end) as FRST_UNPAID_DT,
        null as MONTREAL_TRUST_DSBLTY_STAT_CD,
        COLL_SLS_PRC_AMT as SALE_DT_VAL,
        null as MRKTING_1_CD,
        null as MRKTING_2_CD,
        null as MRKTING_3_CD,
        null as MRKTING_4_CD,
        null as MRKTING_5_CD,
        STATE_CUST_RISK_CD as CRI_CD,
        STATE_ACCT_RISK_F as ARI_CD,
        null as PVSN_AMT,
        STATE_LOAN_AUTH_DT as LOAN_AUTH_DT,
        null as HLTH_CRSIS_PRTCTN_INSUR_STAT_CD,
        COND_PROD_TYPE_CD as ACCT_TP_CD,
        COLL_PROV_STATE_CD as COLL_PROV_STATE_CD,
        (case
            when STATE_LOAN_AUTH_DT <= '1999-12-31' then '19'
            when STATE_LOAN_AUTH_DT >= '2000-01-01' then '20'
        else NULL end) as PRPTY_PROV_CD,
        RNWL_DT as RENEWED_DT,
        cast(replace(COND_PYMT_TERM_CD, 'M', '') as integer) as MORT_TERM_MTH,
        INT_INT_RT_MAX_PCT as MAX_INTR_RT,
        null as YR_5_MTH_6_F,
        date_format(current_timestamp, 'yyyy-MM-dd HH:mm:ss.SSSSSS') as INSTR_PROCESS_TMSTMP,
        PRPAY_YTD_AMT as YTD_PRPY_AMT,
        coalesce(STATE_REMAIN_AMORT_MM_CNT, -1) as AMORT_MTH,
        concat(
            substring(COLL_FIRST_LGL_DESC, 1, 30),
            substring(COLL_ADDR_LINE_1_TXT, 1, 25),
            substring(COLL_CITY_NM, 1, 18),
            COLL_PROV_STATE_CD
        ) as PRPTY_ADDR,
        trim(GL_ACCT_NUM) as GL_ACCT_NUM,
        GL_ACCTNG_TRANSIT as GL_TRNST_NUM,
        trim(COND_PROD_GRP_CD) as PRD_GRP_CD,
        COLL_UNIT_CNT as UNIT_CNT,
        (case when trim(COND_PROD_GRP_CD) = 'COM' or COLL_UNIT_CNT >= 5 then 'Commercial' else 'Residential' end) as COMM_TP,
        {{ task_instance.xcom_pull(task_ids='handle_month_context', key='MTH_TM_ID') }} as mth_tm_id
    FROM MORTGAGE_MTH_SNAPSHOT
    WHERE mth_end_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="MTH_END_DT") }}' AND GL_ACCT_NUM <> '1571664';
    """,
    schema=pa.schema([
        ('mth_end_dt', pa.date64()),
        ('src_sys_cd', pa.string()),
        ('mort_num', pa.int64()),
        ('float_cd', pa.string()),
        ('pd_off_f', pa.string()),
        ('frclsr_f', pa.string()),
        ('mth_in_arrs_cnt', pa.int64()),
        ('insur_cl_cd', pa.string()),
        ('PRPTY_CD', pa.string()),
        ('FUND_CD', pa.string()),
        ('INTR_ADJ_DT', pa.date64()),
        ('crnt_bal_amt', pa.float64()),
        ('SERV_BR_TRNST_NUM', pa.string()),
        ('PROC_BR_TRNST_NUM', pa.string()),
        ('MORT_AUTH_DT', pa.date64()),
        ('PD_OFF_DT', pa.date64()),
        ('CRNT_TERM_MAT_DT', pa.date64()),
        ('LAST_RNEW_DT', pa.date64()),
        ('AUTH_AMT', pa.float64()),
        ('INTR_DUE_AMT', pa.float64()),
        ('LND_VAL', pa.float64()),
        ('TAX_CRNT_BAL_AMT', pa.float64()),
        ('INTR_ACCR_AMT', pa.float64()),
        ('TOT_ADVNC_AMT', pa.float64()),
        ('brwer_cd', pa.string()),
        ('APRSD_LAST_BUILDING_VAL', pa.float64()),
        ('APRSD_LAND_VAL', pa.float64()),
        ('APRSD_LAST_LAND_VAL', pa.float64()),
        ('APRSD_BUILDING_VAL', pa.float64()),
        ('APRSD_ORIG_LAND_VAL', pa.float64()),
        ('APRSD_ORIG_BUILDING_VAL', pa.float64()),
        ('FINAL_ADVNC_DT', pa.date64()),
        ('LIFE_INSUR_CD', pa.int64()),
        ('WK_FRST_UNPAID_DT', pa.date64()),
        ('BUS_SRC_CD', pa.int64()),
        ('SCRTY_TP_2', pa.string()),
        ('FRST_UNPAID_DT', pa.date64()),
        ('MONTREAL_TRUST_DSBLTY_STAT_CD', pa.string()),
        ('SALE_DT_VAL', pa.float64()),
        ('MRKTING_1_CD', pa.string()),
        ('MRKTING_2_CD', pa.string()),
        ('MRKTING_3_CD', pa.string()),
        ('MRKTING_4_CD', pa.string()),
        ('MRKTING_5_CD', pa.string()),
        ('CRI_CD', pa.string()),
        ('ARI_CD', pa.string()),
        ('PVSN_AMT', pa.float64()),
        ('LOAN_AUTH_DT', pa.date64()),
        ('HLTH_CRSIS_PRTCTN_INSUR_STAT_CD', pa.int64()),
        ('ACCT_TP_CD', pa.int64()),
        ('COLL_PROV_STATE_CD', pa.string()),
        ('PRPTY_PROV_CD', pa.string()),
        ('RENEWED_DT', pa.date64()),
        ('MORT_TERM_MTH', pa.int64()),
        ('MAX_INTR_RT', pa.float64()),
        ('YR_5_MTH_6_F', pa.string()),
        ('INSTR_PROCESS_TMSTMP', pa.timestamp('us')),
        ('YTD_PRPY_AMT', pa.float64()),
        ('AMORT_MTH', pa.int64()),
        ('PRPTY_ADDR', pa.string()),
        ('GL_ACCT_NUM', pa.string()),
        ('GL_TRNST_NUM', pa.string()),
        ('PRD_GRP_CD', pa.string()),
        ('UNIT_CNT', pa.int64()),
        ('COMM_TP', pa.string()),
        ('MTH_TM_ID', pa.int64())
    ]),
    rundir="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq008",
    to_parquet=True,
    tmpfileloc="/bns/rrap/data/tmp",
)
def get_rcrr_mortgage_mth_snapshot():
    pass


@task.beeline(
    task_id="join_1",
    beeline_conn_id="edlr-conn",
    target="join_1.parquet",
    sql="""
    use {{ var.value.CRZ_AIRB_SCHEMA }};
    with max_bus_eff_dt as
    (select max(businesseffectivedate) as max_bed from v_ux_ux300u2 where businesseffectivedate <= '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="MTH_END_DT") }}' )
    SELECT
        max_bus_eff_dt.max_bed as businesseffectivedate,
        max(b.agreement_no) as agreement_no,
        b.cab_transit,
        b.account_no AS acct_num,
        coalesce(c.bor_prd_src_sys_cd, b.product_cde) AS source_sys_cd
    FROM v_ux_ux300u2 b, max_bus_eff_dt
    LEFT OUTER JOIN {{ var.value.RCRR_SCHEMA }}.non_bor_prd_mappng c
    ON b.product_cde = c.non_bor_prd_cd
    AND c.src_sys_cd = 'UX'
    WHERE (b.businesseffectivedate BETWEEN c.eff_from_dt AND c.eff_to_dt)
    AND b.businesseffectivedate = max_bus_eff_dt.max_bed
    group by max_bus_eff_dt.max_bed, b.cab_transit, b.account_no, coalesce(c.bor_prd_src_sys_cd, b.product_cde)
    """,
    schema=pa.schema([
        ('businesseffectivedate', pa.date64()),
        ('agreement_no', pa.float64()),
        ('cab_transit', pa.string()),
        ('acct_num', pa.string()),
        ('source_sys_cd', pa.string())
    ]),
    rundir="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq008",
    to_parquet=True,
    tmpfileloc="/bns/rrap/data/tmp",
)
def join_1():
    pass


@task.parquet(
    task_id="extract_lnk_from_mortgage",
    duckdb_conn_id="duckdb-conn",
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq008/lnk_from_mortgage.parquet",
    sql="""
    SELECT
        src.*,
        CAST(CASE WHEN d.acct_num IS NOT NULL THEN 'Y' ELSE 'N' END AS VARCHAR(1)) AS scotia_tot_eqty_pln_f,
        CAST(CAST(d.agreement_no AS BIGINT) AS VARCHAR(9)) AS step_pln_agrmnt_num
    FROM '{{ task_instance.xcom_pull(task_ids='sq008.sq008_source.get_rcrr_mortgage_mth_snapshot', key='parquet') }}' src
    LEFT OUTER JOIN '{{ task_instance.xcom_pull(task_ids='sq008.sq008_source.join_1', key='parquet') }}' d
        ON src.mort_num = cast(d.acct_num as bigint)
        AND trim(src.src_sys_cd) = trim(d.source_sys_cd)
    """,
    export_params={},
    clear_before_write=True,
)
def extract_lnk_from_mortgage():
    pass


@task.parquet(
    task_id="extract_lnk_from_cust_acct",
    duckdb_conn_id="duckdb-conn",
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq008/lnk_from_cust_acct.parquet",
    sql="""
    select
        cast(cust_id as varchar(40)) as cust_id,
        mth_end_dt,
        cast(acct_num as varchar(80)) as mort_num
    from '{{ task_instance.xcom_pull(task_ids='sq004.sq004_source.airb_cust_acct_rltnp', key='parquet') }}'
    where primary_acct_holder_f = 'Y' and src_sys_cd = 'GZ'
    """,
    export_params={},
    clear_before_write=True,
)
def extract_lnk_from_cust_acct():
    pass


@task.parquet(
    task_id="extract_lnk_join_01",
    duckdb_conn_id="duckdb-conn",
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq008/lnk_join_01.parquet",
    sql="""
    SELECT m.*, c.cust_id
    FROM '{{ task_instance.xcom_pull(task_ids='sq008.sq008_source.extract_lnk_from_mortgage', key='parquet') }}' m
    LEFT OUTER JOIN '{{ task_instance.xcom_pull(task_ids='sq008.sq008_source.extract_lnk_from_cust_acct', key='parquet') }}' c
        ON trim(cast(m.mort_num as varchar(10))) = trim(cast(c.mort_num as varchar(10)))
        AND m.mth_end_dt = c.mth_end_dt
    """,
    export_params={},
    clear_before_write=True,
)
def extract_lnk_join_01():
    pass


@task.beeline(
    task_id="extract_lnk_gz_tgz",
    beeline_conn_id="edlr-conn",
    target="lnk_gz_tgz.parquet",
    sql="""
    use {{ var.value.TSZ_SCHEMA }};
    select
        cast(mort_num as varchar(80)) as mort_num,
        suspense_bal_amt as tot_susp_bal_amt,
        cast(businesseffectivedate as varchar(10)) as businesseffectivedate
    from gz_tgz_suspense_bal;
    """,
    schema=pa.schema([
        ('MORT_NUM', pa.string()),
        ('TOT_SUSP_BAL_AMT', pa.float64()),
        ('businesseffectivedate', pa.string())
    ]),
    rundir="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq008",
    to_parquet=True,
    tmpfileloc="/bns/rrap/data/tmp",
)
def extract_lnk_gz_tgz():
    pass


@task.beeline(
    task_id="extract_lnk_prov_src",
    beeline_conn_id="edlr-conn",
    target="lnk_prov_src.parquet",
    sql="""
    use {{ var.value.CRZ_AIRB_SCHEMA }};
    select
        cast(prov_cd as varchar(2)) as prov_cd,
        cast(prov_id as varchar(2)) as prov_id
    from
        (select
            case when prov_cd = 'NF' then 'NL' else prov_cd end as prov_cd,
            prov_id,
            bus_eff_dt,
            rank() over (order by bus_eff_dt desc) as r
         from airb_prov_ref_lkp) s
    where s.r = 1;
    """,
    schema=pa.schema([
        ('PROV_CD', pa.string()),
        ('PROV_ID', pa.string()),
    ]),
    rundir="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq008",
    to_parquet=True,
    tmpfileloc="/bns/rrap/data/tmp",
)
def extract_lnk_prov_src():
    pass


@task.parquet(
    task_id="join_2",
    duckdb_conn_id="duckdb-conn",
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq008/join_2.parquet",
    sql="""
    select j.*, gz.tot_susp_bal_amt, pv.prov_id
    from '{{ task_instance.xcom_pull(task_ids='sq008.sq008_source.extract_lnk_join_01', key='parquet') }}' j
    left outer join '{{ task_instance.xcom_pull(task_ids='sq008.sq008_source.extract_lnk_gz_tgz', key='parquet') }}' gz
        on (j.mth_end_dt::varchar = gz.businesseffectivedate and j.mort_num::varchar = gz.mort_num)
    left outer join '{{ task_instance.xcom_pull(task_ids='sq008.sq008_source.extract_lnk_prov_src', key='parquet') }}' pv
        on j.coll_prov_state_cd = pv.prov_cd
    """,
    export_params={},
    clear_before_write=True,
)
def join_2():
    pass


create_sq008_rundir = create_sq008_rundir()
get_rcrr_mortgage_mth_snapshot = get_rcrr_mortgage_mth_snapshot()
join_1 = join_1()
extract_lnk_from_mortgage = extract_lnk_from_mortgage()
extract_lnk_from_cust_acct = extract_lnk_from_cust_acct()
extract_lnk_join_01 = extract_lnk_join_01()
extract_lnk_gz_tgz = extract_lnk_gz_tgz()
extract_lnk_prov_src = extract_lnk_prov_src()
join_2 = join_2()


create_sq008_rundir >> [
    get_rcrr_mortgage_mth_snapshot,
    join_1,
    extract_lnk_from_cust_acct,
    extract_lnk_gz_tgz,
    extract_lnk_prov_src,
]

[get_rcrr_mortgage_mth_snapshot, join_1] >> extract_lnk_from_mortgage
[extract_lnk_from_mortgage, extract_lnk_from_cust_acct] >> extract_lnk_join_01
[extract_lnk_join_01, extract_lnk_gz_tgz, extract_lnk_prov_src] >> join_2
