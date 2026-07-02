import os
import pyarrow as pa
from airflow.sdk import task, get_current_context


@task
def create_sq005_rundir():
    """
    Task to create RUNDIR for sequence sq005.
    RUNDIR is the directory where extracted data for the sequence is stored.
    """
    context = get_current_context()
    rundir = context["ti"].xcom_pull(task_ids="handle_month_context", key="RUNDIR")
    sq005_rundir = f"{rundir}/sq005"
    os.makedirs(sq005_rundir, exist_ok=True)


@task.beeline(
    task_id="get_v_ux_ux300u1",
    beeline_conn_id="edlr-conn",
    sql="""
    use {{ var.value.CRZ_AIRB_SCHEMA }};
    with max_bus_eff_dt as
    (
        SELECT max(businesseffectivedate) as max_bed from v_ux_ux300u1
        where businesseffectivedate <= '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="MTH_END_DT") }}'
    )
    SELECT
        AGREEMENT_NO AS ACCT_GRP_ID,
        SO_BLT AS BR_LOCTN_TRNST,
        CASE WHEN trim(SO_START_DTE) = "0" OR trim(SO_START_DTE)= "" THEN null ELSE trim(SO_START_DTE) END AS ST_DT,
        CASE WHEN trim(SO_LAST_CHG_DATE) = "0" OR trim(SO_LAST_CHG_DATE)= "" THEN null ELSE trim(SO_LAST_CHG_DATE) END AS LAST_CHNG_DT,
        PRPTY_DESC_LINE1 AS PRPTY_DESC_1,
        PRPTY_DESC_LINE2 AS PRPTY_DESC_2,
        PRPTY_DESC_LINE3 AS PRPTY_DESC_3,
        case
            when cast(PROVINCE_CODE as varchar(2)) = "PQ" then "QC"
            when (cast(PROVINCE_CODE as varchar(2)) = "LB" or cast(PROVINCE_CODE as varchar(2)) = "NF") then "NL"
            else cast(PROVINCE_CODE as varchar(2))
        end AS PRPTY_PROV_CD,
        ORGNTG_APPL_KEY AS ORGTNG_APP_NUM,
        APPRAISED_VAL AS APRSD_VAL,
        CASE WHEN trim(APPRAISED_DATE) = "0" OR trim(APPRAISED_DATE)= "" THEN null ELSE trim(APPRAISED_DATE) END AS APRSD_DT,
        SHEP_CREDIT_LIMIT AS CR_LMT_AMT,
        CASE WHEN trim(SHEP_CRED_LIM_DATE) = "0" OR trim(SHEP_CRED_LIM_DATE)= "" THEN null ELSE trim(SHEP_CRED_LIM_DATE) END AS CR_LMT_DT,
        RGSTRD_AMT AS RGSTRD_AMT,
        CASE WHEN trim(RGSTRN_DATE) = "0" OR trim(RGSTRN_DATE)= "" THEN null ELSE trim(RGSTRN_DATE) END AS RGSTRD_DT,
        RGSTRN_NMBR AS RGSTRD_NUM,
        DISCHG_REASN_CODE AS DISCHARGE_RSN_CD,
        SO_HRINSURER AS HI_RTO_INSURER_NM,
        CASE WHEN trim(SO_HR_START_DTE) = "0" OR trim(SO_HR_START_DTE)= "" THEN null ELSE trim(SO_HR_START_DTE) END AS HI_RTO_INSUR_ST_DT,
        CASE WHEN trim(SO_HR_END_DTE) = "0" OR trim(SO_HR_END_DTE)= "" THEN null ELSE trim(SO_HR_END_DTE) END AS HI_RTO_INSUR_END_DT,
        SO_HR_CUST1_CID AS PRIM_CUST_CID,
        SO_ALI_IND AS ALI_IND,
        CASE WHEN trim(SO_ALI_DATE) = "0" OR trim(SO_ALI_DATE)= "" THEN null ELSE trim(SO_ALI_DATE) END AS ALI_DT,
        SO_CTI_IND AS CTI_IND,
        CASE WHEN trim(SO_CTI_DATE) = "0" OR trim(SO_CTI_DATE)= "" THEN null ELSE trim(SO_CTI_DATE) END AS CTI_DT,
        SO_ALI_PENDING_AMT AS ALI_PNDG_AMT,
        SO_ALI_PRODUCT AS ALI_PRD_CD,
        SO_ALI_ACCT_NUM AS ALI_ACCT_NUM,
        CASE WHEN trim(SO_END_DTE) = "0" OR trim(SO_END_DTE)= "" THEN null ELSE trim(SO_END_DTE) END AS END_DT,
        SO_STATUS AS STAT_CD,
        CASE WHEN trim(DISCHG_DATE) = "0" OR trim(DISCHG_DATE)= "" THEN null ELSE trim(DISCHG_DATE) END AS DISCHARGE_DT,
        SO_HR_CUST2_CID AS SCNDRY_CUST_CID,
        CASE WHEN trim(SO_STATUS_CHG_DTE) = "0" OR trim(SO_STATUS_CHG_DTE)= "" THEN null ELSE trim(SO_STATUS_CHG_DTE) END AS STAT_CHNG_DT,
        ORGNTG_APPL_KEY AS CR_APP_NUM,
        SO_HRCERT_NO AS CRFC_NUM,
        SO_HR_REVPRIOD AS REV_PRD,
        SO_HR_PREMAMT AS PREM_AMT,
        SO_HR_APPLFEE AS APP_FEE,
        CASE WHEN trim(SO_HR_AUTHDATE) = "0" OR trim(SO_HR_AUTHDATE)= "" THEN null ELSE trim(SO_HR_AUTHDATE) END AS AUTH_DT
    FROM v_ux_ux300u1, max_bus_eff_dt
    WHERE businesseffectivedate = max_bus_eff_dt.max_bed;
    """,
    schema=pa.schema([
        ("ACCT_GRP_ID", pa.string()),
        ("BR_LOCTN_TRNST", pa.string()),
        ("ST_DT", pa.date64()),
        ("LAST_CHNG_DT", pa.date64()),
        ("PRPTY_DESC_1", pa.string()),
        ("PRPTY_DESC_2", pa.string()),
        ("PRPTY_DESC_3", pa.string()),
        ("PRPTY_PROV_CD", pa.string()),
        ("ORGTNG_APP_NUM", pa.string()),
        ("APRSD_VAL", pa.float64()),
        ("APRSD_DT", pa.date64()),
        ("CR_LMT_AMT", pa.float64()),
        ("CR_LMT_DT", pa.date64()),
        ("RGSTRD_AMT", pa.float64()),
        ("RGSTRD_DT", pa.date64()),
        ("RGSTRD_NUM", pa.string()),
        ("DISCHARGE_RSN_CD", pa.string()),
        ("HI_RTO_INSURER_NM", pa.string()),
        ("HI_RTO_INSUR_ST_DT", pa.date64()),
        ("HI_RTO_INSUR_END_DT", pa.date64()),
        ("PRIM_CUST_CID", pa.string()),
        ("ALI_IND", pa.string()),
        ("ALI_DT", pa.date64()),
        ("CTI_IND", pa.string()),
        ("CTI_DT", pa.date64()),
        ("ALI_PNDG_AMT", pa.float64()),
        ("ALI_PRD_CD", pa.string()),
        ("ALI_ACCT_NUM", pa.string()),
        ("END_DT", pa.string()),
        ("STAT_CD", pa.string()),
        ("DISCHARGE_DT", pa.string()),
        ("SCNDRY_CUST_CID", pa.string()),
        ("STAT_CHNG_DT", pa.string()),
        ("CR_APP_NUM", pa.string()),
        ("CRFC_NUM", pa.string()),
        ("REV_PRD", pa.int64()),
        ("PREM_AMT", pa.float64()),
        ("APP_FEE", pa.float64()),
        ("AUTH_DT", pa.date64()),
    ]),
    target="v_ux_ux300u1.parquet",
    rundir="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq005",
    to_parquet=True,
    tmpfileloc="/bns/rrap/data/tmp",
)
def get_v_ux_ux300u1():
    pass


@task.beeline(
    task_id="get_counts_from_u2",
    beeline_conn_id="edlr-conn",
    sql="""
    use {{ var.value.CRZ_AIRB_SCHEMA }};
    WITH max_bus_eff_dt as
    (
        SELECT max(businesseffectivedate) as max_bed from v_ux_ux300u2
        WHERE businesseffectivedate <= '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="MTH_END_DT") }}'
    )
    SELECT
        agreement_no,
        count(agreement_no) as acct_cnt
    FROM v_ux_ux300u2, max_bus_eff_dt c
    WHERE businesseffectivedate = c.max_bed
    GROUP BY agreement_no;
    """,
    schema=pa.schema([
        ("agreement_no", pa.string()),
        ("acct_cnt", pa.float64()),
    ]),
    target="v_ux_ux300u2_cnts.parquet",
    rundir="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq005",
    to_parquet=True,
    tmpfileloc="/bns/rrap/data/tmp",
)
def get_counts_from_u2():
    pass


@task.parquet(
    task_id="make_airb_step_pln_mth_snapshot",
    duckdb_conn_id="duckdb-conn",
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq005/airb_step_pln_mth_snapshot.parquet",
    sql="""
    SELECT
        '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="MTH_END_DT") }}' AS mth_end_dt,
        u2.acct_cnt,
        u1.acct_grp_id AS step_pln_agrmnt_num,
        u1.br_loctn_trnst,
        u1.st_dt,
        u1.last_chng_dt AS acty_dt,
        u1.prpty_desc_1,
        u1.prpty_desc_2,
        u1.prpty_desc_3,
        u1.prpty_prov_cd,
        u1.aprsd_val,
        u1.aprsd_dt,
        u1.cr_lmt_amt,
        u1.cr_lmt_dt,
        u1.rgstrd_amt,
        u1.rgstrd_dt,
        u1.rgstrd_num,
        u1.discharge_rsn_cd,
        u1.hi_rto_insurer_nm AS insurer_cd,
        u1.hi_rto_insur_st_dt AS hr_st_dt,
        u1.hi_rto_insur_end_dt AS hr_end_dt,
        u1.prim_cust_cid,
        u1.ali_ind AS auto_lmt_f,
        u1.ali_dt,
        u1.cti_ind AS cti_f,
        u1.cti_dt,
        u1.ali_pndg_amt,
        u1.ali_prd_cd,
        u1.ali_acct_num,
        u1.cr_app_num,
        u1.crfc_num,
        u1.rev_prd,
        u1.prem_amt,
        u1.app_fee,
        u1.auth_dt
    FROM '{{ task_instance.xcom_pull(task_ids='sq005.sq005_source.get_v_ux_ux300u1', key='parquet') }}' u1
    LEFT JOIN '{{ task_instance.xcom_pull(task_ids='sq005.sq005_source.get_counts_from_u2', key='parquet') }}' u2
        ON u1.acct_grp_id = u2.agreement_no
    """,
    export_params={},
    clear_before_write=True,
)
def make_airb_step_pln_mth_snapshot():
    pass


create_sq005_rundir = create_sq005_rundir()
get_v_ux_ux300u1 = get_v_ux_ux300u1()
get_counts_from_u2 = get_counts_from_u2()
make_airb_step_pln_mth_snapshot = make_airb_step_pln_mth_snapshot()


create_sq005_rundir >> [get_v_ux_ux300u1, get_counts_from_u2]
[get_v_ux_ux300u1, get_counts_from_u2] >> make_airb_step_pln_mth_snapshot
