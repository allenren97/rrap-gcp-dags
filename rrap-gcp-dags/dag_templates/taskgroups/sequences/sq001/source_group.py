import os
import pyarrow as pa
from airflow.sdk import task, get_current_context


@task
def create_sq001_rundir():
    """
    Task to create RUNDIR for sequence sq001.
    RUNDIR is the directory where extracted data for the sequence is stored.
    """
    context = get_current_context()
    rundir = context["ti"].xcom_pull(task_ids="handle_month_context", key="RUNDIR")
    sq001_rundir = f"{rundir}/sq001"
    os.makedirs(sq001_rundir, exist_ok=True)


@task.beeline(
    task_id="extract_lnk_rcrr_src",
    beeline_conn_id="edlr-conn",
    sql="""
    use {{ var.value.RCRR_SCHEMA }};
    SELECT
        MTH_END_DT,
        MORT_NUM,
        STATE_REMAIN_AMORT_MM_CNT,
        COLL_COLL_VAL_AMT,
        COLL_FIRST_LGL_DESC,
        COLL_ADDR_LINE_1_TXT,
        COLL_CITY_NM,
        COLL_PROV_STATE_CD,
        PRPAY_YTD_AMT,
        cond_orig_loan_amt,
        COND_ORIG_BRANCH_TRANSIT,
        STATE_DISTR_1_FREQ_NM,
        INT_ACCR_INT_AMT,
        state_orig_disburs_dt,
        state_loan_auth_dt,
        STATE_ACCT_MTUR_DT,
        ACCT_STAT_CD,
        STATE_TOT_DISBURS_AMT,
        COND_OWNSHP_BRANCH_TRANSIT,
        STATE_DISTR_1_NEXT_DUE_DT,
        COND_ACCT_SUB_TYPE_CD,
        GLMAP_INVESTOR_CD,
        coll_unit_cnt,
        coll_prpty_type_cd,
        OS_BAL_COA_AMT,
        FRCLS_FORECL_DT,
        CLS_ACCT_CLS_DT,
        GL_ACCT_NUM,
        CAST(SRC_SYS_CD as VARCHAR(20)) SRC_SYS_CD,
        TRIM(COND_PROD_GRP_CD) AS COND_PROD_GRP_CD,
        TRIM(coll_prpty_age_val) AS coll_prpty_age_val,
        GL_ACCTNG_TRANSIT,
        CURRENCY_CD
    FROM MORTGAGE_MTH_SNAPSHOT
    WHERE
        BUSINESS_MTH_END_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="MTH_END_DT") }}'
        AND GL_ACCT_NUM <> '1571664';
    """,
    schema=pa.schema([
        ("MTH_END_DT", pa.date64()),
        ("MORT_NUM", pa.int64()),
        ("STATE_REMAIN_AMORT_MM_CNT", pa.int16()),
        ("COLL_COLL_VAL_AMT", pa.float64()),
        ("COLL_FIRST_LGL_DESC", pa.string()),
        ("COLL_ADDR_LINE_1_TXT", pa.string()),
        ("COLL_CITY_NM", pa.string()),
        ("COLL_PROV_STATE_CD", pa.string()),
        ("PRPAY_YTD_AMT", pa.float64()),
        ("COND_ORIG_LOAN_AMT", pa.float64()),
        ("COND_ORIG_BRANCH_TRANSIT", pa.string()),
        ("STATE_DISTR_1_FREQ_NM", pa.string()),
        ("INT_ACCR_INT_AMT", pa.float64()),
        ("STATE_ORIG_DISBURS_DT", pa.date64()),
        ("STATE_LOAN_AUTH_DT", pa.date64()),
        ("STATE_ACCT_MTUR_DT", pa.date64()),
        ("ACCT_STAT_CD", pa.string()),
        ("STATE_TOT_DISBURS_AMT", pa.float64()),
        ("COND_OWNSHP_BRANCH_TRANSIT", pa.string()),
        ("STATE_DISTR_1_NEXT_DUE_DT", pa.date64()),
        ("COND_ACCT_SUB_TYPE_CD", pa.string()),
        ("GLMAP_INVESTOR_CD", pa.string()),
        ("COLL_UNIT_CNT", pa.int16()),
        ("COLL_PRPTY_TYPE_CD", pa.int64()),
        ("OS_BAL_COA_AMT", pa.float64()),
        ("FRCLS_FORECL_DT", pa.date64()),
        ("CLS_ACCT_CLS_DT", pa.date64()),
        ("GL_ACCT_NUM", pa.string()),
        ("SRC_SYS_CD", pa.string()),
        ("COND_PROD_GRP_CD", pa.string()),
        ("COLL_PRPTY_AGE_VAL", pa.string()),
        ("GL_ACCTNG_TRANSIT", pa.string()),
        ("CURRENCY_CD", pa.string()),
    ]),
    target="lnk_rcrr_src.parquet",
    rundir="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq001",
    to_parquet=True,
    tmpfileloc="/bns/rrap/data/tmp",
)
def extract_lnk_rcrr_src():
    pass


@task.beeline(
    task_id="extract_lnk_tm_src",
    beeline_conn_id="edlr-conn",
    sql="""
    use {{ var.value.RCRR_SCHEMA }};
    SELECT
        TM_ID,
        TM_LVL_END_DT
    FROM TM_DIM
    WHERE
        SUBSTR(TM_LVL, 1, 5) = 'Month'
        AND TM_LVL_END_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="MTH_END_DT") }}'
        AND TM_ID = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="MTH_TM_ID") }};
    """,
    schema=pa.schema([
        ("TM_ID", pa.int64()),
        ("TM_LVL_END_DT", pa.date64()),
    ]),
    target="lnk_tm_src.parquet",
    rundir="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq001",
    to_parquet=True,
    tmpfileloc="/bns/rrap/data/tmp",
)
def extract_lnk_tm_src():
    pass


@task.beeline(
    task_id="extract_lnk_prov_src",
    beeline_conn_id="edlr-conn",
    sql="""
    use {{ var.value.CRZ_AIRB_SCHEMA }};
    SELECT
        cast(PROV_CD AS varchar(2)) AS PROV_CD,
        cast(PROV_ID as varchar(2)) AS PROV_ID
    FROM (
        SELECT
            case when PROV_CD = 'NF' then 'NL' else PROV_CD end AS PROV_CD,
            PROV_ID,
            bus_eff_dt,
            rank() over (ORDER BY bus_eff_dt DESC) AS r
        FROM AIRB_PROV_REF_LKP
    ) s
    WHERE s.r = 1;
    """,
    schema=pa.schema([
        ("PROV_CD", pa.string()),
        ("PROV_ID", pa.string()),
    ]),
    target="lnk_prov_src.parquet",
    rundir="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq001",
    to_parquet=True,
    tmpfileloc="/bns/rrap/data/tmp",
)
def extract_lnk_prov_src():
    pass


@task.parquet(
    task_id="join_lnk_tm_src",
    duckdb_conn_id="duckdb-conn",
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq001/join_lnk_tm_src.parquet",
    sql="""
    SELECT
        main.*, tm.tm_id
    FROM '{{ task_instance.xcom_pull(task_ids='sq001.sq001_source.extract_lnk_rcrr_src', key='parquet') }}' AS main
    LEFT JOIN '{{ task_instance.xcom_pull(task_ids='sq001.sq001_source.extract_lnk_tm_src', key='parquet') }}' AS tm
        ON main.mth_end_dt = tm.tm_lvl_end_dt
    """,
    export_params={},
    clear_before_write=True,
)
def join_lnk_tm_src():
    pass


@task.parquet(
    task_id="join_lnk_prov_src",
    duckdb_conn_id="duckdb-conn",
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq001/join_lnk_prov_src.parquet",
    sql="""
    SELECT
        main.*, prov.prov_id
    FROM '{{ task_instance.xcom_pull(task_ids='sq001.sq001_source.join_lnk_tm_src', key='parquet') }}' AS main
    LEFT JOIN '{{ task_instance.xcom_pull(task_ids='sq001.sq001_source.extract_lnk_prov_src', key='parquet') }}' AS prov
        ON main.coll_prov_state_cd = prov.prov_cd
    """,
    export_params={},
    clear_before_write=True,
)
def join_lnk_prov_src():
    pass


@task.beeline(
    task_id="extract_lnk_acct_src",
    beeline_conn_id="edlr-conn",
    sql="""
    use {{ var.value.EZ1_SCHEMA }};
    SELECT DISTINCT
        CUST_ID,
        CAST(ACCT_NUM AS BIGINT) AS ACCT_NUM,
        MTH_END_DT
    FROM CUST_ACCT_RLTNP
    WHERE
        SRC_SYS_CD = 'GZ'
        AND PRIMARY_ACCT_HOLDER_F = 'Y'
        AND MTH_END_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="MTH_END_DT") }}';
    """,
    schema=pa.schema([
        ("CUST_ID", pa.string()),
        ("ACCT_NUM", pa.int64()),
        ("MTH_END_DT", pa.date64()),
    ]),
    target="lnk_acct_src.parquet",
    rundir="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq001",
    to_parquet=True,
    tmpfileloc="/bns/rrap/data/tmp",
)
def extract_lnk_acct_src():
    pass


@task.parquet(
    task_id="join_lnk_acct_src",
    duckdb_conn_id="duckdb-conn",
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq001/join_lnk_acct_src.parquet",
    sql="""
    SELECT
        main.*, cust_acct.cust_id
    FROM '{{ task_instance.xcom_pull(task_ids='sq001.sq001_source.join_lnk_prov_src', key='parquet') }}' AS main
    LEFT JOIN '{{ task_instance.xcom_pull(task_ids='sq001.sq001_source.extract_lnk_acct_src', key='parquet') }}' AS cust_acct
        ON main.mort_num = cust_acct.acct_num AND main.mth_end_dt = cust_acct.mth_end_dt
    """,
    export_params={},
    clear_before_write=True,
)
def join_lnk_acct_src():
    pass


@task.beeline(
    task_id="extract_lnk_gz_tgz",
    beeline_conn_id="edlr-conn",
    sql="""
    use {{ var.value.TSZ_SCHEMA }};
    SELECT
        MORT_NUM,
        SUSPENSE_BAL_AMT as TOT_SUSP_BAL,
        businesseffectivedate
    FROM gz_tgz_suspense_bal;
    """,
    schema=pa.schema([
        ("MORT_NUM", pa.int64()),
        ("TOT_SUSP_BAL", pa.float64()),
        ("businesseffectivedate", pa.date64()),
    ]),
    target="lnk_gz_tgz.parquet",
    rundir="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq001",
    to_parquet=True,
    tmpfileloc="/bns/rrap/data/tmp",
)
def extract_lnk_gz_tgz():
    pass


@task.parquet(
    task_id="join_lnk_gz_tgz",
    duckdb_conn_id="duckdb-conn",
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq001/join_lnk_gz_tgz.parquet",
    sql="""
    SELECT
        main.*, gz.tot_susp_bal
    FROM '{{ task_instance.xcom_pull(task_ids='sq001.sq001_source.join_lnk_acct_src', key='parquet') }}' AS main
    LEFT JOIN '{{ task_instance.xcom_pull(task_ids='sq001.sq001_source.extract_lnk_gz_tgz', key='parquet') }}' AS gz
        ON main.mort_num = gz.mort_num AND main.mth_end_dt = gz.businesseffectivedate
    """,
    export_params={},
    clear_before_write=True,
)
def join_lnk_gz_tgz():
    pass


@task.beeline(
    task_id="extract_lnk_acct_rlntp",
    beeline_conn_id="edlr-conn",
    sql="""
    use {{ var.value.EZ1_SCHEMA }};
    SELECT
        CAST(ACCT_NUM AS BIGINT) AS ACCT_NUM,
        SRC_SYS_CD,
        MTH_END_DT
    FROM STEP_ACCT_RLTNP_SNAPSHOT
    WHERE
        ACCT_GRP_ID IS NOT NULL
        AND (GRP_STAT_CD IS NULL OR GRP_STAT_CD = 'F')
        AND MTH_END_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="MTH_END_DT") }}';
    """,
    schema=pa.schema([
        ("ACCT_NUM", pa.int64()),
        ("SRC_SYS_CD", pa.string()),
        ("MTH_END_DT", pa.date64()),
    ]),
    target="lnk_acct_rlntp.parquet",
    rundir="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq001",
    to_parquet=True,
    tmpfileloc="/bns/rrap/data/tmp",
)
def extract_lnk_acct_rlntp():
    pass


@task.parquet(
    task_id="join_lnk_acct_rlntp",
    duckdb_conn_id="duckdb-conn",
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq001/join_lnk_acct_rlntp.parquet",
    sql="""
    SELECT
        main.*, 
        CASE
            WHEN step.acct_num IS NULL THEN 'N'
            ELSE 'Y'
        END AS step_f
    FROM '{{ task_instance.xcom_pull(task_ids='sq001.sq001_source.join_lnk_gz_tgz', key='parquet') }}' AS main
    LEFT JOIN '{{ task_instance.xcom_pull(task_ids='sq001.sq001_source.extract_lnk_acct_rlntp', key='parquet') }}' AS step
        ON main.mort_num = step.acct_num
        AND main.src_sys_cd = step.src_sys_cd
        AND main.mth_end_dt = step.mth_end_dt
    """,
    export_params={},
    clear_before_write=True,
)
def join_lnk_acct_rlntp():
    pass


@task.beeline(
    task_id="extract_v_jw_cbitmstr_branch",
    beeline_conn_id="edlr-conn",
    sql="""
    use {{ var.value.CRZ_AIRB_SCHEMA }};
    SELECT
        CAST(T1.CBIB_TR_NO AS INT) AS CBIB_TR_NO,
        CAST(T1.CBIB_RGN_CDE AS VARCHAR(5)) AS CBIB_RGN_CDE
    FROM V_JW_CBITMSTR_BRANCH T1
    WHERE
        BUSINESSEFFECTIVEDATE IN (
            SELECT MAX(BUSINESSEFFECTIVEDATE)
            FROM V_JW_CBITMSTR_BRANCH
            WHERE MONTH(BUSINESSEFFECTIVEDATE) = MONTH('{{ task_instance.xcom_pull(task_ids="handle_month_context", key="MTH_END_DT") }}')
              AND YEAR(BUSINESSEFFECTIVEDATE) = YEAR('{{ task_instance.xcom_pull(task_ids="handle_month_context", key="MTH_END_DT") }}')
        )
        AND CBIB_INSTN_CDE = '002';
    """,
    schema=pa.schema([
        ("cbib_tr_no", pa.int64()),
        ("cbib_rgn_cde", pa.string()),
    ]),
    target="lnk_v_jw.parquet",
    rundir="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq001",
    to_parquet=True,
    tmpfileloc="/bns/rrap/data/tmp",
)
def extract_v_jw_cbitmstr_branch():
    pass


@task.parquet(
    task_id="join_lnk_v_jw",
    duckdb_conn_id="duckdb-conn",
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq001/join_lnk_v_jw.parquet",
    sql="""
    SELECT
        main.*, CAST(COALESCE(CAST(v.cbib_rgn_cde AS INTEGER), -1) AS VARCHAR(5)) AS tnif_rgn_cd
    FROM '{{ task_instance.xcom_pull(task_ids='sq001.sq001_source.join_lnk_acct_rlntp', key='parquet') }}' AS main
    LEFT JOIN '{{ task_instance.xcom_pull(task_ids='sq001.sq001_source.extract_v_jw_cbitmstr_branch', key='parquet') }}' AS v
        ON main.mort_num = v.cbib_tr_no
    """,
    export_params={},
    clear_before_write=True,
)
def join_lnk_v_jw():
    pass


@task.parquet(
    task_id="transform_to_airb_mort_mth_snapshot",
    duckdb_conn_id="duckdb-conn",
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq001/airb_mort_mth_snapshot_src.parquet",
    sql="""
    SELECT
        '"version_code":"0.0.1","batch_id":"0.0.1"' AS op_field,
        now() AS insrt_process_tmstmp,
        tm_id,
        mort_num,
        state_remain_amort_mm_cnt AS amort,
        coll_coll_val_amt AS lend_value,
        coll_first_lgl_desc AS property_addr_1,
        coll_addr_line_1_txt AS property_addr_2,
        SUBSTR(CONCAT(TRIM(coll_city_nm), ' ', TRIM(coll_prov_state_cd)), 1, 41) AS property_addr_3,
        prov_id AS prop_prov,
        CASE WHEN prpay_ytd_amt IS NULL THEN 0 ELSE prpay_ytd_amt END AS prepay_ytd,
        cond_orig_loan_amt AS auth_amt,
        cond_orig_branch_transit AS cab,
        mth_end_dt AS eff_tmstmp,
        SUBSTR(TRIM(state_distr_1_freq_nm), 1, 1) AS float_ind,
        int_accr_int_amt AS inerest_accr_amt,
        state_orig_disburs_dt AS intr_adj_dt,
        TRIM(CAST(state_loan_auth_dt AS VARCHAR)) AS made_dt,
        state_acct_mtur_dt AS mat_dt,
        CASE WHEN TRIM(acct_stat_cd) = 'CL' THEN 'Y' ELSE 'N' END AS pd_off_f,
        CASE WHEN cust_id IS NULL THEN '-1' ELSE cust_id END AS prim_cust_id,
        step_f,
        state_tot_disburs_amt AS tot_advnc_amt,
        cond_ownshp_branch_transit AS trnst,
        CASE
            WHEN SUBSTRING(TRIM(state_distr_1_freq_nm), 1, 1) = 'M' THEN state_distr_1_next_due_dt
            ELSE NULL
        END AS unpaid_mth_pay_dt,
        CASE
            WHEN SUBSTRING(TRIM(state_distr_1_freq_nm), 1, 1) IN ('W', 'B', 'S') THEN state_distr_1_next_due_dt
            ELSE NULL
        END AS unpaid_wkly_pay_dt,
        TRIM(cond_acct_sub_type_cd) AS class,
        glmap_investor_cd AS fund_cd,
        CASE
            WHEN coll_prpty_type_cd = 165 THEN '66'
            WHEN coll_prpty_type_cd = 211 THEN '31'
            WHEN coll_prpty_type_cd = 900 THEN '09'
            WHEN coll_prpty_type_cd = 101 THEN CASE WHEN coll_prpty_age_val = '0' THEN '05' ELSE '01' END
            WHEN coll_prpty_type_cd = 111 THEN CASE WHEN coll_prpty_age_val = '0' THEN '15' ELSE '11' END
            WHEN coll_prpty_type_cd = 191 THEN CASE WHEN coll_prpty_age_val = '0' THEN '95' ELSE '91' END
            WHEN coll_prpty_type_cd = 221 THEN CASE WHEN coll_prpty_age_val = '0' THEN '25' ELSE '21' END
            WHEN coll_prpty_type_cd IN (160, 162, 163, 164, 167, 168) THEN RIGHT(TRIM(CAST(coll_prpty_type_cd AS VARCHAR)), 2)
            WHEN coll_prpty_type_cd IN (261, 361, 461, 561, 661, 761)
                THEN CASE WHEN coll_prpty_age_val = '0' THEN '65' ELSE RIGHT(TRIM(CAST(coll_prpty_type_cd AS VARCHAR)), 2) END
            WHEN coll_prpty_type_cd = 0 OR coll_prpty_type_cd IS NULL THEN '00'
            ELSE '00'
        END || LPAD(CAST(coll_unit_cnt AS VARCHAR), 3, '0') AS security_type,
        os_bal_coa_amt AS crnt_bal,
        CASE WHEN frcls_forecl_dt IS NOT NULL THEN 'Y' ELSE NULL END AS frclsr_f,
        cls_acct_cls_dt AS pd_off_dt,
        tot_susp_bal,
        tnif_rgn_cd,
        CASE
            WHEN TRIM(cond_prod_grp_cd) = 'COM' OR coll_unit_cnt >= 5 THEN 'Commercial'
            ELSE 'Residential'
        END AS comm_tp,
        CASE
            WHEN TRIM(cond_acct_sub_type_cd) BETWEEN '75' AND '76' THEN 'GUARANTY'
            WHEN TRIM(cond_acct_sub_type_cd) = '72' THEN 'GEM SPEC'
            WHEN TRIM(cond_acct_sub_type_cd) BETWEEN '70' AND '79' AND TRIM(CAST(state_loan_auth_dt AS VARCHAR)) < '1995-09-01' THEN 'MICC'
            WHEN TRIM(cond_acct_sub_type_cd) BETWEEN '70' AND '79' AND TRIM(CAST(state_loan_auth_dt AS VARCHAR)) >= '1995-09-01' AND TRIM(glmap_investor_cd) = '250' THEN 'GEMICO(NO DOWN)'
            WHEN TRIM(cond_acct_sub_type_cd) BETWEEN '70' AND '79' AND TRIM(CAST(state_loan_auth_dt AS VARCHAR)) >= '1995-09-01' THEN 'GEMICO'
            WHEN TRIM(cond_acct_sub_type_cd) BETWEEN '60' AND '69' THEN 'CONV'
            WHEN TRIM(cond_acct_sub_type_cd) BETWEEN '50' AND '59' THEN 'CMHC'
            ELSE 'GEMICO'
        END AS insur_grp,
        CASE
            WHEN glmap_investor_cd BETWEEN '2202' AND '2249'
                OR glmap_investor_cd BETWEEN '6490' AND '6499' THEN 'Y'
            ELSE NULL
        END AS lra_stat,
        gl_acct_num,
        gl_acctng_transit AS gl_trnst_num,
        cond_prod_grp_cd AS prd_grp_cd,
        coll_unit_cnt AS unit_cnt,
        currency_cd AS crncy_cd,
        CAST('{{ task_instance.xcom_pull(task_ids="handle_month_context", key="MTH_END_DT") }}' AS DATE) AS mth_end_dt
    FROM '{{ task_instance.xcom_pull(task_ids='sq001.sq001_source.join_lnk_v_jw', key='parquet') }}'
    """,
    export_params={},
    clear_before_write=True,
)
def transform_to_airb_mort_mth_snapshot():
    pass


@task.parquet(
    task_id="generate_dlqnt_day",
    duckdb_conn_id="duckdb-conn",
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq001/generate_dlqnt_day.parquet",
    sql="""
    SELECT
        src.*,
        CASE
            WHEN pd_off_dt IS NOT NULL THEN 0
            ELSE CASE
                WHEN (mth_end_dt - unpaid_wkly_pay_dt) > 0 THEN (mth_end_dt - unpaid_wkly_pay_dt)
                WHEN (mth_end_dt - unpaid_mth_pay_dt) > 0 THEN (mth_end_dt - unpaid_mth_pay_dt)
                ELSE 0
            END
        END AS dlqnt_day
    FROM '{{ task_instance.xcom_pull(task_ids='sq001.sq001_source.transform_to_airb_mort_mth_snapshot', key='parquet') }}' src
    """,
    export_params={},
    clear_before_write=True,
)
def generate_dlqnt_day():
    pass


@task.parquet(
    task_id="generate_dlqnt_mth",
    duckdb_conn_id="duckdb-conn",
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq001/generate_dlqnt_mth.parquet",
    sql="""
    SELECT
        op_field,
        insrt_process_tmstmp,
        tm_id,
        mort_num,
        amort,
        lend_value,
        property_addr_1,
        property_addr_2,
        property_addr_3,
        prop_prov,
        prepay_ytd,
        auth_amt,
        cab,
        CASE
            WHEN dlqnt_day <= 0 OR dlqnt_day IS NULL THEN 0
            ELSE ROUND(dlqnt_day / 30.0)
        END AS dlqnt_mth,
        eff_tmstmp,
        float_ind,
        inerest_accr_amt,
        intr_adj_dt,
        made_dt,
        mat_dt,
        pd_off_f,
        prim_cust_id,
        step_f,
        tot_advnc_amt,
        trnst,
        unpaid_mth_pay_dt,
        unpaid_wkly_pay_dt,
        class,
        fund_cd,
        security_type,
        crnt_bal,
        frclsr_f,
        pd_off_dt,
        tot_susp_bal,
        tnif_rgn_cd,
        comm_tp,
        insur_grp,
        lra_stat,
        dlqnt_day,
        unit_cnt,
        prd_grp_cd,
        gl_acct_num,
        gl_trnst_num,
        crncy_cd,
        mth_end_dt
    FROM '{{ task_instance.xcom_pull(task_ids='sq001.sq001_source.generate_dlqnt_day', key='parquet') }}' src
    """,
    export_params={},
    clear_before_write=True,
)
def generate_dlqnt_mth():
    pass


@task.parquet(
    task_id="airb_mort_mth_snapshot",
    duckdb_conn_id="duckdb-conn",
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq001/airb_mort_mth_snapshot.parquet",
    sql="""
    SELECT
        '"version_code":"0.0.1","batch_id":"0.0.1"' AS op_code,
        current_timestamp AS insrt_process_tmstmp,
        tm_id,
        mort_num,
        amort,
        lend_value,
        property_addr_1,
        property_addr_2,
        property_addr_3,
        prop_prov,
        prepay_ytd,
        auth_amt,
        cab,
        dlqnt_mth,
        strftime(eff_tmstmp, '%Y-%m-%d') AS eff_tmstmp,
        float_ind,
        inerest_accr_amt,
        strftime(intr_adj_dt, '%Y-%m-%d') AS intr_adj_dt,
        made_dt,
        strftime(mat_dt, '%Y-%m-%d') AS mat_dt,
        pd_off_f,
        prim_cust_id,
        step_f,
        tot_advnc_amt,
        trnst,
        strftime(unpaid_mth_pay_dt, '%Y-%m-%d') AS unpaid_mth_pay_dt,
        strftime(unpaid_wkly_pay_dt, '%Y-%m-%d') AS unpaid_wkly_pay_dt,
        class,
        fund_cd,
        security_type,
        crnt_bal,
        frclsr_f,
        strftime(pd_off_dt, '%Y-%m-%d') AS pd_off_dt,
        tot_susp_bal,
        tnif_rgn_cd,
        comm_tp,
        insur_grp,
        lra_stat,
        dlqnt_day,
        unit_cnt,
        prd_grp_cd,
        gl_acct_num,
        gl_trnst_num,
        crncy_cd,
        mth_end_dt
    FROM '{{ task_instance.xcom_pull(task_ids='sq001.sq001_source.generate_dlqnt_mth', key='parquet') }}'
    """,
    export_params={},
    clear_before_write=True,
)
def airb_mort_mth_snapshot():
    pass


""" TaskFlow function calls """
create_sq001_rundir = create_sq001_rundir()
extract_lnk_rcrr_src = extract_lnk_rcrr_src()
extract_lnk_tm_src = extract_lnk_tm_src()
extract_lnk_prov_src = extract_lnk_prov_src()
join_lnk_tm_src = join_lnk_tm_src()
join_lnk_prov_src = join_lnk_prov_src()
extract_lnk_acct_src = extract_lnk_acct_src()
join_lnk_acct_src = join_lnk_acct_src()
extract_lnk_gz_tgz = extract_lnk_gz_tgz()
join_lnk_gz_tgz = join_lnk_gz_tgz()
extract_lnk_acct_rlntp = extract_lnk_acct_rlntp()
join_lnk_acct_rlntp = join_lnk_acct_rlntp()
extract_v_jw_cbitmstr_branch = extract_v_jw_cbitmstr_branch()
join_lnk_v_jw = join_lnk_v_jw()
transform_to_airb_mort_mth_snapshot = transform_to_airb_mort_mth_snapshot()
generate_dlqnt_day = generate_dlqnt_day()
generate_dlqnt_mth = generate_dlqnt_mth()
airb_mort_mth_snapshot = airb_mort_mth_snapshot()

""" Dependency chaining """
create_sq001_rundir >> [
    extract_lnk_rcrr_src,
    extract_lnk_tm_src,
    extract_lnk_prov_src,
    extract_lnk_acct_src,
    extract_lnk_gz_tgz,
    extract_lnk_acct_rlntp,
    extract_v_jw_cbitmstr_branch,
]
extract_lnk_acct_src >> join_lnk_acct_src
extract_lnk_gz_tgz >> join_lnk_gz_tgz
extract_lnk_acct_rlntp >> join_lnk_acct_rlntp
extract_v_jw_cbitmstr_branch >> join_lnk_v_jw

[
    extract_lnk_rcrr_src,
    extract_lnk_tm_src,
    extract_lnk_prov_src,
] >> join_lnk_tm_src 
join_lnk_tm_src >> join_lnk_prov_src >> join_lnk_acct_src >> join_lnk_gz_tgz
extract_lnk_acct_rlntp >> join_lnk_acct_rlntp >> join_lnk_v_jw
extract_v_jw_cbitmstr_branch >> join_lnk_v_jw >> transform_to_airb_mort_mth_snapshot >> generate_dlqnt_day 
generate_dlqnt_day >> generate_dlqnt_mth >> airb_mort_mth_snapshot
