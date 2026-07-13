import os
import pyarrow as pa
from airflow.sdk import get_current_context, task


@task
def create_sq023_rundir():
    """Create sq023 run directory."""
    context = get_current_context()
    rundir = context["ti"].xcom_pull(task_ids="handle_month_context", key="RUNDIR")
    sq023_rundir = f"{rundir}/sq023"
    os.makedirs(sq023_rundir, exist_ok=True)


@task.beeline(
    task_id="extract_r1",
    beeline_conn_id="edlr-conn",
    sql="""
        SELECT  
            b.mth_end_dt,
            b.mort_num,
            'CA201' || LPAD(b.mort_num, 13, '0') AS unq_acct_id,
            b.cond_acct_sub_type_cd,
            NVL(b.os_bal_coa_amt, 0) os_bal_coa_amt,
            CAST(NVL(b.os_bal_coa_amt, 1599) AS INT) os_bal_coa_amt_temp,
            b.cond_ownshp_branch_transit,
            b.cond_int_div_index_cd,
            b.delq_day_cnt,
            b.state_loan_auth_dt,
            b.state_tot_disburs_amt,
            b.cond_orig_loan_amt,
            b.coll_coll_val_amt,
            b.cond_orig_branch_transit,
            b.glmap_investor_cd,
            b.state_acct_mtur_dt,
            a.ncr_key_val,
            CASE 
                WHEN b.state_acct_mtur_dt IS NULL THEN NULL  
                ELSE ABS(CAST(MONTHS_BETWEEN(b.mth_end_dt, b.state_acct_mtur_dt) AS INT))  
            END AS residual_mat  
        FROM {{ var.value.RCRR_SCHEMA }}.mortgage_mth_snapshot b  
        LEFT OUTER JOIN {{ var.value.CRZ_AIRB_SCHEMA }}.airb_ncr_expsr_size_lkp a  
        WHERE NVL(b.os_bal_coa_amt, 0) >= a.min_bal_amt  
            AND NVL(b.os_bal_coa_amt, 0) < a.max_bal_amt  
            AND a.bus_eff_dt IN (SELECT MAX(bus_eff_dt) FROM {{ var.value.CRZ_AIRB_SCHEMA }}.airb_ncr_expsr_size_lkp)  
            AND a.min_bal_amt IS NOT NULL  
            AND a.max_bal_amt IS NOT NULL  
            AND b.gl_acct_num <> '1571664'  
            AND b.mth_end_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="MTH_END_DT") }}';
    """,
    rundir="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq023",
    to_parquet=True,
    tmpfileloc="/bns/rrap/data/tmp",
    target="extract_r1.parquet",
    schema=pa.schema([
        ("mth_end_dt", pa.date64()),
        ("mort_num", pa.int64()),
        ("unq_acct_id", pa.string()),
        ("cond_acct_sub_type_cd", pa.string()),
        ("os_bal_coa_amt", pa.float64()),
        ("os_bal_coa_amt_temp", pa.int64()),
        ("cond_ownshp_branch_transit", pa.string()),
        ("cond_int_div_index_cd", pa.string()),
        ("delq_day_cnt", pa.int64()),
        ("state_loan_auth_dt", pa.date64()),
        ("state_tot_disburs_amt", pa.float64()),
        ("cond_orig_loan_amt", pa.float64()),
        ("coll_coll_val_amt", pa.float64()),
        ("cond_orig_branch_transit", pa.string()),
        ("glmap_investor_cd", pa.string()),
        ("state_acct_mtur_dt", pa.date64()),
        ("ncr_key_val", pa.string()),
        ("residual_mat", pa.int64()),
    ]),
)
def extract_r1():
    """Extract R1 data from mortgage snapshot with NCR exposure size lookup."""
    pass


@task.beeline(
    task_id="extract_r2",
    beeline_conn_id="edlr-conn",
    sql="""
        SELECT  
            c.ncr_dlqnt_cd,  
            c.dlqnt_cycl_cd,  
            c.min_days_dlqnt,    
            c.max_days_dlqnt    
        FROM {{ var.value.CRZ_AIRB_SCHEMA }}.airb_dlqnt_lkp c  
        WHERE c.bus_eff_dt IN (SELECT MAX(bus_eff_dt) FROM {{ var.value.CRZ_AIRB_SCHEMA }}.airb_dlqnt_lkp)  
            AND c.min_days_dlqnt >= 0  
            AND c.max_days_dlqnt >= 0;
    """,
    rundir="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq023",
    to_parquet=True,
    tmpfileloc="/bns/rrap/data/tmp",
    target="extract_r2.parquet",
    schema=pa.schema([
        ("ncr_dlqnt_cd", pa.string()),
        ("dlqnt_cycl_cd", pa.string()),
        ("min_days_dlqnt", pa.int64()),
        ("max_days_dlqnt", pa.int64()),
    ]),
)
def extract_r2():
    """Extract R2 delinquency lookup data."""
    pass


@task.parquet(
    task_id="make_main_dataset",
    duckdb_conn_id="duckdb-conn",
    export_params={},
    clear_before_write=True,
    sql="""
        SELECT 
            R1.mth_end_dt,
            R1.mort_num,
            R1.unq_acct_id,
            R1.cond_acct_sub_type_cd,
            R1.os_bal_coa_amt,
            R1.os_bal_coa_amt_temp,
            R1.cond_ownshp_branch_transit,
            R1.cond_int_div_index_cd,
            R1.delq_day_cnt,
            R1.state_loan_auth_dt,
            R1.state_tot_disburs_amt,
            R1.cond_orig_loan_amt,
            R1.coll_coll_val_amt,
            R1.cond_orig_branch_transit,
            R1.glmap_investor_cd,
            R1.state_acct_mtur_dt,
            CAST(R1.ncr_key_val AS VARCHAR(4)) ncr_key_val,
            R1.residual_mat,
            CAST(R2.ncr_dlqnt_cd AS VARCHAR(4)) AS ncr_dlqnt_bckt_key_val,
            CAST(R2.dlqnt_cycl_cd AS VARCHAR(1)) AS dlqnt_stg
        FROM '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="RUNDIR") }}/sq023/extract_r1.parquet' AS R1  
        LEFT OUTER JOIN '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="RUNDIR") }}/sq023/extract_r2.parquet' AS R2     
        ON (COALESCE(R1.delq_day_cnt, CAST(0 AS BIGINT)) BETWEEN R2.min_days_dlqnt AND R2.max_days_dlqnt)
    """,
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq023/make_main_dataset.parquet",
)
def make_main_dataset():
    """Join R1 and R2 to create main dataset."""
    pass


@task.parquet(
    task_id="extract_airb_cust_acct_rltnp",
    duckdb_conn_id="duckdb-conn",
    export_params={},
    clear_before_write=True,
    sql="""
        SELECT
            CAST(cust_id AS varchar(10)) AS CUST_ID,
            CAST(acct_num AS bigint) AS ACCT_NUM,
            MTH_END_DT
        FROM
            '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="RUNDIR") }}/sq004/airb_cust_acct_rltnp.parquet'
        WHERE
            MTH_END_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="MTH_END_DT") }}'
            AND primary_acct_holder_f = 'Y'
            AND src_sys_cd = 'GZ'
    """,
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq023/extract_airb_cust_acct_rltnp.parquet",
)
def extract_airb_cust_acct_rltnp():
    """Extract AIRB customer/account relationship from sq004."""
    pass


@task.beeline(
    task_id="extract_airb_ncr_rt_lkp",
    beeline_conn_id="edlr-conn",
    sql="""
        SELECT 
            NCR_RT_KEY_VAL,
            NCR_RT_DESC
        FROM
            {{ var.value.CRZ_AIRB_SCHEMA }}.AIRB_NCR_RT_LKP
        WHERE
            ncr_rt_desc in ('Fixed Rate', 'Variable Rate')
        ;
    """,
    rundir="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq023",
    to_parquet=True,
    tmpfileloc="/bns/rrap/data/tmp",
    target="extract_airb_ncr_rt_lkp.parquet",
    schema=pa.schema([
        ("NCR_RT_KEY_VAL", pa.string()),
        ("NCR_RT_DESC", pa.string()),
    ]),
)
def extract_airb_ncr_rt_lkp():
    """Extract NCR rate type lookup."""
    pass


@task.beeline(
    task_id="extract_lnk_v_jw_src",
    beeline_conn_id="edlr-conn",
    sql="""
        SELECT 						
            CAST(T1.CBIB_TR_NO AS VARCHAR(5)) AS CBIB_TR_NO,						
            CAST(T1.CBIB_RGN_CDE AS VARCHAR(5)) AS CBIB_RGN_CDE, 						
            BUSINESSEFFECTIVEDATE
        FROM
            {{ var.value.CRZ_AIRB_SCHEMA }}.V_JW_CBITMSTR_BRANCH T1						
        WHERE
            CBIB_INSTN_CDE='002'
        ;
    """,
    rundir="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq023",
    to_parquet=True,
    tmpfileloc="/bns/rrap/data/tmp",
    target="extract_lnk_v_jw_src.parquet",
    schema=pa.schema([
        ("CBIB_TR_NO", pa.string()),
        ("CBIB_RGN_CDE", pa.string()),
        ("BUSINESSEFFECTIVEDATE", pa.date64()),
    ]),
)
def extract_lnk_v_jw_src():
    """Extract V_JW_CBITMSTR_BRANCH for branch transit mappings."""
    pass


@task.beeline(
    task_id="extract_max_date",
    beeline_conn_id="edlr-conn",
    sql="""
        SELECT
            MAX(BUSINESSEFFECTIVEDATE) AS BUSINESSEFFECTIVEDATE
        FROM
            {{ var.value.CRZ_AIRB_SCHEMA }}.V_JW_CBITMSTR_BRANCH
        WHERE
            CBIB_TR_NO IN (
                SELECT cond_ownshp_branch_transit
                FROM {{ var.value.RCRR_SCHEMA }}.mortgage_mth_snapshot
            )
        ;
    """,
    rundir="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq023",
    to_parquet=True,
    tmpfileloc="/bns/rrap/data/tmp",
    target="extract_max_date.parquet",
    schema=pa.schema([("BUSINESSEFFECTIVEDATE", pa.date64())]),
)
def extract_max_date():
    """Extract max effective date from branch master."""
    pass


@task.parquet(
    task_id="make_dslink136",
    duckdb_conn_id="duckdb-conn",
    export_params={},
    clear_before_write=True,
    sql="""
        SELECT 
            a.CBIB_TR_NO,
            a.CBIB_RGN_CDE
        FROM '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="RUNDIR") }}/sq023/extract_lnk_v_jw_src.parquet' AS a
        INNER JOIN '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="RUNDIR") }}/sq023/extract_max_date.parquet' AS b
        ON a.BUSINESSEFFECTIVEDATE = b.BUSINESSEFFECTIVEDATE
    """,
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq023/make_dslink136.parquet",
)
def make_dslink136():
    """Create DSLink136 branch mapping."""
    pass


@task.beeline(
    task_id="extract_airb_genworth_bulkins_lkp",
    beeline_conn_id="edlr-conn",
    sql="""
        SELECT
            LENDER_LOAN
        FROM
            {{ var.value.CRZ_AIRB_SCHEMA }}.AIRB_GENWORTH_BULKINS_LKP
        ;
    """,
    rundir="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq023",
    to_parquet=True,
    tmpfileloc="/bns/rrap/data/tmp",
    target="extract_airb_genworth_bulkins_lkp.parquet",
    schema=pa.schema([("LENDER_LOAN", pa.float64())]),
)
def extract_airb_genworth_bulkins_lkp():
    """Extract Genworth bulk insurance lookup."""
    pass


@task.parquet(
    task_id="generate_bulk_f",
    duckdb_conn_id="duckdb-conn",
    export_params={},
    clear_before_write=True,
    sql="""
        SELECT
            m.mort_num,
            CASE
                WHEN TRIM(m.cond_acct_sub_type_cd) IN ('54', '71', '72', '74')
                    AND EXISTS (
                        SELECT 1
                        FROM '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="RUNDIR") }}/sq023/extract_airb_genworth_bulkins_lkp.parquet' AS b
                        WHERE b.lender_loan IS NOT NULL
                        AND b.lender_loan = m.mort_num
                        AND TRIM(CAST(b.lender_loan AS VARCHAR)) <> '0'
                    )
                THEN 'Y'
                ELSE 'N'
            END AS BULK_F
        FROM '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="RUNDIR") }}/sq023/make_main_dataset.parquet' AS m
    """,
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq023/generate_bulk_f.parquet",
)
def generate_bulk_f():
    """Generate bulk insurance flag."""
    pass


@task.parquet(
    task_id="generate_prim_cust_cid",
    duckdb_conn_id="duckdb-conn",
    export_params={},
    clear_before_write=True,
    sql="""
        SELECT
            m.mort_num,
            r.CUST_ID AS PRIM_CUST_CID
        FROM '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="RUNDIR") }}/sq023/make_main_dataset.parquet' AS m
        INNER JOIN '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="RUNDIR") }}/sq023/extract_airb_cust_acct_rltnp.parquet' AS r
        ON m.mort_num = r.ACCT_NUM
        AND m.mth_end_dt = r.mth_end_dt
    """,
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq023/generate_prim_cust_cid.parquet",
)
def generate_prim_cust_cid():
    """Generate primary customer ID."""
    pass


@task.parquet(
    task_id="generate_ncr_rt_key_val",
    duckdb_conn_id="duckdb-conn",
    export_params={},
    clear_before_write=True,
    sql="""
        SELECT
            m.mort_num,
            lkp.NCR_RT_KEY_VAL
        FROM (
            SELECT *,
                CASE
                    WHEN cond_int_div_index_cd IS NULL OR TRIM(cond_int_div_index_cd) = '' THEN '11'
                    ELSE '12'
                END AS transformed_cd
            FROM '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="RUNDIR") }}/sq023/make_main_dataset.parquet'
        ) AS m
        INNER JOIN (
            SELECT *,
                CASE
                    WHEN ncr_rt_desc = 'Fixed Rate' THEN '11'
                    WHEN ncr_rt_desc = 'Variable Rate' THEN '12'
                    ELSE '0'
                END AS transformed_desc
            FROM '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="RUNDIR") }}/sq023/extract_airb_ncr_rt_lkp.parquet'
        ) AS lkp
        ON m.transformed_cd = lkp.transformed_desc
    """,
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq023/generate_ncr_rt_key_val.parquet",
)
def generate_ncr_rt_key_val():
    """Generate NCR rate key value."""
    pass


@task.parquet(
    task_id="generate_rgnl_offc_cd",
    duckdb_conn_id="duckdb-conn",
    export_params={},
    clear_before_write=True,
    sql="""
        SELECT
            m.mort_num,
            d.CBIB_RGN_CDE AS RGNL_OFFC_CD
        FROM '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="RUNDIR") }}/sq023/make_main_dataset.parquet' AS m
        INNER JOIN '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="RUNDIR") }}/sq023/make_dslink136.parquet' AS d
        ON m.cond_ownshp_branch_transit = d.CBIB_TR_NO
    """,
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq023/generate_rgnl_offc_cd.parquet",
)
def generate_rgnl_offc_cd():
    """Generate regional office code."""
    pass


@task.parquet(
    task_id="join_main_with_lkp02_fields",
    duckdb_conn_id="duckdb-conn",
    export_params={},
    clear_before_write=True,
    sql="""
        SELECT
            m.*,
            b.BULK_F,
            c.PRIM_CUST_CID,
            d.NCR_RT_KEY_VAL,
            e.RGNL_OFFC_CD
        FROM '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="RUNDIR") }}/sq023/make_main_dataset.parquet' AS m
        LEFT JOIN '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="RUNDIR") }}/sq023/generate_bulk_f.parquet' AS b
            ON m.mort_num = b.mort_num
        LEFT JOIN '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="RUNDIR") }}/sq023/generate_prim_cust_cid.parquet' AS c
            ON m.mort_num = c.mort_num
        LEFT JOIN '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="RUNDIR") }}/sq023/generate_ncr_rt_key_val.parquet' AS d
            ON m.mort_num = d.mort_num
        LEFT JOIN '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="RUNDIR") }}/sq023/generate_rgnl_offc_cd.parquet' AS e
            ON m.mort_num = e.mort_num
    """,
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq023/join_main_with_lkp02_fields.parquet",
)
def join_main_with_lkp02_fields():
    """Join main dataset with lookup-derived fields."""
    pass


@task.beeline(
    task_id="extract_temp_lkp_ncr_geo",
    beeline_conn_id="edlr-conn",
    sql="""
        SELECT 							
            cast(a.cbib_tr_no as Varchar(5)) cbib_tr_no,					
            c.ncr_geo_key_val ncr_geo_key_val
        FROM
            {{ var.value.CRZ_AIRB_SCHEMA }}.V_JW_CBITMSTR_BRANCH a
        LEFT OUTER JOIN {{ var.value.TSZ_RMA_SCHEMA }}.airb_prov_ref_lkp b					
                    ON a.cbib_province_cde = b.prov_id					
        LEFT OUTER JOIN {{ var.value.TSZ_RMA_SCHEMA }}.airb_ncr_geo_lkp c							
                    ON ltrim(rtrim(b.prov_nm)) = ltrim(rtrim(c.ncr_geo_desc))					
        WHERE cbib_instn_cde = '002' and a.businesseffectivedate in							
            (SELECT MAX(businesseffectivedate) FROM {{ var.value.CRZ_AIRB_SCHEMA }}.V_JW_CBITMSTR_BRANCH)
        ;
    """,
    rundir="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq023",
    to_parquet=True,
    tmpfileloc="/bns/rrap/data/tmp",
    target="extract_temp_lkp_ncr_geo.parquet",
    schema=pa.schema([
        ("cbib_tr_no", pa.string()),
        ("ncr_geo_key_val", pa.string())
    ]),
)
def extract_temp_lkp_ncr_geo():
    """Extract geographic lookup for NCR."""
    pass


@task.parquet(
    task_id="join_main_with_ncr_expsr_geo_key_val",
    duckdb_conn_id="duckdb-conn",
    export_params={},
    clear_before_write=True,
    sql="""
        SELECT
            a.*,
            b.ncr_geo_key_val AS NCR_EXPSR_GEO_KEY_VAL
        FROM '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="RUNDIR") }}/sq023/join_main_with_lkp02_fields.parquet' AS a
        LEFT JOIN '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="RUNDIR") }}/sq023/extract_temp_lkp_ncr_geo.parquet' AS b
        ON a.cond_ownshp_branch_transit = b.cbib_tr_no
    """,
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq023/join_main_with_ncr_expsr_geo_key_val.parquet",
)
def join_main_with_ncr_expsr_geo_key_val():
    """Join main with geographic exposure key."""
    pass


@task.parquet(
    task_id="join_main_with_insur_grp",
    duckdb_conn_id="duckdb-conn",
    export_params={},
    clear_before_write=True,
    sql="""
        SELECT
            a.*,
            CASE
                WHEN CAST(a.COND_ACCT_SUB_TYPE_CD AS INTEGER) BETWEEN 75 AND 76 THEN 'GUARANTY'
                WHEN CAST(a.COND_ACCT_SUB_TYPE_CD AS INTEGER) = 72 THEN 'GEM SPEC'
                WHEN CAST(a.COND_ACCT_SUB_TYPE_CD AS INTEGER) BETWEEN 70 AND 79
                    AND a.STATE_LOAN_AUTH_DT < DATE '1995-09-01' THEN 'MICC'
                WHEN CAST(a.COND_ACCT_SUB_TYPE_CD AS INTEGER) BETWEEN 70 AND 79
                    AND a.STATE_LOAN_AUTH_DT >= DATE '1995-09-01'
                    AND CAST(a.GLMAP_INVESTOR_CD AS INTEGER) = 250 THEN 'GEMICO(NO DOWN)'
                WHEN CAST(a.COND_ACCT_SUB_TYPE_CD AS INTEGER) BETWEEN 70 AND 79
                    AND a.STATE_LOAN_AUTH_DT >= DATE '1995-09-01' THEN 'GEMICO'
                WHEN CAST(a.COND_ACCT_SUB_TYPE_CD AS INTEGER) BETWEEN 60 AND 69 THEN 'CONV'
                WHEN CAST(a.COND_ACCT_SUB_TYPE_CD AS INTEGER) BETWEEN 50 AND 59 THEN 'CMHC'
                ELSE 'GEMICO'
            END AS INSUR_GRP
        FROM
            '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="RUNDIR") }}/sq023/join_main_with_ncr_expsr_geo_key_val.parquet' AS a
    """,
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq023/join_main_with_insur_grp.parquet",
)
def join_main_with_insur_grp():
    """Join main with insurance group classification."""
    pass


@task.beeline(
    task_id="extract_mort_rptg_prd_lkp",
    beeline_conn_id="edlr-conn",
    sql="""
        SELECT
            a.PRD_ID,
            a.BASEL_MORT_INSURER_GRP_DESC,
            a.BULK_F
        FROM {{ var.value.TSZ_RMA_SCHEMA }}.AIRB_MORT_RPTG_PRD_LKP a
        WHERE a.src_sys_cd = 'MOR'
        ;
    """,
    rundir="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq023",
    to_parquet=True,
    tmpfileloc="/bns/rrap/data/tmp",
    target="extract_mort_rptg_prd_lkp.parquet",
    schema=pa.schema([
        ("PRD_ID", pa.string()),
        ("BASEL_MORT_INSURER_GRP_DESC", pa.string()),
        ("BULK_F", pa.string()),
    ]),
)
def extract_mort_rptg_prd_lkp():
    """Extract mortgage reporting product lookup."""
    pass


@task.parquet(
    task_id="join_main_with_prd_id",
    duckdb_conn_id="duckdb-conn",
    export_params={},
    clear_before_write=True,
    sql="""
        SELECT
            a.*,
            b.PRD_ID
        FROM '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="RUNDIR") }}/sq023/join_main_with_insur_grp.parquet' AS a
        INNER JOIN '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="RUNDIR") }}/sq023/extract_mort_rptg_prd_lkp.parquet' AS b
            ON a.INSUR_GRP = b.BASEL_MORT_INSURER_GRP_DESC
            AND a.BULK_F = b.BULK_F
    """,
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq023/join_main_with_prd_id.parquet",
)
def join_main_with_prd_id():
    """Join main with product ID."""
    pass


@task.beeline(
    task_id="generate_legal_entity_lkp",
    beeline_conn_id="edlr-conn",
    sql="""
        SELECT DISTINCT
            a.glmap_investor_cd,
            b.legal_entity_nm
        FROM {{ var.value.RCRR_SCHEMA }}.mortgage_mth_snapshot AS a
        INNER JOIN {{ var.value.TSZ_CMF_SCHEMA }}.AIRB_LGL_ENT_FUND_CD_MAP_LKP AS b
            ON CAST(a.glmap_investor_cd AS INT) BETWEEN b.fund_cd_st AND b.fund_cd_end
        ;
    """,
    rundir="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq023",
    to_parquet=True,
    tmpfileloc="/bns/rrap/data/tmp",
    target="generate_legal_entity_lkp.parquet",
    schema=pa.schema([
        ("glmap_investor_cd", pa.string()),
        ("legal_entity_nm", pa.string())
    ]),
)
def generate_legal_entity_lkp():
    """Generate legal entity lookup."""
    pass


@task.parquet(
    task_id="join_main_with_legal_entity",
    duckdb_conn_id="duckdb-conn",
    export_params={},
    clear_before_write=True,
    sql="""
        SELECT
            a.*,
            CASE
                WHEN b.legal_entity_nm IS NULL OR TRIM(b.legal_entity_nm) = ''
                    THEN 'DOM-SUB-BNS'
                ELSE b.legal_entity_nm
            END AS LEGAL_ENTITY
        FROM '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="RUNDIR") }}/sq023/join_main_with_prd_id.parquet' AS a
        LEFT JOIN '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="RUNDIR") }}/sq023/generate_legal_entity_lkp.parquet' AS b
            ON a.glmap_investor_cd = b.glmap_investor_cd
    """,
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq023/join_main_with_legal_entity.parquet",
)
def join_main_with_legal_entity():
    """Join main with legal entity."""
    pass


@task.beeline(
    task_id="generate_ccar_expsr_lkp",
    beeline_conn_id="edlr-conn",
    sql="""
        SELECT DISTINCT
            CAST(c.ccar_expsr_clss_nm AS VARCHAR(20)) AS ccar_expsr_cl_nm,
            CAST(b.prd_id AS VARCHAR(10)) AS prd_id
        FROM {{ var.value.CRZ_AIRB_SCHEMA }}.airb_mort_rptg_prd_lkp b
        INNER JOIN {{ var.value.CRZ_AIRB_SCHEMA }}.airb_expsr_clss_lkp c
            ON b.ncr_expsr_cl_key_val = c.ncr_expsr_clss_key_val
    """,
    rundir="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq023",
    to_parquet=True,
    tmpfileloc="/bns/rrap/data/tmp",
    target="generate_ccar_expsr_lkp.parquet",
    schema=pa.schema([
        ("ccar_expsr_cl_nm", pa.string()),
        ("prd_id", pa.string())
    ]),
)
def generate_ccar_expsr_lkp():
    """Generate CCAR exposure class lookup."""
    pass


@task.parquet(
    task_id="join_ccar_expsr_clss_nm",
    duckdb_conn_id="duckdb-conn",
    export_params={},
    clear_before_write=True,
    sql="""
        SELECT
            a.*,
            b.ccar_expsr_cl_nm AS CCAR_EXPSR_CLSS_NM
        FROM '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="RUNDIR") }}/sq023/join_main_with_legal_entity.parquet' AS a
        INNER JOIN '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="RUNDIR") }}/sq023/generate_ccar_expsr_lkp.parquet' AS b
            ON a.prd_id = b.prd_id
    """,
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq023/join_ccar_expsr_clss_nm.parquet",
)
def join_ccar_expsr_clss_nm():
    """Join CCAR exposure class."""
    pass


@task.beeline(
    task_id="extract_ncr_expsr_size_lkp",
    beeline_conn_id="edlr-conn",
    sql="""
        SELECT
            ncr_key_val,
            min_bal_amt,
            max_bal_amt
        FROM {{ var.value.CRZ_AIRB_SCHEMA }}.AIRB_NCR_EXPSR_SIZE_LKP
        WHERE min_bal_amt IS NOT NULL AND max_bal_amt IS NOT NULL
    """,
    rundir="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq023",
    to_parquet=True,
    tmpfileloc="/bns/rrap/data/tmp",
    target="extract_ncr_expsr_size_lkp.parquet",
    schema=pa.schema([
        ("ncr_key_val", pa.string()),
        ("min_bal_amt", pa.float64()),
        ("max_bal_amt", pa.float64())
    ]),
)
def extract_ncr_expsr_size_lkp():
    """Extract NCR exposure size lookup."""
    pass


@task.parquet(
    task_id="join_ncr_expsr_size_key_val",
    duckdb_conn_id="duckdb-conn",
    export_params={},
    clear_before_write=True,
    sql="""
        SELECT 
            a.*,
            CASE 
                WHEN a.os_bal_coa_amt_temp = 1599 THEN '1599'
                ELSE b.ncr_key_val
            END AS ncr_expsr_size_key_val
        FROM '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="RUNDIR") }}/sq023/join_ccar_expsr_clss_nm.parquet' AS a
        LEFT JOIN 
            '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="RUNDIR") }}/sq023/extract_ncr_expsr_size_lkp.parquet' AS b
        ON 
            b.min_bal_amt <= COALESCE(a.os_bal_coa_amt_temp, 0) 
            AND COALESCE(a.os_bal_coa_amt_temp, 0) < b.max_bal_amt
            AND b.min_bal_amt IS NOT NULL
            AND b.max_bal_amt IS NOT NULL
    """,
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq023/join_ncr_expsr_size_key_val.parquet",
)
def join_ncr_expsr_size_key_val():
    """Join NCR exposure size key value."""
    pass


@task.parquet(
    task_id="rename_selected_columns",
    duckdb_conn_id="duckdb-conn",
    export_params={},
    clear_before_write=True,
    sql="""
        SELECT
            *,
            STATE_LOAN_AUTH_DT AS ACCT_OPN_DT,
            COND_OWNSHP_BRANCH_TRANSIT AS TRNST_NUM,
            STATE_TOT_DISBURS_AMT AS ADVNC_AMT,
            COND_ORIG_LOAN_AMT AS AUTH_AMT,
            CASE
                WHEN DELQ_DAY_CNT IS NULL THEN 0
                ELSE DELQ_DAY_CNT
            END AS DLQNT_DAYS_CNT,
            CASE
                WHEN OS_BAL_COA_AMT IS NULL THEN 0
                ELSE OS_BAL_COA_AMT
            END AS BEFR_ZERO_NET_DRAWN_AMT,
            COLL_COLL_VAL_AMT AS ORIG_PRPTY_VAL_AMT,
            COND_ORIG_BRANCH_TRANSIT AS CAB_TRNST_NUM,
            STATE_ACCT_MTUR_DT AS MAT_DT,
            OS_BAL_COA_AMT AS AF_ZERO_NET_DRAWN_AMT
        FROM '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="RUNDIR") }}/sq023/join_ncr_expsr_size_key_val.parquet'
    """,
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq023/rename_selected_columns.parquet",
)
def rename_selected_columns():
    """Rename selected columns and create derived fields."""
    pass


@task.parquet(
    task_id="add_extra_fields",
    duckdb_conn_id="duckdb-conn",
    export_params={},
    clear_before_write=True,
    sql="""
        SELECT
            *,
            '"version_code":"0.0.1","batch_id":"0.0.1"' AS OP_FIELD,
            now() AS INSRT_PROCESS_TMSTMP,
            0 AS GL_BAL_ADJUSTING_AMT,
            0 AS BEFR_ZERO_NET_UNDRAWN_AMT,
            'A' AS CONSM_PRD_TREATMNT_CD,
            NULL AS BASEL_CIF_KEY,
            NULL AS SCOTIA_TOT_EQTY_PLN_F,
            'N' AS TRNST_EXCLSN_F,
            0 AS AF_ZERO_NET_UNDRAWN_AMT
        FROM '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="RUNDIR") }}/sq023/rename_selected_columns.parquet'
    """,
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq023/add_extra_fields.parquet",
)
def add_extra_fields():
    """Add extra fields with default values."""
    pass


@task.parquet(
    task_id="make_airb_baselayer_mort",
    duckdb_conn_id="duckdb-conn",
    export_params={},
    clear_before_write=True,
    sql="""
        SELECT
            OP_FIELD,
            INSRT_PROCESS_TMSTMP,
            MORT_NUM,
            unq_acct_id,
            INSUR_GRP,
            BULK_F,
            PRD_ID,
            ncr_expsr_size_key_val AS NCR_EXPSR_SIZE_KEY_VAL,
            NCR_EXPSR_GEO_KEY_VAL,
            NCR_RT_KEY_VAL,
            ncr_dlqnt_bckt_key_val,
            ACCT_OPN_DT,
            TRNST_NUM,
            ADVNC_AMT,
            AUTH_AMT,
            GL_BAL_ADJUSTING_AMT,
            DLQNT_DAYS_CNT,
            BEFR_ZERO_NET_DRAWN_AMT,
            BEFR_ZERO_NET_UNDRAWN_AMT,
            ORIG_PRPTY_VAL_AMT,
            CCAR_EXPSR_CLSS_NM,
            CONSM_PRD_TREATMNT_CD,
            PRIM_CUST_CID,
            RGNL_OFFC_CD,
            dlqnt_stg,
            BASEL_CIF_KEY,
            CAB_TRNST_NUM,
            LEGAL_ENTITY,
            MAT_DT,
            SCOTIA_TOT_EQTY_PLN_F,
            RESIDUAL_MAT,
            TRNST_EXCLSN_F,
            AF_ZERO_NET_DRAWN_AMT,
            AF_ZERO_NET_UNDRAWN_AMT,
            MTH_END_DT
        FROM '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="RUNDIR") }}/sq023/add_extra_fields.parquet'
    """,
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq023/make_airb_baselayer_mort.parquet",
)
def make_airb_baselayer_mort():
    """Generate final AIRB Basel Layer Mortgage dataset."""
    pass


"""Source layer for sq023."""
rundir_task = create_sq023_rundir()

# Parallel extracts
r1 = extract_r1()
r2 = extract_r2()
airb_cust_acct = extract_airb_cust_acct_rltnp()
ncr_rt = extract_airb_ncr_rt_lkp()
lnk_v_jw = extract_lnk_v_jw_src()
max_date = extract_max_date()
genworth = extract_airb_genworth_bulkins_lkp()
geo_lkp = extract_temp_lkp_ncr_geo()
mort_rptg_prd = extract_mort_rptg_prd_lkp()
legal_entity = generate_legal_entity_lkp()
ccar_lkp = generate_ccar_expsr_lkp()
size_lkp = extract_ncr_expsr_size_lkp()

# Join R1 and R2
main_ds = make_main_dataset()

# Parallel transformations
bulk_f = generate_bulk_f()
prim_cust = generate_prim_cust_cid()
rt_key = generate_ncr_rt_key_val()
rgnl = generate_rgnl_offc_cd()
dslink = make_dslink136()

# Sequential joins
lkp02 = join_main_with_lkp02_fields()
geo = join_main_with_ncr_expsr_geo_key_val()
insur = join_main_with_insur_grp()
prd = join_main_with_prd_id()
entity = join_main_with_legal_entity()
ccar = join_ccar_expsr_clss_nm()
size = join_ncr_expsr_size_key_val()
rename = rename_selected_columns()
extra = add_extra_fields()
final = make_airb_baselayer_mort()

# Dependency chain
rundir_task >> [r1, r2, airb_cust_acct, ncr_rt, lnk_v_jw, max_date, genworth, geo_lkp, mort_rptg_prd, legal_entity, ccar_lkp, size_lkp]

[r1, r2] >> main_ds
[main_ds, genworth] >> bulk_f
[main_ds, airb_cust_acct] >> prim_cust
[main_ds, ncr_rt] >> rt_key
[main_ds, dslink] >> rgnl
[lnk_v_jw, max_date] >> dslink

[main_ds, bulk_f, prim_cust, rt_key, rgnl] >> lkp02
[lkp02, geo_lkp] >> geo >> insur
[insur, mort_rptg_prd] >> prd
[prd, legal_entity] >> entity
[entity, ccar_lkp] >> ccar
[ccar, size_lkp] >> size >> rename >> extra >> final
