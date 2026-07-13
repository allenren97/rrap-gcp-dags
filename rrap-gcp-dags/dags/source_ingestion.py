from airflow.exceptions import AirflowException
from airflow.sdk import Asset, AssetAlias, dag, get_current_context, task, task_group
from bns.rrap.helpers.dependency_utilities import _auto_wire_dependencies
from bns.rrap.hooks.duckdb import DuckLakeHook
from datetime import datetime
import os
import pyarrow as pa
import unicodedata
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pendulum
import logging


logger = logging.getLogger(__name__)

@dag(
    dag_id="source_ingestion",
    schedule="@monthly",
    start_date=pendulum.datetime(2024, 1, 1, tz="America/Toronto"),
    catchup=False,
    tags=["sequence", "source", "ingestion"],
    params={}
)
def source_ingestion():
    """
    Source ingestion DAG template.
    Each sequence previously migrated as part of TSYS to be re-written and converted
    to using a generic SQL adapter for IIAS replacement. The adapter can point to either
    MSSQL or DuckLake as the tables for enriching source data being ingested.
    """

    @task()
    def handle_month_context():
        """
        Task to create XComs (mth_tm_id, rundate, etc.) for the DAG run.
        """
        context = get_current_context()
        rundate = context['logical_date'].subtract(months=1).end_of('month').\
                    strftime('%Y-%m-%d')
        
        hook = DuckLakeHook(duckdb_conn_id='duckdb-conn')
        mth_tm_id = hook.duckdb.sql(f"""
            SELECT TM_ID FROM ingestion.TM_DIM 
            WHERE TM_LVL = 'Month' AND TM_LVL_END_DT = '{rundate}'
        """).fetchone()[0]

        logger.warning(f"Rundate: {rundate}, MTH_TM_ID: {mth_tm_id}")

        prev_mth_tm_id = mth_tm_id - 40
        popn_dt = context['logical_date'].strftime('%Y-%m-15')
        rundir = f"/bns/rrap/data/source_ingestion/{rundate}"
        os.makedirs(rundir, exist_ok=True)

        context['ti'].xcom_push(key='MTH_TM_ID', value=mth_tm_id)
        context['ti'].xcom_push(key='PREV_MTH_TM_ID', value=prev_mth_tm_id)
        context['ti'].xcom_push(key='RUNDATE', value=rundate)
        context['ti'].xcom_push(key='POPN_DT', value=popn_dt)
        context['ti'].xcom_push(key='RUNDIR', value=rundir)
        context['ti'].xcom_push(key='MTH_END_DT', value=rundate)


    handle_month_context = handle_month_context()

    """
    Import TaskGroups for each sequence. Wire dependencies between each sequence based 
    on the order of execution and create manual fail tasks before each sequence.
    """
    @task
    def sq001_start():
        """ Manual approval task to start sq001 """
        raise AirflowException("Please mark this task successful to start sequence sq001.")


    @task_group(group_id="sq001")
    def sq001_group():
        """
        TaskGroup for sequence sq001.
        """

        @task_group(group_id="sq001_source")
        def sq001_source_group():
            """
            TaskGroup for source tasks from EDL in sequence sq001.
            """
            # Import of source_group.py
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
        @task_group(group_id="sq001_enrichment")
        def sq001_enrichment_group():
            """
            TaskGroup for enrichment tasks in sequence sq001.
            """
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
        sq001_source_group = sq001_source_group()
        sq001_enrichment_group = sq001_enrichment_group()

        sq001_source_group >> sq001_enrichment_group


    @task(outlets=[AssetAlias("airb_mort_mth_snapshot")])
    def airb_mort_mth_snapshot(*, outlet_events):
        outlet_events[AssetAlias("airb_mort_mth_snapshot")].add(
            Asset("ingestion.AIRB_MORT_MTH_SNAPSHOT", extra={})
        )


    sq001 = sq001_group()
    sq001_start = sq001_start()
    airb_mort_mth_snapshot = airb_mort_mth_snapshot()

    sq001_start >> sq001 >> airb_mort_mth_snapshot
    @task
    def sq002_start():
        """ Manual approval task to start sq002 """
        raise AirflowException("Please mark this task successful to start sequence sq002.")


    @task_group(group_id="sq002")
    def sq002_group():
        """
        TaskGroup for sequence sq002.
        """

        @task_group(group_id="sq002_source")
        def sq002_source_group():
            """
            TaskGroup for source tasks from EDL in sequence sq002.
            """
            # Import of source_group.py
            @task
            def create_sq002_rundir():
                """
                Task to create RUNDIR for sequence sq002.
                RUNDIR is the directory where extracted data for the sequence is stored.
                """
                context = get_current_context()
                rundir = context["ti"].xcom_pull(task_ids="handle_month_context", key="RUNDIR")
                sq002_rundir = f"{rundir}/sq002"
                os.makedirs(sq002_rundir, exist_ok=True)


            @task.beeline(
                task_id="make_airb_cust_dim",
                beeline_conn_id="edlr-conn",
                sql="""
                use {{ var.value.EZ1_SCHEMA }};
                SELECT
                    trim(cust_id) as cust_id,
                    trim(cust_type_cd) as cust_tp_cd,
                    mth_end_dt
                FROM CUST_INV_PRTY_NON_PII
                WHERE
                    mth_end_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="MTH_END_DT") }}'
                    AND CUST_SRC_SYS_CD = 'CI';
                """,
                schema=pa.schema([
                    ("cust_id", pa.string()),
                    ("cust_tp_cd", pa.string()),
                    ("mth_end_dt", pa.date64()),
                ]),
                target="airb_cust_dim_src.parquet",
                rundir="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq002",
                to_parquet=True,
                tmpfileloc="/bns/rrap/data/tmp",
            )
            def make_airb_cust_dim():
                """
                Extract customer id and type code from EZ1.CUST_INV_PRTY_NON_PII for current month-end.
                """
                pass


            @task.parquet(
                task_id="xfm",
                duckdb_conn_id="duckdb-conn",
                target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq002/airb_cust_dim.parquet",
                sql="""
                SELECT
                    now() as insrt_process_tmstmp,
                    'version_code: abc, batch_id: def' as op_field,
                    cust_id,
                    CASE
                        WHEN cust_tp_cd IN ('COMAB','COMBS','COMFN','COMFR','COMRE', 'CORNR','CORRE','NPERS','SMBAB','SMBAS','SMBFN', 'SMBUS','XXXXX') THEN 'NON_PSNL'
                        WHEN cust_tp_cd IN ('PB','RN','RO','RM','','RX') THEN 'PSNL'
                        ELSE 'UNKNOWN'
                    END AS cust_tp_cd,
                    mth_end_dt
                FROM '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq002/airb_cust_dim_src.parquet'
                """,
                export_params={},
                clear_before_write=True,
            )
            def xfm():
                """
                Map customer type code to PSNL or NON_PSNL categories.
                """
                pass


            create_sq002_rundir = create_sq002_rundir()
            make_airb_cust_dim = make_airb_cust_dim()
            xfm = xfm()

            create_sq002_rundir >> make_airb_cust_dim >> xfm
        @task_group(group_id="sq002_enrichment")
        def sq002_enrichment_group():
            """
            TaskGroup for enrichment tasks in sequence sq002.
            """
            @task
            def get_max_basel_cust_id():
                """
                Task to get the current max BASEL_CUST_ID in BASEL_CUST_DIM, which will be used as the starting point for generating 
                new BASEL_CUST_ID values for net new accounts in the enrichment tasks.
                """
                hook = DuckLakeHook(duckdb_conn_id="duckdb-conn")

                sql = """
                    SELECT MAX(BASEL_CUST_ID) as max_id
                    FROM ingestion.BASEL_CUST_DIM
                """

                result = hook.duckdb.sql(sql)

                return result.to_df()["max_id"][0]


            @task.parquet(
                task_id="join_1",
                duckdb_conn_id="duckdb-conn",
                target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq002/join_1.parquet",
                sql="""
                SELECT
                    A.CUST_ID,
                    A.CUST_TP_CD,
                    B.BASEL_CUST_ID,
                    B.CIF_KEY,
                    B.CUST_CID,
                    B.CUST_TP_CD_NZ,
                    B.IP_ID,
                    B.CIS_PURGED_F,
                    B.CIS_PURGED_DT,
                    B.INSRT_PROCESS_TMSTMP,
                    B.UPDT_PROCESS_TMSTMP
                FROM '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq002/airb_cust_dim.parquet' as A
                LEFT OUTER JOIN ingestion.BASEL_CUST_DIM as B
                ON A.CUST_ID = B.CUST_CID
                """,
                export_params={},
                clear_before_write=True,
            )
            def join_1():
                """
                This task performs a left outer join between the AIRB_CUST_DIM dataset generated from source tasks and the existing BASEL_CUST_DIM table in PROD/IIAS, 
                to identify which accounts are net new (no match in BASEL_CUST_DIM) vs. which accounts are existing but may have changes (matched on CUST_ID but different CUST_TP_CD).
                The result is written to 'join_1.parquet' and will be used as the basis for subsequent enrichment tasks.
                """
                pass


            @task.parquet(
                task_id="xfm_01_new",
                duckdb_conn_id="duckdb-conn",
                target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq002/xfm_01_new.parquet",
                sql="""
                SELECT                 
                    {{ task_instance.xcom_pull(task_ids='sq002.sq002_enrichment.get_max_basel_cust_id') }} + ROW_NUMBER() over (order by CUST_ID, CUST_TP_CD) as BASEL_CUST_ID, -- is this the right way to add new acct ids?
                    NULL AS CIF_KEY,
                    CUST_ID AS CUST_CID,                           -- direct move from AIRB_CUST_DIM
                    CUST_TP_CD,        -- direct move from AIRB_CUST_DIM
                    NULL AS IP_ID,
                    'N' AS CIS_PURGED_F,
                    NULL AS CIS_PURGED_DT,
                    now() as INSRT_PROCESS_TMSTMP,
                    now() as UPDT_PROCESS_TMSTMP
                FROM '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq002/join_1.parquet'
                WHERE BASEL_CUST_ID IS NULL
                """,
                export_params={},
                clear_before_write=True,
            )
            def xfm_01_new():
                """
                This task pulls 'join_1.parquet' contents to generate new BASEL_CUST_ID values (just an index) where BASEL_CUST_ID is null 
                (i.e. for accounts that failed the 'join_1' join - net new accounts) and writes to 'xfm_01_new.parquet'.

                if IsNull(LNK_JOIN_1.BASEL_CUST_ID)  then generate dataset for new record insertion.
                """
                pass


            @task.parquet(
                task_id="xfm_01_update",
                duckdb_conn_id="duckdb-conn",
                target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq002/xfm_01_update.parquet",
                sql="""
                SELECT                 
                    BASEL_CUST_ID,
                    CIF_KEY,                            -- same
                    CUST_CID,                           -- same
                    CUST_TP_CD,        -- updated with AIRB_CUST_DIM
                    IP_ID,                              -- same
                    CIS_PURGED_F,                       -- same
                    CIS_PURGED_DT,                      -- same
                    INSRT_PROCESS_TMSTMP,               -- same
                    now() as UPDT_PROCESS_TMSTMP        -- updated 
                FROM '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq002/join_1.parquet'
                WHERE BASEL_CUST_ID IS NOT NULL AND -- UPDATING EXISTING NEW CUST_TP_CD
                    CUST_TP_CD != CUST_TP_CD_NZ
                """,
                export_params={},
                clear_before_write=True,
            )
            def xfm_01_update():
                """
                This task pulls 'join_1.parquet' contents and filters for records where 'BASEL_CUST_ID' is not null and customer type code doesn't match 
                existing PROD/IIAS data, and then writes to 'xfm_01_update.parquet'.

                if IsNotNull(LNK_JOIN_1.BASEL_CUST_ID) and (LNK_JOIN_1.CUST_TP_CD <> LNK_JOIN_1.CUST_TP_CD_NZ) then generate dataset for existing record update.
                """
                pass


            @task.duckdb(
                task_id="delete_old_basel_cust_dim_records",
                duckdb_conn_id="duckdb-conn",
                sql="""
                    DELETE FROM ingestion.BASEL_CUST_DIM
                    WHERE BASEL_CUST_ID IN (SELECT BASEL_CUST_ID 
                    FROM '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq002/xfm_01_update.parquet')
                """,
            )
            def delete_old_basel_cust_dim_records():
                """
                Task to delete old records from BASEL_CUST_DIM that are being updated, to prevent duplicates when we re-insert updated records in the next step.
                """
                pass


            @task.duckdb(
                task_id="insert_updated_basel_cust_dim_records",
                duckdb_conn_id="duckdb-conn",
                sql="""
                    INSERT INTO ingestion.BASEL_CUST_DIM
                    SELECT * FROM '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq002/xfm_01_update.parquet'
                """,
            )
            def insert_updated_basel_cust_dim_records():
                """
                Task to insert updated records into BASEL_CUST_DIM for accounts that had changes (e.g. customer type code changes) but are not net new accounts.
                """
                pass


            @task.duckdb(
                task_id="insert_new_basel_cust_dim_records",
                duckdb_conn_id="duckdb-conn",
                sql="""
                    INSERT INTO ingestion.BASEL_CUST_DIM
                    SELECT * FROM '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq002/xfm_01_new.parquet'
                """,
            )
            def insert_new_basel_cust_dim_records():
                """
                Task to insert new records into BASEL_CUST_DIM for net new accounts.
                """
                pass


            """ TaskFlow function calls """
            get_max_basel_cust_id = get_max_basel_cust_id()
            join_1 = join_1()
            xfm_01_new = xfm_01_new()
            xfm_01_update = xfm_01_update()
            delete_old_basel_cust_dim_records = delete_old_basel_cust_dim_records()
            insert_updated_basel_cust_dim_records = insert_updated_basel_cust_dim_records()
            insert_new_basel_cust_dim_records = insert_new_basel_cust_dim_records()

            """ Dependency chaining"""
            get_max_basel_cust_id >> join_1
            join_1 >> [
                xfm_01_new, 
                xfm_01_update
            ]
            xfm_01_update >> delete_old_basel_cust_dim_records >> insert_updated_basel_cust_dim_records
            insert_updated_basel_cust_dim_records >> insert_new_basel_cust_dim_records
            xfm_01_new >> insert_new_basel_cust_dim_records
        sq002_source_group = sq002_source_group()
        sq002_enrichment_group = sq002_enrichment_group()

        sq002_source_group >> sq002_enrichment_group


    @task(outlets=[AssetAlias("basel_cust_dim")])
    def basel_cust_dim(*, outlet_events):
        outlet_events[AssetAlias("basel_cust_dim")].add(
            Asset("ingestion.BASEL_CUST_DIM", extra={})
        )


    sq002 = sq002_group()
    sq002_start = sq002_start()
    basel_cust_dim = basel_cust_dim()

    sq002_start >> sq002 >> basel_cust_dim
    @task
    def sq003_start():
        """ Manual approval task to start sq003 """
        raise AirflowException("Please mark this task successful to start sequence sq003.")


    @task_group(group_id="sq003")
    def sq003_group():
        """
        TaskGroup for sequence sq003.
        """

        @task_group(group_id="sq003_source")
        def sq003_source_group():
            """
            TaskGroup for source tasks from EDL in sequence sq003.
            """
            # Import of source_group.py
            @task
            def create_sq003_rundir():
                """
                Task to create RUNDIR for sequence sq003.
                RUNDIR is the directory where extracted data for the sequence is stored.
                """
                context = get_current_context()
                rundir = context["ti"].xcom_pull(task_ids="handle_month_context", key="RUNDIR")
                sq003_rundir = f"{rundir}/sq003"
                os.makedirs(sq003_rundir, exist_ok=True)


            @task.beeline(
                task_id="get_cust_acct_rltnp",
                beeline_conn_id="edlr-conn",
                sql="""
                use {{ var.value.EZ1_SCHEMA }};
                select distinct
                    mth_end_dt,
                    cast(cast(acct_num as bigint) as string) as acct_num,
                    src_sys_cd
                from cust_acct_rltnp
                where mth_end_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="MTH_END_DT") }}';
                """,
                schema=pa.schema([
                    ("mth_end_dt", pa.date64()),
                    ("acct_num", pa.string()),
                    ("src_sys_cd", pa.string()),
                ]),
                target="ez1_cust_acct_rltnp.parquet",
                rundir="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq003",
                to_parquet=True,
                tmpfileloc="/bns/rrap/data/tmp",
            )
            def get_cust_acct_rltnp():
                pass


            @task.beeline(
                task_id="get_mortgage_mth_snapshot",
                beeline_conn_id="edlr-conn",
                sql="""
                use {{ var.value.RCRR_SCHEMA }};
                select
                    mth_end_dt,
                    cast(mort_num as varchar(80)) as acct_num,
                    src_sys_cd
                from mortgage_mth_snapshot
                where mth_end_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="MTH_END_DT") }}';
                """,
                schema=pa.schema([
                    ("mth_end_dt", pa.date64()),
                    ("acct_num", pa.string()),
                    ("src_sys_cd", pa.string()),
                ]),
                target="mortgage_mth_snapshot.parquet",
                rundir="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq003",
                to_parquet=True,
                tmpfileloc="/bns/rrap/data/tmp",
            )
            def get_mortgage_mth_snapshot():
                pass


            @task.beeline(
                task_id="get_tsys_revlvng_credit_mth_snapshot",
                beeline_conn_id="edlr-conn",
                sql="""
                use {{ var.value.RCRR_SCHEMA }};
                select
                    mth_end_dt,
                    cast(acct_num as varchar(80)) as acct_num,
                    src_sys_cd
                from tsys_revlvng_credit_mth_snapshot
                where mth_end_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="MTH_END_DT") }}';
                """,
                schema=pa.schema([
                    ("mth_end_dt", pa.date64()),
                    ("acct_num", pa.string()),
                    ("src_sys_cd", pa.string()),
                ]),
                target="tsys_revlvng_credit_mth_snapshot.parquet",
                rundir="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq003",
                to_parquet=True,
                tmpfileloc="/bns/rrap/data/tmp",
            )
            def get_tsys_revlvng_credit_mth_snapshot():
                pass


            @task.beeline(
                task_id="get_psnl_loan_mth_snapshot",
                beeline_conn_id="edlr-conn",
                sql="""
                use {{ var.value.RCRR_SCHEMA }};
                select
                    mth_end_dt,
                    cast(acct_num as varchar(80)) as acct_num,
                    src_sys_cd
                from psnl_loan_mth_snapshot
                where mth_end_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="MTH_END_DT") }}'
                group by mth_end_dt, acct_num, src_sys_cd;
                """,
                schema=pa.schema([
                    ("mth_end_dt", pa.date64()),
                    ("acct_num", pa.string()),
                    ("src_sys_cd", pa.string()),
                ]),
                target="psnl_loan_mth_snapshot.parquet",
                rundir="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq003",
                to_parquet=True,
                tmpfileloc="/bns/rrap/data/tmp",
            )
            def get_psnl_loan_mth_snapshot():
                pass


            @task.beeline(
                task_id="get_tng_mort_acct_mth_snapshot",
                beeline_conn_id="edlr-conn",
                sql="""
                use {{ var.value.RCRR_SCHEMA }};
                select
                    mth_end_dt,
                    cast(acct_id as varchar(80)) as acct_num,
                    src_sys_cd
                from tng_mort_acct_mth_snapshot
                where mth_end_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="MTH_END_DT") }}'
                group by mth_end_dt, acct_id, src_sys_cd;
                """,
                schema=pa.schema([
                    ("mth_end_dt", pa.date64()),
                    ("acct_num", pa.string()),
                    ("src_sys_cd", pa.string()),
                ]),
                target="tng_mort_acct_mth_snapshot.parquet",
                rundir="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq003",
                to_parquet=True,
                tmpfileloc="/bns/rrap/data/tmp",
            )
            def get_tng_mort_acct_mth_snapshot():
                pass


            @task.parquet(
                task_id="odbc_ez_cust_acct_rltnp",
                duckdb_conn_id="duckdb-conn",
                target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq003/odbc_ez_cust_acct_rltnp.parquet",
                sql="""
                SELECT
                    mth_end_dt,
                    acct_num,
                    CASE trim(src_sys_cd)
                        WHEN 'KQ_TSYS' THEN cast('TSYS-rev' as varchar(20))
                        WHEN 'TSYS' THEN cast('TSYS-rev' as varchar(20))
                        ELSE trim(src_sys_cd)
                    END AS src_sys_cd
                FROM '{{ task_instance.xcom_pull(task_ids='sq003.sq003_source.get_cust_acct_rltnp', key='parquet') }}'

                UNION

                SELECT
                    mth_end_dt,
                    acct_num,
                    cast('GZ' as varchar(20)) as src_sys_cd
                FROM '{{ task_instance.xcom_pull(task_ids='sq003.sq003_source.get_mortgage_mth_snapshot', key='parquet') }}'

                UNION

                SELECT
                    mth_end_dt,
                    acct_num,
                    CASE trim(src_sys_cd)
                        WHEN 'KQ_TSYS' THEN cast('TSYS-rev' as varchar(20))
                        WHEN 'TSYS' THEN cast('TSYS-rev' as varchar(20))
                        ELSE cast('KQ' as varchar(20))
                    END AS src_sys_cd
                FROM '{{ task_instance.xcom_pull(task_ids='sq003.sq003_source.get_tsys_revlvng_credit_mth_snapshot', key='parquet') }}'

                UNION

                SELECT
                    mth_end_dt,
                    acct_num,
                    cast('SL' as varchar(20)) as src_sys_cd
                FROM '{{ task_instance.xcom_pull(task_ids='sq003.sq003_source.get_psnl_loan_mth_snapshot', key='parquet') }}'

                UNION

                SELECT
                    mth_end_dt,
                    acct_num,
                    cast('TNG_MTG' as varchar(20)) as src_sys_cd
                FROM '{{ task_instance.xcom_pull(task_ids='sq003.sq003_source.get_tng_mort_acct_mth_snapshot', key='parquet') }}'
                """,
                export_params={},
                clear_before_write=True,
            )
            def odbc_ez_cust_acct_rltnp():
                pass


            @task.parquet(
                task_id="remove_tsys_dupes",
                duckdb_conn_id="duckdb-conn",
                target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq003/remove_tsys_dupes.parquet",
                sql="""
                SELECT DISTINCT
                    LPAD(TRIM(acct_num), 23, '0') AS acct_num,
                    src_sys_cd,
                    mth_end_dt
                FROM '{{ task_instance.xcom_pull(task_ids='sq003.sq003_source.odbc_ez_cust_acct_rltnp', key='parquet') }}'
                WHERE src_sys_cd = 'TSYS-rev'
                """,
                export_params={},
                clear_before_write=True,
            )
            def remove_tsys_dupes():
                pass


            @task.parquet(
                task_id="select_other_sources",
                duckdb_conn_id="duckdb-conn",
                target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq003/select_other_sources.parquet",
                sql="""
                SELECT *
                FROM '{{ task_instance.xcom_pull(task_ids='sq003.sq003_source.odbc_ez_cust_acct_rltnp', key='parquet') }}'
                WHERE src_sys_cd != 'TSYS-rev'
                """,
                export_params={},
                clear_before_write=True,
            )
            def select_other_sources():
                pass


            @task.parquet(
                task_id="xfm",
                duckdb_conn_id="duckdb-conn",
                target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq003/airb_acct_dim.parquet",
                sql="""
                SELECT
                    ANY_VALUE(acct_num) as acct_num,
                    src_sys_cd
                FROM (
                    SELECT
                        acct_num,
                        CASE src_sys_cd
                            WHEN 'TSYS-rev' THEN 'KQ'
                            ELSE src_sys_cd
                        END AS src_sys_cd
                    FROM '{{ task_instance.xcom_pull(task_ids='sq003.sq003_source.remove_tsys_dupes', key='parquet') }}'

                    UNION

                    SELECT
                        acct_num,
                        src_sys_cd
                    FROM '{{ task_instance.xcom_pull(task_ids='sq003.sq003_source.select_other_sources', key='parquet') }}'
                )
                GROUP BY LPAD(TRIM(acct_num), 23, '0'), src_sys_cd
                """,
                export_params={},
                clear_before_write=True,
            )
            def xfm():
                pass


            @task.beeline(
                task_id="kq_tkq_ks_tsys_xref",
                beeline_conn_id="edlr-conn",
                sql="""
                use {{ var.value.TSZ_SCHEMA }};
                select
                    bcm_acct_num,
                    tsys_acct_id,
                    client_prod_cd,
                    tsys_prod_cd,
                    emp_cd,
                    ks_plastic_card_num,
                    bcm_prod_cd,
                    bcm_sub_prod_cd,
                    bcm_block_reclass,
                    tsys_cust_id,
                    tsys_cust_type_cd,
                    tsys_plastic_card_num,
                    transfer_from_acct_num,
                    bns_cust_id,
                    conversion_dt,
                    end_of_chain_indicator,
                    businesseffectivedate
                from kq_tkq_ks_tsys_xref
                where businesseffectivedate = '2025-08-16' and end_of_chain_indicator = 'Y';
                """,
                schema=pa.schema([
                    ("bcm_acct_num", pa.string()),
                    ("tsys_acct_id", pa.string()),
                    ("client_prod_cd", pa.string()),
                    ("tsys_prod_cd", pa.string()),
                    ("emp_cd", pa.string()),
                    ("ks_plastic_card_num", pa.string()),
                    ("bcm_prod_cd", pa.string()),
                    ("bcm_sub_prod_cd", pa.string()),
                    ("bcm_block_reclass", pa.string()),
                    ("tsys_cust_id", pa.string()),
                    ("tsys_cust_type_cd", pa.string()),
                    ("tsys_plastic_card_num", pa.string()),
                    ("transfer_from_acct_num", pa.string()),
                    ("bns_cust_id", pa.string()),
                    ("conversion_dt", pa.string()),
                    ("end_of_chain_indicator", pa.string()),
                    ("businesseffectivedate", pa.date64()),
                ]),
                target="kq_tkq_ks_tsys_xref.parquet",
                rundir="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq003",
                to_parquet=True,
                tmpfileloc="/bns/rrap/data/tmp",
            )
            def kq_tkq_ks_tsys_xref():
                pass



            @task.parquet(
                task_id="join_basel_acct_id",
                duckdb_conn_id="duckdb-conn",
                target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq003/join_basel_acct_id.parquet",
                sql="""
                SELECT
                    airb.*, iias.basel_acct_id
                FROM '{{ task_instance.xcom_pull(task_ids='sq003.sq003_source.odbc_ez_cust_acct_rltnp', key='parquet') }}' as airb
                LEFT JOIN ingestion.BASEL_ACCT_DIM as iias
                    ON lpad(trim(iias.app_id), 23, '0') = lpad(trim(airb.acct_num), 23, '0')
                    AND trim(iias.app_cd) = trim(
                        CASE airb.src_sys_cd
                            WHEN 'KQ_TSYS' THEN 'KS'
                            WHEN 'TSYS-rev' THEN 'KS'
                        END
                    )
                WHERE iias.basel_acct_id is null and src_sys_cd in ('KQ_TSYS', 'TSYS-rev')
                """,
                export_params={},
                clear_before_write=True,
            )
            def join_basel_acct_id():
                pass


            @task.parquet(
                task_id="join_xref",
                duckdb_conn_id="duckdb-conn",
                target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq003/join_xref.parquet",
                sql="""
                SELECT j.*
                FROM '{{ task_instance.xcom_pull(task_ids='sq003.sq003_source.join_basel_acct_id', key='parquet') }}' as j
                LEFT JOIN '{{ task_instance.xcom_pull(task_ids='sq003.sq003_source.kq_tkq_ks_tsys_xref', key='parquet') }}' as m1
                    ON lpad(trim(j.acct_num), 23, '0') = lpad(trim(m1.bcm_acct_num), 23, '0')
                LEFT JOIN '{{ task_instance.xcom_pull(task_ids='sq003.sq003_source.kq_tkq_ks_tsys_xref', key='parquet') }}' as m2
                    ON lpad(trim(j.acct_num), 23, '0') = lpad(trim(m2.tsys_acct_id), 23, '0')
                WHERE m1.tsys_acct_id is null and m2.tsys_acct_id is null
                """,
                export_params={},
                clear_before_write=True,
            )
            def join_xref():
                pass


            @task.parquet(
                task_id="tsys_net_new_report",
                duckdb_conn_id="duckdb-conn",
                target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq003/tsys_net_new_report.parquet",
                sql="""
                SELECT
                    ANY_VALUE(acct_num) as acct_num,
                    CASE
                        WHEN COUNT(CASE WHEN src_sys_cd = 'TSYS-rev' THEN 1 END) > 0
                            AND COUNT(CASE WHEN src_sys_cd = 'KQ_TSYS' THEN 1 END) > 0 THEN 'both'
                        WHEN COUNT(CASE WHEN src_sys_cd = 'TSYS-rev' THEN 1 END) > 0 THEN 'prod_rcrr1.tsys_revlvng_credit_mth_snapshot'
                        WHEN COUNT(CASE WHEN src_sys_cd = 'KQ_TSYS' THEN 1 END) > 0 THEN 'ez1.cust_acct_rltnp'
                    END AS tsys_cd_origin,
                    now() AS date_converted,
                    '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="MTH_END_DT") }}' AS mth_end_dt
                FROM '{{ task_instance.xcom_pull(task_ids='sq003.sq003_source.join_xref', key='parquet') }}'
                WHERE src_sys_cd in ('TSYS-rev', 'KQ_TSYS')
                GROUP BY lpad(trim(acct_num), 23, '0')
                """,
                export_params={},
                clear_before_write=True,
            )
            def tsys_net_new_report():
                pass


            create_sq003_rundir = create_sq003_rundir()
            get_cust_acct_rltnp = get_cust_acct_rltnp()
            get_mortgage_mth_snapshot = get_mortgage_mth_snapshot()
            get_tsys_revlvng_credit_mth_snapshot = get_tsys_revlvng_credit_mth_snapshot()
            get_psnl_loan_mth_snapshot = get_psnl_loan_mth_snapshot()
            get_tng_mort_acct_mth_snapshot = get_tng_mort_acct_mth_snapshot()
            odbc_ez_cust_acct_rltnp = odbc_ez_cust_acct_rltnp()
            remove_tsys_dupes = remove_tsys_dupes()
            select_other_sources = select_other_sources()
            xfm = xfm()
            kq_tkq_ks_tsys_xref = kq_tkq_ks_tsys_xref()
            join_basel_acct_id = join_basel_acct_id()
            join_xref = join_xref()
            tsys_net_new_report = tsys_net_new_report()


            create_sq003_rundir >> [
                get_cust_acct_rltnp,
                get_mortgage_mth_snapshot,
                get_tsys_revlvng_credit_mth_snapshot,
                get_psnl_loan_mth_snapshot,
                get_tng_mort_acct_mth_snapshot,
                kq_tkq_ks_tsys_xref,
            ]

            [
                get_cust_acct_rltnp,
                get_mortgage_mth_snapshot,
                get_tsys_revlvng_credit_mth_snapshot,
                get_psnl_loan_mth_snapshot,
                get_tng_mort_acct_mth_snapshot,
            ] >> odbc_ez_cust_acct_rltnp

            odbc_ez_cust_acct_rltnp >> [remove_tsys_dupes, select_other_sources] >> xfm

            odbc_ez_cust_acct_rltnp >> join_basel_acct_id
            [join_basel_acct_id, kq_tkq_ks_tsys_xref] >> join_xref >> tsys_net_new_report
        @task_group(group_id="sq003_enrichment")
        def sq003_enrichment_group():
            """
            TaskGroup for enrichment tasks in sequence sq003.
            """
            @task
            def get_max_basel_acct_id():
                """
                This task queries the max BASEL_ACCT_ID from IIAS_BASEL_ACCT_DIM and pushes the value to XCom for downstream use.
                """
                hook = DuckLakeHook(duckdb_conn_id='duckdb-conn')
                result = hook.get_first("SELECT MAX(BASEL_ACCT_ID) AS max_id FROM ingestion.BASEL_ACCT_DIM")
                return result.to_df()['max_id'][0]


            @task.parquet(
                task_id="join_01",
                duckdb_conn_id="duckdb-conn",
                target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq003/join_01.parquet",
                sql="""
                SELECT
                    airb.acct_num::text as ACCT_NUM,
                    airb.src_sys_cd as SRC_SYS_CD,
                    lpad(trim(airb.acct_num), 23, '0') as APP_ID,
                    CASE airb.src_sys_cd 
                        WHEN 'KQ' THEN 'KS' 
                        WHEN 'GZ' THEN 'MO' 
                        WHEN 'SL' THEN 'SPL'
                        WHEN 'TNG_MTG' THEN 'TNG-MOR'
                        WHEN 'TSYS' THEN 'KS'
                        WHEN 'KQ_TSYS' THEN 'KS'
                    END as APP_CD,
                    iias.basel_acct_id as BASEL_ACCT_ID
                FROM '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq003/airb_acct_dim.parquet' as airb
                LEFT JOIN ingestion.BASEL_ACCT_DIM as iias
                    ON lpad(trim(airb.acct_num), 23, '0') = lpad(trim(iias.app_id), 23, '0')
                    AND trim(airb.src_sys_cd) = trim(iias.app_cd)
                WHERE iias.basel_acct_id is null
                """,
                export_params={},
                clear_before_write=True,
            )
            def join_01():
                """
                This task filters out any existing accounts (i.e. accounts with existing BASEL_ACCT_IDs) by joining airb_acct_dim.parquet 
                with BASEL_ACCT_DIM on account number (APP_ID) and source system code (APP_CD).
                """
                pass


            @task.parquet(
                task_id="join_02",
                duckdb_conn_id="duckdb-conn",
                target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq003/join_02.parquet",
                sql="""
                SELECT j.* 
                FROM '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq003/join_01.parquet' as j
                LEFT JOIN  '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq003/kq_tkq_ks_tsys_xref.parquet' as m1
                    ON lpad(trim(j.ACCT_NUM), 23, '0') = lpad(trim(m1.bcm_acct_num), 23, '0')
                LEFT JOIN  '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq003/kq_tkq_ks_tsys_xref.parquet' as m2
                    ON lpad(trim(j.ACCT_NUM), 23, '0') = lpad(trim(m2.tsys_acct_id), 23, '0')
                WHERE m1.bcm_acct_num is null AND m2.tsys_acct_id is null
                """,
                export_params={},
                clear_before_write=True,
            )
            def join_02():
                """
                Similar to 'join_xref' in jb0031 TSYS_net_new_accts_report, new records with missing account numbers in the migrated account/cross-reference 
                table are **filtered out** and written to 'join_02.parquet'.
                """
                pass


            @task.parquet(
                task_id="xfm_01",
                duckdb_conn_id="duckdb-conn",
                target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq003/basel_acct_dim.parquet",
                sql="""
                SELECT
                {{ task_instance.xcom_pull(task_ids='sq003.sq003_enrichment.get_max_basel_acct_id') }} + ROW_NUMBER() over (order by ACCT_NUM, SRC_SYS_CD) as BASEL_ACCT_ID,
                null as CIS_PRD_CD,
                CASE WHEN SRC_SYS_CD = 'TNG_MTG' THEN ACCT_NUM ELSE RTRIM(APP_ID) END as ACCT_NUM,
                CASE WHEN SRC_SYS_CD = 'TNG_MTG' THEN ACCT_NUM ELSE APP_ID END as SRC_APP_ID,
                null as INTG_LAYER_SRC_ID,
                '9999-12-31' as SRC_SYS_DEL_DT,
                APP_CD as SRC_APP_CD,
                null as INTG_LAYER_SRC_TBL_NM,
                'N' as SRC_SYS_DEL_F,
                now() as INSRT_PROCESS_TMSTMP,
                now() as UPDT_PROCESS_TMSTMP
                FROM '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq003/join_02.parquet'
                """,
                export_params={},
                clear_before_write=True,
            )
            def xfm_01():
                """ 
                This task generates new BASEL_ACCT_ID values for new records from 'join_02.parquet' using the max BASEL_ACCT_ID from BASEL_ACCT_DIM
                 as a starting point, and writes to 'basel_acct_dim.parquet'.
                """


            @task
            def approve_load() -> None:
                """ Auto-failing task to prevent BASEL_ACCT_DIM from being loaded before review. """
                raise AirflowException("Approval required to load data to BASEL_ACCT_DIM. Please review the contents of 'basel_acct_dim.parquet' before approving.")


            @task.duckdb(
                task_id="load_to_base_acct_dim",
                duckdb_conn_id="duckdb-conn",
                sql="""
                INSERT INTO ingestion.BASEL_ACCT_DIM
                SELECT * FROM '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq003/basel_acct_dim.parquet'
                """,
            )
            def load_to_base_acct_dim():
                """
                This task loads the new BASEL_ACCT_DIM records from 'basel_acct_dim.parquet' into the BASEL_ACCT_DIM table in DuckDB.
                The task is set to run only after manual approval to ensure data quality checks can be performed on the generated parquet file before loading.
                """
                pass


            """ TaskFlow function calling """
            get_max_basel_acct_id = get_max_basel_acct_id()
            join_01 = join_01()
            join_02 = join_02()
            xfm_01 = xfm_01()
            approve_load = approve_load()
            load_to_base_acct_dim = load_to_base_acct_dim()

            """ Dependency chaining """
            join_01 >> join_02 >> xfm_01 >> load_to_base_acct_dim
            get_max_basel_acct_id >> xfm_01
            approve_load >> load_to_base_acct_dim
        sq003_source_group = sq003_source_group()
        sq003_enrichment_group = sq003_enrichment_group()

        sq003_source_group >> sq003_enrichment_group


    @task(outlets=[AssetAlias("basel_acct_dim")])
    def basel_acct_dim(*, outlet_events):
        outlet_events[AssetAlias("basel_acct_dim")].add(
            Asset("ingestion.BASEL_ACCT_DIM", extra={})
        )


    sq003 = sq003_group()
    sq003_start = sq003_start()
    basel_acct_dim = basel_acct_dim()

    sq003_start >> sq003 >> basel_acct_dim
    @task
    def sq004_start():
        """ Manual approval task to start sq004 """
        raise AirflowException("Please mark this task successful to start sequence sq004.")


    @task_group(group_id="sq004")
    def sq004_group():
        """
        TaskGroup for sequence sq004
        """

        @task_group(group_id="sq004_source")
        def sq004_source_group():
            """
            TaskGroup for source tasks from EDL in sequence sq004
            """
            # Import of source_group.py
            @task
            def create_sq004_rundir():
                """
                Task to create RUNDIR for sequence sq004.
                RUNDIR is the directory where extracted data for the sequence is stored.
                """
                context = get_current_context()
                rundir = context["ti"].xcom_pull(task_ids="handle_month_context", key="RUNDIR")
                sq004_rundir = f"{rundir}/sq004"
                os.makedirs(sq004_rundir, exist_ok=True)


            @task.beeline(
                task_id="airb_cust_acct_rltnp",
                beeline_conn_id="edlr-conn",
                sql="""
                use {{ var.value.EZ1_SCHEMA }};
                SELECT
                    DISTINCT
                    TRIM(CUST_ID) AS cust_id,
                    CAST(CAST(ACCT_NUM AS BIGINT) AS string) AS acct_num,
                    CUST_ACCT_RLTNP_TYPE_CD AS cust_acct_rltnp_type_cd,
                    PRIMARY_ACCT_HOLDER_F AS primary_acct_holder_f,
                    SRC_SYS_CD AS src_sys_cd,
                    MTH_END_DT AS mth_end_dt
                FROM
                    cust_acct_rltnp
                WHERE
                    src_sys_cd IN ('KQ', 'GZ', 'SL', 'KQ_TSYS')
                    AND mth_end_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="MTH_END_DT") }}';
                """,
                schema=pa.schema([
                    ("cust_id", pa.string()),
                    ("acct_num", pa.string()),
                    ("cust_acct_rltnp_type_cd", pa.string()),
                    ("primary_acct_holder_f", pa.string()),
                    ("src_sys_cd", pa.string()),
                    ("mth_end_dt", pa.date64()),
                ]),
                target="airb_cust_acct_rltnp.parquet",
                rundir="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq004",
                to_parquet=True,
                tmpfileloc="/bns/rrap/data/tmp",
            )
            def airb_cust_acct_rltnp():
                """
                Task to extract customer/account relationship data for RRAP portfolios from EZ1.cust_acct_rltnp.
                """
                pass


            create_sq004_rundir = create_sq004_rundir()
            airb_cust_acct_rltnp = airb_cust_acct_rltnp()

            create_sq004_rundir >> airb_cust_acct_rltnp
        @task_group(group_id="sq004_enrichment")
        def sq004_enrichment_group():
            """
            TaskGroup for enrichment tasksin sequence sq004
            Currently, IIAS data used to enrich EDL data
            Future, DuckLake / MSSQL data used to enrich EDL data
            """
            # Implementation for enrichment tasks goes here
            @task.parquet(
                task_id="join_1_cust_id",
                duckdb_conn_id="duckdb-conn",
                target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq004/join_1_cust_id.parquet",
                sql="""
                SELECT acct_num, src_sys_cd, mth_end_dt, primary_acct_holder_f as prim_cust_f,
                        cust_acct_rltnp_type_cd as rel_cd, B.BASEL_CUST_ID 
                FROM "{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq004/airb_cust_acct_rltnp.parquet" AS A -- comes from jb0042
                LEFT OUTER JOIN ingestion.BASEL_CUST_DIM AS B ON A.cust_id = B.CUST_CID
                """,
                export_params={},
                clear_before_write=True,
            )
            def join_1_cust_id():
                pass


            @task.beeline(
                task_id="converted_ks_to_tsys_accts",
                beeline_conn_id="edlr-conn",
                sql="""
                SELECT 
                    bcm_acct_num,
                    tsys_acct_id,
                    client_prod_cd,
                    tsys_prod_cd,
                    emp_cd,
                    ks_plastic_card_num,
                    bcm_prod_cd,
                    bcm_sub_prod_cd,
                    bcm_block_reclass,
                    tsys_cust_id,
                    tsys_cust_type_cd,
                    tsys_plastic_card_num,
                    transfer_from_acct_num,
                    bns_cust_id,
                    conversion_dt,
                    end_of_chain_indicator,
                    businesseffectivedate
                FROM {{ params.EDL_schema_tsz }}.kq_tkq_ks_tsys_xref
                WHERE businesseffectivedate IN ('2024-11-09', '2025-08-16') AND end_of_chain_indicator='Y';
                """,
                schema=pa.schema([
                    ('bcm_acct_num', pa.string()),
                    ('tsys_acct_id', pa.string()),
                    ('client_prod_cd', pa.string()),
                    ('tsys_prod_cd', pa.string()),
                    ('emp_cd', pa.string()),
                    ('ks_plastic_card_num', pa.string()),
                    ('bcm_prod_cd', pa.string()),
                    ('bcm_sub_prod_cd', pa.string()),
                    ('bcm_block_reclass', pa.string()),
                    ('tsys_cust_id', pa.string()),
                    ('tsys_cust_type_cd', pa.string()),
                    ('tsys_plastic_card_num', pa.string()),
                    ('transfer_from_acct_num', pa.string()),
                    ('bns_cust_id', pa.string()),
                    ('conversion_dt', pa.string()), 
                    ('end_of_chain_indicator', pa.string()),
                    ('businesseffectivedate', pa.date64())
                ]),
                target="converted_ks_to_tsys_accts.parquet",
                rundir="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq004",
                to_parquet=True,
                tmpfileloc="/bns/rrap/data/tmp"
            )
            def converted_ks_to_tsys_accts():
                pass


            @task.parquet(
                task_id="exclude_converted_tsys_accts",
                duckdb_conn_id="duckdb-conn",
                target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq004/exclude_converted_tsys_accts.parquet",
                sql="""
                SELECT * 
                FROM "{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq004/join_1_cust_id.parquet"
                WHERE lpad(trim(acct_num), 23, '0') NOT IN
                (
                    SELECT lpad(trim(tsys_acct_id), 23, '0') 
                    FROM "{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq004/converted_ks_to_tsys_accts.parquet"
                )
                """,
                export_params={},
                clear_before_write=True,
            )
            def exclude_converted_tsys_accts():
                pass


            @task.parquet(
                task_id="replace_tsys_acct_ids",
                duckdb_conn_id="duckdb-conn",
                target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq004/replace_tsys_acct_ids.parquet",
                sql="""
                SELECT
                    COALESCE(xref.bcm_acct_num, a.acct_num) as acct_num,
                    a.src_sys_cd, 
                    a.mth_end_dt, 
                    a.prim_cust_f, 
                    a.rel_cd,
                    a.BASEL_CUST_ID 
                FROM "{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq004/exclude_converted_tsys_accts.parquet" as a
                LEFT JOIN (
                        SELECT bcm_acct_num, tsys_acct_id  -- no need to do anyval bc of AND end_of_chain_indicator='Y';
                        FROM "{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq004/converted_ks_to_tsys_accts.parquet"
                        GROUP BY bcm_acct_num, tsys_acct_id
                    ) as xref
                ON 
                    lpad(trim(a.acct_num), 23, '0') = lpad(trim(xref.tsys_acct_id), 23, '0')
                """,
                export_params={},
                clear_before_write=True,
            )
            def replace_tsys_acct_ids():
                pass


            @task.parquet(
                task_id="join_2_acct_id",
                duckdb_conn_id="duckdb-conn",
                target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq004/join_2_acct_id.parquet",
                sql="""
                SELECT A.*, B.BASEL_ACCT_ID
                FROM "{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq004/replace_tsys_acct_ids.parquet" AS A
                LEFT OUTER JOIN ingestion.BASEL_ACCT_DIM AS B
                ON 
                lpad(trim(A.acct_num), 23, '0') = lpad(trim(B.SRC_APP_ID), 23, '0')
                AND 
                (CASE A.src_sys_cd 
                    WHEN 'KQ' THEN 'KS'
                    WHEN 'KQ_TSYS' THEN 'KS'
                    WHEN 'GZ' THEN 'MO'
                    WHEN 'SL' THEN 'SPL'
                    END) = trim(B.SRC_APP_CD)
                WHERE 
                    B.BASEL_ACCT_ID IS NOT NULL
                """,
                export_params={},
                clear_before_write=True,
            )
            def join_2_acct_id():
                pass


            @task.parquet(
                task_id="join_3_mth_tm",
                duckdb_conn_id="duckdb-conn",
                target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq004/join_3_mth_tm.parquet",
                sql="""
                SELECT A.*, B.TM_ID AS MTH_TM_ID
                FROM "{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq004/join_2_acct_id.parquet" AS A
                INNER JOIN ingestion.TM_DIM as B
                ON A.mth_end_dt = B.TM_LVL_END_DT
                """,
                export_params={},
                clear_before_write=True,
            )
            def join_3_mth_tm():
                pass


            @task.parquet(
                task_id="cleanup_tables",
                duckdb_conn_id="duckdb-conn",
                target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq004/BASEL_CUST_ACCT_RLTNP_SNAPSHOT.parquet",
                sql="""
                SELECT COALESCE(BASEL_CUST_ID, -1) AS BASEL_CUST_ID, COALESCE(BASEL_ACCT_ID, 1) AS BASEL_ACCT_ID, 
                COALESCE(MTH_TM_ID, -1) AS MTH_TM_ID, PRIM_CUST_F, REL_CD, CURRENT_TIMESTAMP AS INSRT_PROCESS_TMSTMP,
                CURRENT_TIMESTAMP AS UPDT_PROCESS_TMSTMP
                FROM "{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq004/join_3_mth_tm.parquet"
                WHERE NOT (
                    BASEL_CUST_ID is NULL AND MTH_TM_ID IN 
                        ( 
                        SELECT DISTINCT MTH_TM_ID
                        FROM "{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq004/join_3_mth_tm.parquet"
                        WHERE MTH_END_DT = '{{ var.value.MTH_END_DT }}' AND MTH_TM_ID IS NOT NULL
                        )
                )
                """,
                export_params={},
                clear_before_write=True,
            )
            def cleanup_tables():
                pass


            @task.duckdb(
                task_id="load_basel_cust_acct_rltnp_snapshot",
                duckdb_conn_id="duckdb-conn",
                sql="""
                INSERT INTO ingestion.BASEL_CUST_ACCT_RLTNP_SNAPSHOT BY NAME
                SELECT * FROM "{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq004/BASEL_CUST_ACCT_RLTNP_SNAPSHOT.parquet"
                """,
            )
            def load_basel_cust_acct_rltnp_snapshot():
                pass


            """ TaskFlow function calls """
            join_1_cust_id = join_1_cust_id()
            converted_ks_to_tsys_accts = converted_ks_to_tsys_accts()
            exclude_converted_tsys_accts = exclude_converted_tsys_accts()
            replace_tsys_acct_ids = replace_tsys_acct_ids()
            join_2_acct_id = join_2_acct_id()
            join_3_mth_tm = join_3_mth_tm()
            cleanup_tables = cleanup_tables()
            load_basel_cust_acct_rltnp_snapshot = load_basel_cust_acct_rltnp_snapshot()

            """ Dependency chaining """
            [ 
                converted_ks_to_tsys_accts, 
                join_1_cust_id 
            ] >> exclude_converted_tsys_accts

            exclude_converted_tsys_accts >> replace_tsys_acct_ids
            replace_tsys_acct_ids >> join_2_acct_id
            join_2_acct_id >> join_3_mth_tm
            join_3_mth_tm >> cleanup_tables
            cleanup_tables >> load_basel_cust_acct_rltnp_snapshot
        sq004_source_group = sq004_source_group()
        sq004_enrichment_group = sq004_enrichment_group() 

        sq004_source_group >> sq004_enrichment_group


    @task(outlets=[AssetAlias("basel_cust_acct_rltnp_snapshot")])
    def basel_cust_acct_rltnp_snapshot(*, outlet_events):
        outlet_events[AssetAlias("basel_cust_acct_rltnp_snapshot")].add(
            Asset("ingestion.BASEL_CUST_ACCT_RLTNP_SNAPSHOT", extra={})
        )



    sq004 = sq004_group()
    sq004_start = sq004_start()
    basel_cust_acct_rltnp_snapshot = basel_cust_acct_rltnp_snapshot()

    sq004_start >> sq004 >> basel_cust_acct_rltnp_snapshot
    @task
    def sq005_start():
        """ Manual approval task to start sq005 """
        raise AirflowException("Please mark this task successful to start sequence sq005.")


    @task_group(group_id="sq005")
    def sq005_group():
        """
        TaskGroup for sequence sq005.
        """

        @task_group(group_id="sq005_source")
        def sq005_source_group():
            """
            TaskGroup for source tasks from EDL in sequence sq005.
            """
            # Import of source_group.py
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
        @task_group(group_id="sq005_enrichment")
        def sq005_enrichment_group():
            """
            TaskGroup for enrichment tasks in sequence sq005.
            """
            @task.parquet(
                task_id="join_1_mth_tm_id",
                duckdb_conn_id="duckdb-conn",
                target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq005/join_1_mth_tm_id.parquet",
                sql="""
                SELECT 
                A.*, -- will also include MTH_END_DT which we will remove before upload
                B.TM_ID AS MTH_TM_ID
                FROM "{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq005/airb_step_pln_mth_snapshot.parquet" A
                INNER JOIN ingestion.TM_DIM as B
                ON A.MTH_END_DT = B.TM_LVL_END_DT
                """,
                export_params={},
                clear_before_write=True,
            )
            def join_1_mth_tm_id():
                pass


            @task.parquet(
                task_id="join_2_cust_id",
                duckdb_conn_id="duckdb-conn",
                target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq005/join_2_cust_id.parquet",
                # TODO: should we be using -1 or 0 - in IIAS it uses 0... but the BSTM says to use -1
                sql="""
                SELECT 
                A.*,
                CASE 
                    WHEN B.BASEL_CUST_ID IS NULL                               THEN -1  -- No match
                    WHEN A.PRIM_CUST_CID IS NULL OR TRIM(A.PRIM_CUSTCID) = '' THEN -2  -- Match on a NULL or empty PRIM_CUST_CID
                    ELSE B.BASEL_CUST_ID                                                -- Match, and PRIM_CUST_CID is valid
                END AS PRIM_BASEL_CUST_ID
                FROM "{{ task_instance.xcom_pull(task_ids='sq005.sq005.join_1_mth_tm_id', key='parquet') }}" AS A
                LEFT JOIN ingestion.BASEL_CUST_DIM AS B
                ON lpad(trim(A.PRIM_CUST_CID), 23, '0') = lpad(trim(B.CUST_CID), 23, '0') -- CUST_CID is already trimmed
                """,
                export_params={},
                clear_before_write=True,
            )
            def join_2_cust_id():
                pass


            @task.parquet(
                task_id="join_3_br_loctn_ou_id",
                duckdb_conn_id="duckdb-conn",
                target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq005/join_3_br_loctn_ou_id.parquet",
                sql="""
                SELECT
                    A.*,
                    CASE 
                        WHEN B.ORG_UNIT_ID IS NULL THEN -1
                        ELSE B.ORG_UNIT_ID
                    END AS BR_LOCTN_OU_ID
                FROM '{{ task_instance.xcom_pull(task_ids='sq005.sq005.join_2_cust_id', key='parquet') }}' A
                LEFT JOIN ingestion.ORG_UNIT_DIM B
                ON trim(A.BR_LOCTN_TRNST) = trim(CAST(B.TRNST_NUM AS VARCHAR))
                """,
                export_params={},
                clear_before_write=True,
            )


            @task
            def get_max_step_plan_snapshot_id():
                """ This task extracts the max STEP_PLN_SNAPSHOT_ID to be used for ID generation """
                hook = DuckLakeHook(duckdb_conn_id="duckdb-conn")
                result = hook.execute("""
                SELECT 
                    MAX(STEP_PLN_SNAPSHOT_ID) AS max_id
                FROM ingestion.BASEL_STEP_PLN_MTH_SNAPSHOT
                WHERE
                    MTH_TM_ID = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="PREV_MTH_TM_ID") }}'
                """)
                return result.to_df()["max_id"][0]


            @task.parquet(
                task_id="join_3_br_loctn_ou_id_with_id",
                duckdb_conn_id="duckdb-conn",
                target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq005/join_3_br_loctn_ou_id_with_id.parquet",
                sql="""
                SELECT
                    {{{{ task_instance.xcom_pull(task_ids='sq005.sq005_enrichment.get_max_step_plan_snapshot_id') }}}} + row_number() over (order by PRIM_BASEL_CUST_ID) as STEP_PLN_SNAPSHOT_ID,
                    A.*,
                    CASE
                        WHEN B.ORG_UNIT_ID IS NULL THEN -1
                        ELSE B.ORG_UNIT_ID
                    END AS BR_LOCTN_OU_ID,
                    FROM '{{ task_instance.xcom_pull(task_ids='sq005.sq005_enrichment.join_2_cust_id', key='parquet') }}' A
                    LEFT JOIN ingestion.ORG_UNIT_DIM B
                    ON trim(A.BR_LOCTN_TRNST) = trim(CAST(B.TRNST_NUM AS VARCHAR))
                """,
                export_params={},
                clear_before_write=True,
            )
            def join_3_br_loctn_ou_id_with_id():
                pass


            @task.update(
                task_id="update_basel_step_pln_mth_snapshot",
                duckdb_conn_id="duckdb-conn",
                target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq005/basel_step_pln_mth_snapshot.parquet",
                source="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq005/join_3_br_loctn_ou_id_with_id.parquet",
                sql="""
                SET 
                    CRFC_NUM = COALESCE(CRFC_NUM, ''),
                    INSURER_CD = COALESCE(INSURER_CD, ''),
                    ALI_PRD_CD = COALESCE(ALI_PRD_CD, ''),
                    PRPTY_PROV_CD = COALESCE(PRPTY_PROV_CD, '')
                WHERE MTH_TM_ID = {{ task_instance.xcom_pull(task_ids='handle_month_context', key='MTH_TM_ID') }} 
                AND (
                    CRFC_NUM IS NULL
                    OR INSURER_CD IS NULL
                    OR ALI_PRD_CD IS NULL
                    OR PRPTY_PROV_CD IS NULL
                )
                """,
                export_params={},
                clear_before_write=True,
            )
            def update_basel_step_pln_mth_snapshot():
                pass


            @task.duckdb(
                task_id="load_basel_step_pln_mth_snapshot",
                duckdb_conn_id="duckdb-conn",
                sql="""
                INSERT INTO ingestion.BASEL_STEP_PLN_MTH_SNAPSHOT BY NAME
                SELECT * EXCEPT (MTH_END_DT)
                FROM '{{ task_instance.xcom_pull(task_ids='sq005.sq005_enrichment.update_basel_step_pln_mth_snapshot', key='parquet') }}'
                """,
            )
            def load_basel_step_pln_mth_snapshot():
                pass


            """ TaskFlow function calls"""
            join_1_mth_tm_id = join_1_mth_tm_id()
            join_2_cust_id = join_2_cust_id()
            get_max_step_plan_snapshot_id = get_max_step_plan_snapshot_id()
            join_3_br_loctn_ou_id_with_id = join_3_br_loctn_ou_id_with_id()
            update_basel_step_pln_mth_snapshot = update_basel_step_pln_mth_snapshot()
            load_basel_step_pln_mth_snapshot = load_basel_step_pln_mth_snapshot()

            """ Dependency chaining """
            join_1_mth_tm_id >> join_2_cust_id
            [
                join_2_cust_id, 
                get_max_step_plan_snapshot_id
            ] >> join_3_br_loctn_ou_id_with_id
            join_3_br_loctn_ou_id_with_id >> update_basel_step_pln_mth_snapshot >> load_basel_step_pln_mth_snapshot
        sq005_source_group = sq005_source_group()
        sq005_enrichment_group = sq005_enrichment_group()

        sq005_source_group >> sq005_enrichment_group


    @task(outlets=[AssetAlias("basel_step_pln_mth_snapshot")])
    def basel_step_pln_mth_snapshot(*, outlet_events):
        outlet_events[AssetAlias("basel_step_pln_mth_snapshot")].add(
            Asset("ingestion.BASEL_STEP_PLN_MTH_SNAPSHOT", extra={})
        )


    sq005 = sq005_group()
    sq005_start = sq005_start()
    basel_step_pln_mth_snapshot = basel_step_pln_mth_snapshot()

    sq005_start >> sq005 >> basel_step_pln_mth_snapshot
    @task
    def sq006_start():
        """ Manual approval task to start sq006 """
        raise AirflowException("Please mark this task successful to start sequence sq006.")


    @task_group(group_id="sq006")
    def sq006_group():
        """
        TaskGroup for sequence sq006.
        """

        @task_group(group_id="sq006_source")
        def sq006_source_group():
            """
            TaskGroup for source tasks from EDL in sequence sq006.
            """
            # Import of source_group.py
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
        @task_group(group_id="sq006_enrichment")
        def sq006_enrichment_group():
            """
            TaskGroup for enrichment tasks in sequence sq006.
            """
            @task
            def get_max_basel_psnl_loan_mth_snapshot_id():
                ddb = DuckLakeHook(duckdb_conn_id="duckdb-conn")
                result = ddb.sql("""
                    SELECT MAX(BASEL_PSNL_LOAN_MTH_SNAPSHOT_ID) as max_id
                    FROM ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT
                    WHERE MTH_TM_ID = {{ task_instance.xcom_pull(task_ids='handle_month_context', key='PREV_MTH_TM_ID') }}
                """)
                return result.to_df()["max_id"][0]


            @task.parquet(
                task_id="make_basel_psnl_loan_mth_snapshot",
                duckdb_conn_id="duckdb-conn",
                target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq006/basel_psnl_loan_mth_snapshot.parquet",
                sql="""
                    with main as (
                        select
                            airb.*,
                            tm_dim.TM_ID as MTH_TM_ID
                        from
                            '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq006/airb_psnl_loan_mth_snapshot.parquet' as airb
                        inner join
                            ingestion.TM_DIM as tm_dim
                            on airb.MTH_END_DT = tm_dim.TM_LVL_END_DT
                    )
                    select
                        {{ task_instance.xcom_pull(task_ids='sq006.sq006_enrichment.get_max_basel_psnl_loan_mth_snapshot_id') }} + row_number() over (order by BASEL_ACCT_ID) as BASEL_PSNL_LOAN_MTH_SNAPSHOT_ID,
                        cast(main.TRNST_NUM as VARCHAR) as TRNST_NUM,
                        cast(main.LOAN_NUM as VARCHAR) as LOAN_NUM,
                        cast(main.RECD_STAT_CD as VARCHAR) as RECD_STAT_CD,
                        case
                            when main.RECD_STAT_DT = ''
                            then NULL
                            else cast(main.RECD_STAT_DT as DATE)
                        end as RECD_STAT_DT,
                        cast(main.CUST_RSDNC_CD as VARCHAR) as RSDNC_CD,
                        cast(main.TP_SRC_CD as VARCHAR) as LOAN_SRC_CD,
                        cast(main.LOAN_PRPS_CD as VARCHAR) as PRPS_CD,
                        cast(main.SCRTY_CD as VARCHAR) as SCRTY_CD,
                        cast(main.RT_CD as VARCHAR) as RT_CD,
                        cast(main.PROMISSORS_CNT as NUMERIC(1, 0)) as PROMISSORS_CNT,
                        cast(main.GRNT_CNT as NUMERIC(1, 0)) as GRNT_CNT,
                        cast(main.COMM_LOAN_CD as VARCHAR) as COMM_LOAN_CD,
                        case
                            when main.NOTE_DT = ''
                            then NULL
                            else cast(main.NOTE_DT as DATE)
                        end as NOTE_DT,
                        case
                            when main.FRST_RGL_PYMT_DT = ''
                            then NULL
                            else cast(main.FRST_RGL_PYMT_DT as DATE)
                        end as FRST_PAY_DT,
                        case
                            when main.LAST_RGL_PYMT_DT = ''
                            then NULL
                            else cast(main.LAST_RGL_PYMT_DT as DATE)
                        end as LAST_RGL_PAY_DT,
                        cast(main.ORIG_LOAN_AMT as NUMERIC(17, 3)) as ORIG_LOAN_AMT,
                        cast(main.ADD_ON_BAL_AMT as NUMERIC(17, 3)) as ADD_ON_BAL_AMT,
                        cast(main.ADD_ON_INTR_AMT as NUMERIC(17, 3)) as ADD_ON_INTR_AMT,
                        cast(main.DAYS_ODUE as NUMERIC(6, 0)) as DAY_ODUE,
                        cast(main.ACCR_INTR_AMT as NUMERIC(17, 3)) as ACCR_INTR,
                        case
                            when main.EARLY_MAT_DT = ''
                            then NULL
                            else cast(main.EARLY_MAT_DT as DATE)
                        end as EARLY_MAT_DT,
                        case
                            when main.LAST_PYMT_DT = ''
                            then NULL
                            else cast(main.LAST_PYMT_DT as DATE)
                        end as LAST_PYMT_DT,
                        cast(main.PRINCIPAL_BALANCE_AMT as NUMERIC(17, 3)) as TOT_CRNT_BAL_AMT,
                        cast(main.MOTOR_VEHCL_VAL as NUMERIC(17, 3)) as MOTOR_VEHCL_VAL,
                        cast(
                            main.SECURITY_HOUSEHOLD_CR_SCORE as NUMERIC(17, 3)
                        ) as HH_VAL,
                        cast(main.SCRTY_OTH_VAL as NUMERIC(17, 3)) as LOAN_VAL_OTH,
                        cast(main.PLS_CR_SCORE_OVRD_CD as NUMERIC(3, 0)) as CR_SCORE,
                        cast(main.BR_LOCTN_TRNST as VARCHAR) as CRNT_BR_LOCTN_TRNST,
                        cast(main.EARNED_MTH_INTR_AMT as NUMERIC(17, 3)) as EARNED_MTH_INTR,
                        case
                            when main.ORIG_NOTE_DT = ''
                            then NULL
                            else cast(main.ORIG_NOTE_DT as DATE)
                        end as LOAN_ORIG_NOTE_DT,
                        case
                            when main.CHRG_OFF_DT = ''
                            then NULL
                            else cast(main.CHRG_OFF_DT as DATE)
                        end as CHRG_OFF_DT,
                        cast(main.CHRG_OFF_AMT as NUMERIC(17, 3)) as CHRG_OFF_AMT,
                        cast(main.SECRTZTN_CD as VARCHAR) as SECRTZTN_CD,
                        cast(main.LOAN_TERM as NUMERIC(6, 0)) as LOAN_TERM,
                        cast(main.EARLY_MAT_TERM as NUMERIC(4, 0)) as EARLY_MAT_TERM,
                        cast(main.EARLY_MAT_STAT_CD as VARCHAR) as EARLY_MAT_STAT_CD,
                        cast(main.RGL_PYMT_AMT as NUMERIC(17, 3)) as RGL_PYMT_AMT,
                        cast(main.PRE_AUTHORIZED_DR_PYMT_FREQ_CD as VARCHAR) as PYMT_FREQ_CD,
                        cast(main.INTR_RT as NUMERIC(6, 2)) as INTR_RT,
                        cast(main.CIF_COMPANY_ID as NUMERIC(6, 0)) as CIF_COMPANY_ID,
                        cast(main.CIF_CUST_ID as VARCHAR) as CIF_CUST_ID,
                        cast(
                            main.CIF_CUST_ID_TIE_BRKR as NUMERIC(6, 0)
                        ) as CIF_TIE_BREAKER,
                        cast(main.STEP_PLN_AGRMNT_NUM as VARCHAR) as STEP_PLN_AGRMNT_NUM,
                        cast(main.PRIM_CUST_ID as VARCHAR) as CUST_CID,
                        cast(main.GL_ACCT_NUM as VARCHAR) as GL_ACCT_NUM,
                        cast(main.GL_TRNST_NUM as VARCHAR) as GL_TRNST_NUM,
                        cast(main.BOOKED_AMT as NUMERIC(17, 3)) as BOOKED_AMT,
                        cast(main.CRNCY_CD as VARCHAR) as CRNCY_CD,
                        cast(main.MTH_TM_ID as INTEGER) as MTH_TM_ID,
                        coalesce(acct_dim.BASEL_ACCT_ID, -1) as BASEL_ACCT_ID,
                        coalesce(cust_dim.BASEL_CUST_ID, -1) as PRIM_BASEL_CUST_ID,
                        coalesce(step_pln.STEP_PLN_SNAPSHOT_ID, -1) as STEP_PLN_SNAPSHOT_ID,
                        case
                            when unit_dim.ORG_UNIT_ID = 0
                            then -1
                            else unit_dim.ORG_UNIT_ID
                        end as TRNST_OU_ID,
                        case
                            when unit_dim_br.ORG_UNIT_ID = 0
                            then -1
                            else unit_dim_br.ORG_UNIT_ID
                        end as CRNT_BR_LOCTN_OU_ID,
                        CURRENT_TIMESTAMP as INSRT_PROCESS_TMSTMP,
                        CURRENT_TIMESTAMP as UPDT_PROCESS_TMSTMP
                    from main
                    left outer join
                        ingestion.BASEL_ACCT_DIM as acct_dim
                        on lpad(trim(main.ACCT_NUM), 23, '0') = lpad(trim(acct_dim.ACCT_NUM), 23, '0')
                    left outer join
                        ingestion.BASEL_CUST_DIM as cust_dim
                        on main.PRIM_CUST_ID = trim(cust_dim.CUST_CID)
                    left outer join
                        ingestion.BASEL_STEP_PLN_MTH_SNAPSHOT as step_pln
                        on main.STEP_PLN_AGRMNT_NUM = trim(step_pln.STEP_PLN_AGRMNT_NUM)
                        and main.MTH_TM_ID = step_pln.MTH_TM_ID
                    left outer join
                        ingestion.ORG_UNIT_DIM as unit_dim
                        on main.TRNST_NUM = trim(unit_dim.TRNST_NUM)
                    left outer join
                        ingestion.ORG_UNIT_DIM as unit_dim_br
                        on main.BR_LOCTN_TRNST = trim(unit_dim_br.TRNST_NUM)
                """,
                export_params={},
                clear_before_write=True,
            )
            def make_basel_psnl_loan_mth_snapshot():
                pass


            @task.parquet(
                task_id="get_duplicate_basel_acct_id",
                duckdb_conn_id="duckdb-conn",
                target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq006/dupe_check_basel_acct_id.parquet",
                sql="""
                    SELECT BASEL_ACCT_ID, COUNT(*) as cnt
                    FROM '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq006/basel_psnl_loan_mth_snapshot.parquet'
                    WHERE BASEL_ACCT_ID IS NOT NULL
                    GROUP BY BASEL_ACCT_ID
                    HAVING COUNT(*) > 1
                """,
                export_params={},
                clear_before_write=True,
            )
            def check_duplicate_basel_acct_id():
                pass


            @task.parquet(
                task_id="get_duplicate_crnt_br_loctn_trnst_loan_num",
                duckdb_conn_id="duckdb-conn",
                target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq006/dupe_check_crnt_br_loctn_trnst_loan_num.parquet",
                sql="""
                    SELECT CRNT_BR_LOCTN_TRNST_LOAN_NUM, LOAN_NUM, COUNT(*) as cnt
                    FROM '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq006/basel_psnl_loan_mth_snapshot.parquet'
                    WHERE CRNT_BR_LOCTN_TRNST_LOAN_NUM IS NOT NULL
                    GROUP BY CRNT_BR_LOCTN_TRNST_LOAN_NUM, LOAN_NUM
                    HAVING COUNT(*) > 1
                """,
                export_params={},
                clear_before_write=True,
            )
            def check_duplicate_crnt_br_loctn_trnst_loan_num():
                pass


            @task.parquet(
                task_id="get_duplicate_trnst_num_loan_num",
                duckdb_conn_id="duckdb-conn",
                target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq006/dupe_check_trnst_num_loan_num.parquet",
                sql="""
                    SELECT TRNST_NUM_LOAN_NUM, LOAN_NUM, COUNT(*) as cnt
                    FROM '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq006/basel_psnl_loan_mth_snapshot.parquet'
                    WHERE TRNST_NUM_LOAN_NUM IS NOT NULL
                    GROUP BY TRNST_NUM_LOAN_NUM, LOAN_NUM
                    HAVING COUNT(*) > 1
                """,
                export_params={},
                clear_before_write=True,
            )
            def check_duplicate_trnst_num_loan_num():
                pass


            @task
            def check_duplicate_results():
                """ Check the results of the duplicate check tasks and raise an exception if duplicates are found."""
                hook = DuckLakeHook(duckdb_conn_id="duckdb-conn")
                results = hook.sql("""
                SELECT COUNT(*) as cnt FROM (
                    SELECT * FROM '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq006/dupe_check_basel_acct_id.parquet'
                    UNION ALL
                    SELECT * FROM '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq006/dupe_check_crnt_br_loctn_trnst_loan_num.parquet'
                    UNION ALL
                    SELECT * FROM '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq006/dupe_check_trnst_num_loan_num.parquet'
                )""")
                count = results.to_df()["cnt"][0]
                if count > 0:
                    raise AirflowException(f"Duplicate records found in duplicate check tasks. Total duplicates: {count}")


            @task.duckdb(
                task_id="load_basel_psnl_loan_mth_snapshot",
                duckdb_conn_id="duckdb-conn",
                sql="""
                INSERT INTO ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT BY NAME
                SELECT * FROM '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq006/basel_psnl_loan_mth_snapshot.parquet'
                """,
            )
            def load_basel_psnl_loan_mth_snapshot():
                pass


            """ TaskFlow function calls """
            make_basel_psnl_loan_mth_snapshot = make_basel_psnl_loan_mth_snapshot()
            check_duplicate_basel_acct_id = check_duplicate_basel_acct_id()
            check_duplicate_crnt_br_loctn_trnst_loan_num = check_duplicate_crnt_br_loctn_trnst_loan_num()
            check_duplicate_trnst_num_loan_num = check_duplicate_trnst_num_loan_num()
            check_duplicate_results = check_duplicate_results()
            load_basel_psnl_loan_mth_snapshot = load_basel_psnl_loan_mth_snapshot()

            """ Dependency chaining """
            make_basel_psnl_loan_mth_snapshot >> [
                check_duplicate_basel_acct_id,
                check_duplicate_crnt_br_loctn_trnst_loan_num,
                check_duplicate_trnst_num_loan_num,
            ] >> check_duplicate_results >> load_basel_psnl_loan_mth_snapshot
        sq006_source_group = sq006_source_group()
        sq006_enrichment_group = sq006_enrichment_group()

        sq006_source_group >> sq006_enrichment_group


    @task(outlets=[AssetAlias("basel_step_psnl_loan_mth_snapshot")])
    def basel_step_psnl_loan_mth_snapshot(*, outlet_events):
        outlet_events[AssetAlias("basel_step_psnl_loan_mth_snapshot")].add(
            Asset("ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT", extra={})
        )


    sq006 = sq006_group()
    sq006_start = sq006_start()
    basel_step_psnl_loan_mth_snapshot = basel_step_psnl_loan_mth_snapshot()

    sq006_start >> sq006 >> basel_step_psnl_loan_mth_snapshot
    @task
    def sq007_start():
        """ Manual approval task to start sq007 """
        raise AirflowException("Please mark this task successful to start sequence sq007.")


    @task_group(group_id="sq007")
    def sq007_group():
        """
        TaskGroup for sequence sq007
        """

        @task_group(group_id="sq007_source")
        def sq007_source_group():
            """
            TaskGroup for source tasks from EDL in sequence sq007
            """
            # Import of source_group.py
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
        @task_group(group_id="sq007_enrichment")
        def sq007_enrichment_group():
            """
            TaskGroup for enrichment tasksin sequence sq007
            Currently, IIAS data used to enrich EDL data
            Future, DuckLake / MSSQL data used to enrich EDL data
            """
            # Implementation for enrichment tasks goes here
            @task.parquet(
                task_id="lookup_67",
                duckdb_conn_id="duckdb-conn",
                target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq007/lookup_67.parquet",
                sql="""
                    SELECT tm.tm_id as mth_tm_id, airb.* 
                    FROM 
                        '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq007/airb_revlvng_cr_mth_snapshot/*.parquet' as airb 
                    JOIN
                        ingestion.TM_DIM as tm 
                    ON (airb.mth_end_dt = tm.tm_lvl_end_dt)
                """,
                export_params={},
                clear_before_write=True,
            )
            def lookup_67() -> None:
                """
                This task joins '/bns/rrap/data/<YYYY-MM-DD>/jb0071_airb_revlvng_cr_mth_snapshot/*.parquet' 
                contents with 'jb0072_ODBC_RCRR1_TIM_DIM.parquet' to obtain mth_tm_id column.
                """
                pass


            @task.parquet(
                task_id="join_step_pln",
                duckdb_conn_id="duckdb-conn",
                target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq007/join_step_pln.parquet",
                sql="""
                    SELECT pln.STEP_PLN_SNAPSHOT_ID, l67.*
                    FROM '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq007/lookup_67.parquet' as l67 
                    LEFT OUTER JOIN
                    ingestion.BASEL_STEP_PLN_MTH_SNAPSHOT as pln 
                    USING (STEP_PLN_AGRMNT_NUM)
                """,
                export_params={},
                clear_before_write=True,
            )
            def join_step_pln() -> None:
                """
                This task joins 'lookup_67.parquet' contents with ingestion.BASEL_STEP_PLN_MTH_SNAPSHOT DuckLake table
                on STEP_PLN_AGRMNT_NUM to obtain STEP_PLN_SNAPSHOT_ID column.
                """
                pass


            @task.parquet(
                task_id="join_cust_id",
                duckdb_conn_id="duckdb-conn",
                target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq007/join_cust_id.parquet",
                sql="""
                    SELECT bc.basel_cust_id, js.*
                    FROM 
                        '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq007/join_step_pln.parquet' as js 
                    LEFT JOIN
                        ingestion.BASEL_CUST_DIM as bc 
                    ON 
                        lpad(trim(js.acct_num), 23, '0') = lpad(trim(bc.acct_num), 23, '0')
                """,
                export_params={},
                clear_before_write=True,
            )
            def join_cust_id() -> None:
                """
                This task joins 'join_step_pln.parquet' contents with 'jb0072_BASEL_CUST_DIM.parquet' on trimmed and 
                left padded account numbers in order to obtain basel_cust_id column. 
                Output file is '/bns/rrap/data/<YYYY-MM-DD>/jb0072_Join_cust_id/*.parquet'.
                """
                pass


            @task.parquet(
                task_id="join_acct_num",
                duckdb_conn_id="duckdb-conn",
                target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq007/join_acct_num.parquet",
                sql="""
                    SELECT b.basel_acct_id, j.*
                    FROM '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq007/join_cust_id.parquet' as j 
                    LEFT JOIN
                    ingestion.BASEL_ACCT_DIM as b 
                    ON 
                        lpad(trim(j.acct_num), 23, '0') = lpad(trim(b.acct_num), 23, '0')

                """,
                export_params={},
                clear_before_write=True,
            )
            def join_acct_num() -> None:
                """
                This task joins 'join_cust_id.parquet' contents with 'jb0072_BASEL_ACCT_DIM.parquet' on trimmed 
                and left padded account numbers in order to obtain basel_acct_id column. 
                Output file is '/bns/rrap/data/<YYYY-MM-DD>/jb0072_Join_acct_num/*.parquet'.
                """
                pass


            @task.parquet(
                task_id="join_72",
                duckdb_conn_id="duckdb-conn",
                target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq007/join_72.parquet",
                sql="""
                    SELECT o.org_unit_id, j.*
                    FROM '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq007/join_acct_num.parquet' as j LEFT JOIN
                    ingestion.ORG_UNIT_DIM as o
                    ON (o.trnst_num = j.trnst_num::varchar)
                """,
                export_params={},
                clear_before_write=True,
            )
            def join_72() -> None:
                """
                This task joins 'join_acct_num.parquet' contents with 'jb0072_ORG_UNIT_DIM.parquet' on transit numbers in order 
                to obtain org unit ID column.
                Output file is '/bns/rrap/data/<YYYY-MM-DD>/jb0072_Join_72/*.parquet'.
                """
                pass


            @task.parquet(
                task_id="transformer_58",
                duckdb_conn_id="duckdb-conn",
                target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq007/transformer_58.parquet",
                sql="""
                SELECT
                    last_prch_dt ,
                    last_pymt_dt ,
                    acct_cls_rsn_cd as acct_cls_rsn_cd ,
                    acct_stat_cd as acct_stat_cd ,
                    (anul_fee_cd::int)::text as anul_fee_cd,
                    anul_fee_lnk_f as anul_fee_lnk_f ,
                    bal_hist_csh_adv_amt as bal_hist_csh_adv_amt ,
                    bal_hist_prchs_amt as bal_hist_prchs_amt ,
                    bal_hist_pymts_amt as bal_hist_pymts_amt ,
                    bns_dlqnt_days as bns_dlqnt_day ,
                    chip_f,
                    chrg_off_cd as chrg_off_cd ,
                    cmpgn_notified_cd as cmpgn_notified_cd ,
                    cmpgn_stat_cd as cmpgn_stat_cd ,
                    lpad(colctn_ctr_cd::text, 2, '0') AS colctn_ctr_cd,
                    corp_rtl_f as corp_rtl_f ,
                    cr_lmt_amt as cr_lmt_amt ,
                    cr_score as cr_score ,
                    csh_advnc_1_cycl_ago_bal_amt as csh_advnc_1_cycl_ago_bal_amt ,
                    csh_advnc_2_cycl_ago_bal_amt as csh_advnc_2_cycl_ago_bal_amt ,
                    csh_advnc_crnt_cycl_bal_amt as csh_advnc_crnt_cycl_bal_amt ,
                    csh_advnc_rcvry_intr_amt as csh_advnc_rcvry_intr_amt ,
                    divdd_rbt_last_yr_amt as divdd_rbt_last_yr_amt ,
                    dlqnt_hist_1_12 as dlqnt_hist_1_12 ,
                    dlqnt_hist_13_24 as dlqnt_hist_13_24 ,
                    final_cr_score as final_cr_score ,
                    fscl_ytd_prchs_amt as fncl_ytd_prchs_amt ,
                    fscl_ytd_rtrns_amt as fncl_ytd_rtrns_amt ,
                    full_pymt_cd as full_pymt_cd ,
                    inact_cd as inact_cd ,
                    last_pymt_amt as last_pymt_amt ,
                    last_yr_cr_intr_pd_amt as last_yr_cr_intr_pd_amt ,
                    mdl_cd_3 as mdl_cd_3 ,
                    mdl_cd_4 as mdl_cd_4 ,
                    mdl_citifone as mdl_citifone ,
                    mdl_compaction as mdl_compaction ,
                    mths_dlqnt_cnt as mth_dlqnt_cnt ,
                    non_responder_f as non_responder_f ,
                    oprtr_last_onlne_updt as oprtr_last_online_updt ,
                    orig_chrg_off_amt as orig_chrg_off_amt ,
                    pad_oth_fncl_inst_f as pad_oth_fncl_inst_f ,
                    prch_1_cycl_ago_bal_amt as prch_1_cycl_ago_bal_amt ,
                    prch_2_cycl_ago_bal_amt as prch_2_cycl_ago_bal_amt ,
                    prch_crnt_cycl_bal_amt as prch_crnt_cycl_bal_amt ,
                    prch_rcvry_intr_amt as prch_rcvry_intr_amt ,
                    prd_cd as prd_cd ,
                    prev_sub_prd_cd as prev_sub_prd_cd ,
                    proc_tp_cd as proc_tp_cd ,
                    reqst_cr_lmt_amt as reqst_cr_lmt_amt ,
                    risk_bal_rto as risk_bal_rto ,
                    rtn_envelope as rtn_envelope ,
                    scrd_tp_cd as scrd_tp_cd ,
                    lpad(scrty_tp_cd::text, 2, '0') AS scrty_tp_cd,
                    scrty_val_amt as scrty_val_amt ,
                    lpad(setp_trnst_num::text, 5,'0') as setp_trnst_num,
                    solctn_cd as solctn_cd ,
                    nullif(spsp_ccli, '') as spsp_ccli ,
                    step_pln_agrmnt_num as step_pln_agrmnt_num ,
                    sub_prd_cd as sub_prd_cd ,
                    switch_cd as switch_cd ,
                    tot_cycl_to_dt_fncl_chrg_amt as tot_cycl_to_dt_fncl_chrg_amt ,
                    tot_new_bal_amt as tot_new_bal_amt ,
                    tot_rcvry_intr_amt as tot_rcvry_intr_amt ,
                    tot_unpaid_fncl_chrg_amt as tot_unpaid_fncl_chrg_amt ,
                    tot_ytd_cr_intr_pd_amt as tot_ytd_cr_intr_pd_amt ,
                    lpad(trnst_num::text, 5, '0') as trnst_num,
                    visa_plastic_out as visa_plastic_out ,
                    ytd_csh_advnc_intr_chrgd_amt as ytd_csh_advnc_intr_chrgd_amt ,
                    ytd_csh_advnc_intr_pd as ytd_csh_advnc_intr_pd ,
                    ytd_prchs_cnt as ytd_prchs_cnt ,
                    ytd_prchs_intr_chrgd_amt as ytd_prchs_intr_chrgd_amt ,
                    ytd_prchs_intr_pd_amt as ytd_prchs_intr_pd_amt ,
                    now() as insrt_process_tmstmp ,
                    now() as updt_process_tmstmp ,
                    src_cd::text as src_cd ,            -- comes from transformer_30 as acct1_src from ODBC_rcrr_revlvng_credit_mth_snapshot as an integer (which trims 0s)
                    switch_xref::text as switch_xref ,  -- Also from transformer_30 as acct1_switch_xref from ODBC_rcrr_revlvng_credit_mth_snapshot as an integer
                    rtrim(crnt_bill_cd) as crnt_bill_cd ,
                    ifnull(rtrim(prim_cust_id), '') as prim_cust_cid ,
                    trim(block_recl_cd) as block_recl_cd ,
                    CASE WHEN basel_cust_id IS NULL THEN -1 ELSE  basel_cust_id END as prim_basel_cust_id,
                    CASE WHEN basel_cust_id IS NULL THEN -1 ELSE  basel_cust_id END as basel_cust_id,
                    CASE WHEN step_pln_snapshot_id IS NULL THEN -1 ELSE step_pln_snapshot_id END as step_pln_snapshot_id,
                    CASE WHEN org_unit_id IS NULL THEN -1 ELSE org_unit_id END as trnst_ou_id,
                    CASE WHEN acct_opnd_dt IS NULL THEN '9999-12-31' ELSE acct_opnd_dt END as acct_opnd_dt,
                    CASE WHEN last_acty_dt IS NULL THEN '9999-12-31' ELSE last_acty_dt END as last_acty_dt,
                    CASE WHEN chrg_off_dt IS NULL THEN '9999-12-31' ELSE chrg_off_dt END as chrg_off_dt,
                    CASE WHEN frst_use_dt IS NULL THEN '9999-12-31' ELSE frst_use_dt END as frst_use_dt,
                    CASE WHEN non_accrl_dt IS NULL THEN '9999-12-31' ELSE non_accrl_dt END as non_accrl_dt,
                    CASE WHEN write_off_dt IS NULL THEN '9999-12-31' ELSE write_off_dt END as write_off_dt,
                    CASE WHEN last_blocked_dt IS NULL THEN '9999-12-31' ELSE last_blocked_dt END as last_blocked_dt,
                    CASE WHEN next_rnew_fee_dt IS NULL THEN '9999-12-31' ELSE next_rnew_fee_dt END as next_rnew_fee_dt,
                    CASE WHEN switch_dt IS NULL THEN '9999-12-31' ELSE switch_dt END as switch_dt,
                    CASE WHEN sub_prd_tfr_dt IS NULL THEN '9999-12-31' ELSE sub_prd_tfr_dt END as sub_prd_tfr_dt,
                    CASE WHEN last_onlne_updt_dt IS NULL THEN '9999-12-31' ELSE last_onlne_updt_dt END as last_online_updt_dt,
                    CASE WHEN last_addr_chng_dt IS NULL THEN '9999-12-31' ELSE last_addr_chng_dt END last_addr_chng_dt,
                    crncy_cd,
                    crnt_bill_cd,
                    basel_acct_id,
                    lpad(trim(acct_num), 23, '0') as acct_num, -- no need to lpad since this is now done at the input of each table
                    gl_acct_num,
                    gl_trnst_num,
                    mth_tm_id
                FROM '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq007/join_72.parquet'
                """,
                export_params={},
                clear_before_write=True,
            )
            def transformer_58() -> None:
                """
                This task transforms and renames column names from 'join_72.parquet' and writes them to 
                '/bns/rrap/data/<YYYY-MM-DD>/sq007/transformer_58.parquet'.
                """
                pass


            @task.parquet(
                task_id="rows_requiring_update",
                duckdb_conn_id="duckdb-conn",
                target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq007/rows_requiring_update.parquet",
                sql="""
                WITH prev_month AS (
                    SELECT basel_acct_id, 
                    mth_dlqnt_cnt,
                    bns_dlqnt_day,
                    FROM ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT
                    WHERE mth_tm_id = '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='PREV_MTH_TM_ID') }}'
                ),
                curr_month AS (
                    SELECT basel_acct_id,
                    mth_dlqnt_cnt,
                    bns_dlqnt_day
                    FROM {{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq007/transformer_58.parquet
                    WHERE mth_tm_id = '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='MTH_TM_ID') }}'
                )
                SELECT 
                    curr_month.BASEL_ACCT_ID AS curr_acct_id, 
                    curr_month.MTH_DLQNT_CNT AS curr_mth_dlqnt_cnt, 
                    curr_month.bns_dlqnt_day AS curr_dlqnt_day,
                    prev_month.BASEL_ACCT_ID AS prev_acct_id, 
                    prev_month.MTH_DLQNT_CNT AS prev_mth_dlqnt_cnt, 
                    prev_month.bns_dlqnt_day AS prev_bns_dlqnt_day
                FROM curr_month
                LEFT OUTER JOIN prev_month
                ON curr_month.BASEL_ACCT_ID = prev_month.BASEL_ACCT_ID
                """,
                export_params={},
                clear_before_write=True,
            )
            def rows_requiring_update() -> None:
                """
                This task identifies rows that require updates and writes them to a Parquet file.
                """
                pass


            @task.update(
                task_id="set_mth_dlqnt_cnt",
                duckdb_conn_id="duckdb-conn",
                sql="SET PREV_ACCT_ID = CURR_ACCT_ID, PREV_MTH_DLQNT_CNT = 0 WHERE PREV_MTH_DLQNT_CNT IS NULL",
                source="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq007/rows_requiring_update.parquet",
                target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq007/set_mth_dlqnt_cnt",
                export_params={
                    'PER_THREAD_OUTPUT': 'TRUE',
                    'file_size_bytes':  '100000000',
                },
                clear_before_write=True,
            )
            def set_mth_dlqnt_cnt() -> None:
                """ This task sets previous month's 'month delinquent count' to 0 where it is null. """
                pass


            @task.update(
                task_id="set_net_new_account",
                duckdb_conn_id="duckdb-conn",
                sql="SET CURR_MTH_DLQNT_CNT = PREV_MTH_DLQNT_CNT",
                source="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq007/set_mth_dlqnt_cnt/*.parquet",
                target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq007/set_net_new_account",
                export_params={
                    'PER_THREAD_OUTPUT': 'TRUE',
                    'file_size_bytes':  '100000000',
                },
                clear_before_write=True,
            )
            def set_net_new_account() -> None:
                """ This task sets current month's 'month delinquent count' to previous month's value for all records. """
                pass


            @task.update(
                task_id="update_mth_dlqnt_cnt",
                duckdb_conn_id="duckdb-conn",
                sql="SET CURR_MTH_DLQNT_CNT = PREV_MTH_DLQNT_CNT + 1 where curr_dlqnt_day >= 30",
                source="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq007/set_net_new_account/*.parquet",
                target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq007/update_mth_dlqnt_cnt",
                export_params={
                    'PER_THREAD_OUTPUT': 'TRUE',
                    'file_size_bytes':  '100000000',
                },
                clear_before_write=True,
            )
            def update_mth_dlqnt_cnt() -> None:
                """ 
                This task sets current month's 'month delinquent count' to previous month's value plus 1 
                where current month's 'days delinquent' is >= 30. 
                """
                pass


            @task.parquet(
                task_id="update_basel_revlvng_cr_mth_snapshot",
                duckdb_conn_id="duckdb-conn",
                target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq007/update_basel_revlvng_cr_mth_snapshot",
                sql= f"""
                    SELECT DISTINCT ON (a.BASEL_ACCT_ID) 
                        a.* EXCLUDE(MTH_DLQNT_CNT), 
                        b.curr_mth_dlqnt_cnt MTH_DLQNT_CNT
                    FROM '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq007/transformer_58.parquet' as a
                    JOIN '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq007/update_mth_dlqnt_cnt/*.parquet' as b
                        ON a.BASEL_ACCT_ID = b.curr_acct_id
                    WHERE a.MTH_TM_ID = (SELECT TM_ID FROM '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq007/TM_DIM.parquet')
                """,
                export_params={
                    'PER_THREAD_OUTPUT': 'TRUE',
                    'file_size_bytes':  '100000000',
                },
                clear_before_write=True,
            )
            def update_basel_revlvng_cr_mth_snapshot() -> None:
                """
                This task pulls a distinct list of BASEL_ACCT_IDs and all other columns from 'transformer_58.parquet' 
                along with their corresponding 'delinquent month count' from 'update_mth_dlqnt_cnt'.
                """


            @task.parquet(
                task_id="src_prd_stdnt_loan_lkp",
                duckdb_conn_id="duckdb-conn",
                target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq007/src_prd_stdnt_loan_lkp.parquet",
                sql="""
                    SELECT PRD_SYS_CD, SRC_PRD_CD, SRC_SUB_PRD_CD, BILL_CD_CHAR, EFF_TO_YR_MTH, BASEL_PRD_CD 
                    FROM ingestion.SRC_PRD_STDNT_LOAN_LKP
                    WHERE EFF_TO_YR_MTH = '999912'
                """,
                export_params={},
                clear_before_write=True,
            )
            def src_prd_stdnt_loan_lkp() -> None:
                """
                This task pulls data from ingestion.SRC_PRD_STDNT_LOAN_LKP DuckLake table and writes it to a Parquet file.
                """
                pass


            @task.parquet(
                task_id="sslb_account_ids",
                duckdb_conn_id="duckdb-conn",
                target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq007/sslb_account_ids.parquet",
                sql="""
                    SELECT a.basel_acct_id
                    FROM
                    '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq007/update_basel_revlvng_cr_mth_snapshot/*.parquet' a,
                    '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq007/src_prd_stdnt_loan_lkp.parquet' b
                    WHERE 
                    a.PRD_CD  = trim(b.SRC_PRD_CD)
                    AND a.SUB_PRD_CD = trim(b.SRC_SUB_PRD_CD)
                    AND SUBSTR(trim(a.CRNT_BILL_CD), 3, 1) = trim(b.BILL_CD_CHAR)
                    AND trim(a.PRD_CD) = 'SSL'
                    AND trim(b.BILL_CD_CHAR) = 'B'
                """,
                export_params={},
                clear_before_write=True,
            )
            def sslb_account_ids() -> None:
                """
                This task identifies SSL B accounts by joining 'update_basel_revlvng_cr_mth_snapshot' with 'src_prd_stdnt_loan_lkp' on product code, 
                sub product code and billing code conditions.
                Output is a Parquet file with a list of basel_acct_ids that are SSL B accounts.
                """
                pass


            @task.parquet(
                task_id="non_sslb_accounts",
                duckdb_conn_id="duckdb-conn",
                target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq007/non_sslb_accounts.parquet",
                sql=f"""
                    SELECT a.*
                    FROM '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq007/update_basel_revlvng_cr_mth_snapshot/*.parquet' a
                    LEFT JOIN '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq007/sslb_account_ids.parquet' b 
                    ON a.basel_acct_id = b.basel_acct_id
                    WHERE b.basel_acct_id is null
                """,
                export_params={},
                clear_before_write=True,
            )
            def non_sslb_accounts() -> None:
                """
                This task identifies non SSL B accounts by left joining 'update_basel_revlvng_cr_mth_snapshot' with 'sslb_account_ids' on basel_acct_id and filtering for nulls.
                Output is a Parquet file with records of non SSL B accounts.
                """
                pass


            @task.parquet(
                task_id="sslb_accounts",
                duckdb_conn_id="duckdb-conn",
                target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq007/sslb_accounts.parquet",
                sql=f"""
                    SELECT a.* REPLACE(
                        CASE
                            WHEN CR_LMT_AMT = 0 AND TOT_NEW_BAL_AMT < 0
                            THEN CR_LMT_AMT
                            ELSE TOT_NEW_BAL_AMT
                        END
                        as CR_LMT_AMT
                    )
                    FROM '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq007/update_basel_revlvng_cr_mth_snapshot/*.parquet' a
                    LEFT JOIN '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq007/sslb_account_ids.parquet' b 
                    ON a.basel_acct_id = b.basel_acct_id
                    WHERE b.basel_acct_id is not null
                """,
                export_params={},
                clear_before_write=True,
            )
            def sslb_accounts() -> None:
                """
                This task applies the SSLB patch by setting CR_LMT_AMT to TOT_NEW_BAL_AMT for SSL B accounts where CR_LMT_AMT is 0 and TOT_NEW_BAL_AMT is negative.
                Output is a Parquet file with records of SSL B accounts with the patch applied.
                """
                pass


            @task.parquet(
                task_id="merge_final_output",
                duckdb_conn_id="duckdb-conn",
                target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq007/basel_revlvng_cr_mth_snapshot",
                sql=f"""
                    SELECT * FROM '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq007/sslb_accounts.parquet'
                    UNION ALL
                    SELECT * FROM '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq007/non_sslb_accounts.parquet'
                """,
                export_params={},
                clear_before_write=True,
            )
            def merge_final_output() -> None:
                """
                This task merges the SSL B accounts with the non SSL B accounts to produce the final output Parquet file that will be used for downstream processing.
                Output is a Parquet file with all accounts, where SSL B accounts have the patch applied.
                """
                pass


            @task.duckdb(
                task_id="load_basel_revlvng_cr_mth_snapshot",
                duckdb_conn_id="duckdb-conn",
                sql=f"""
                    INSERT INTO ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT BY NAME
                    SELECT * FROM '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq007/basel_revlvng_cr_mth_snapshot/*.parquet'
                """,
            )
            def load_basel_revlvng_cr_mth_snapshot() -> None:
                """
                This task loads the final Parquet output into the ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT DuckLake table.
                """
                pass


            """ TaskFlow function calls """
            # Original DataStage steps

            lookup_67 = lookup_67()
            join_step_pln = join_step_pln()
            join_cust_id = join_cust_id()
            join_acct_num = join_acct_num()
            join_72 = join_72()
            transformer_58 = transformer_58()
            # TSYS updates
            rows_requiring_update = rows_requiring_update()
            set_mth_dlqnt_cnt = set_mth_dlqnt_cnt()
            set_net_new_account = set_net_new_account()
            update_mth_dlqnt_cnt = update_mth_dlqnt_cnt()
            update_basel_revlvng_cr_mth_snapshot = update_basel_revlvng_cr_mth_snapshot()
            # SSLB patch
            src_prd_stdnt_loan_lkp = src_prd_stdnt_loan_lkp()
            sslb_account_ids = sslb_account_ids()
            non_sslb_accounts = non_sslb_accounts()
            sslb_accounts = sslb_accounts()
            merge_final_output = merge_final_output()
            # Load to DuckLake
            load_basel_revlvng_cr_mth_snapshot = load_basel_revlvng_cr_mth_snapshot()


            """ Dependency chaining """
            lookup_67 >> join_step_pln
            join_step_pln >> join_cust_id
            join_cust_id >> join_acct_num
            join_acct_num >> join_72
            join_72 >> transformer_58
            transformer_58 >> rows_requiring_update 
            rows_requiring_update >> set_mth_dlqnt_cnt 
            set_mth_dlqnt_cnt >> set_net_new_account 
            set_net_new_account >> update_mth_dlqnt_cnt 
            update_mth_dlqnt_cnt >> update_basel_revlvng_cr_mth_snapshot
            update_basel_revlvng_cr_mth_snapshot >> src_prd_stdnt_loan_lkp
            src_prd_stdnt_loan_lkp >> sslb_account_ids
            sslb_account_ids >> [
                non_sslb_accounts,
                sslb_accounts
            ] >> merge_final_output >> load_basel_revlvng_cr_mth_snapshot
        sq007_source_group = sq007_source_group()
        sq007_enrichment_group = sq007_enrichment_group() 

        sq007_source_group >> sq007_enrichment_group


    @task(outlets=[AssetAlias("basel_revlvng_cr_mth_snapshot")])
    def basel_revlvng_cr_mth_snapshot(*, outlet_events):
        outlet_events[AssetAlias("basel_revlvng_cr_mth_snapshot")].add(
            Asset("ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT", extra={})
        )


    sq007_start = sq007_start()
    sq007 = sq007_group()
    basel_revlvng_cr_mth_snapshot = basel_revlvng_cr_mth_snapshot()

    sq007_start >> sq007 >> basel_revlvng_cr_mth_snapshot
    @task
    def sq008_start():
        """ Manual approval task to start sq008 """
        raise AirflowException("Please mark this task successful to start sequence sq008.")


    @task_group(group_id="sq008")
    def sq008_group():
        """
        TaskGroup for sequence sq008.
        """

        @task_group(group_id="sq008_source")
        def sq008_source_group():
            """
            TaskGroup for source tasks from EDL in sequence sq008.
            """
            # Import of source_group.py
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
        @task_group(group_id="sq008_enrichment")
        def sq008_enrichment_group():
            """
            TaskGroup for enrichment tasks in sequence sq008.
            """
            @task.parquet(
                task_id="join_step_pln_snapshot_id",
                duckdb_conn_id="duckdb-conn",
                target="{{ task_instance.xcom_pull(task_ids='handle_month_context', 'RUNDIR') }}/sq008/join_step_pln_snapshot_id.parquet",
                sql="""
                SELECT * EXCLUDE(STEP_PLN_SNAPSHOT_ID),
                (
                    CASE
                        WHEN STEP_PLN_SNAPSHOT_ID IS NULL OR STEP_PLN_SNAPSHOT_ID = 0 THEN -1
                        ELSE STEP_PLN_SNAPSHOT_ID
                    END
                ) AS STEP_PLN_SNAPSHOT_ID
                FROM (
                SELECT j.*, s.STEP_PLN_SNAPSHOT_ID
                FROM '{{ task_instance.xcom_pull(task_ids='handle_month_context', 'RUNDIR') }}/sq008/join_2.parquet' j
                LEFT JOIN ingestion.BASEL_STEP_PLN_MTH_SNAPSHOT s
                ON j.MTH_TM_ID = s.MTH_TM_ID
                AND j.STEP_PLN_AGRMNT_NUM = TRIM(s.STEP_PLN_AGRMNT_NUM)
                )
                """,
                export_params={},
                clear_before_write=True,
            )
            def join_step_pln_snapshot_id():
                pass


            @task.parquet(
                task_id="join_basel_cust_id",
                duckdb_conn_id="duckdb-conn",
                target="{{ task_instance.xcom_pull(task_ids='handle_month_context', 'RUNDIR') }}/sq008/join_basel_cust_id.parquet",
                sql="""
                SELECT a.ACCT_NUM as MORT_NUM, b.BASEL_CUST_ID
                FROM ingestion.BASEL_ACCT_DIM a
                INNER JOIN ingestion.BASEL_CUST_ACCT_RLTNP_SNAPSHOT b
                ON a.BASEL_ACCT_ID = b.BASEL_ACCT_ID
                """,
                export_params={},
                clear_before_write=True,
            )
            def join_basel_cust_id():
                pass


            @task.parquet(
                task_id="join_prim_basel_cust_id",
                duckdb_conn_id="duckdb-conn",
                target="{{ task_instance.xcom_pull(task_ids='handle_month_context', 'RUNDIR') }}/sq008/join_prim_basel_cust_id.parquet",
                sql="""
                SELECT j.*, b.BASEL_CUST_ID as PRIM_BASEL_CUST_ID
                FROM '{{ task_instance.xcom_pull(task_ids='handle_month_context', 'RUNDIR') }}/sq008/join_step_pln_snapshot_id.parquet' j
                LEFT JOIN '{{ task_instance.xcom_pull(task_ids='handle_month_context', 'RUNDIR') }}/sq008/join_basel_cust_id.parquet' b
                ON j.MORT_NUM = b.MORT_NUM
                """,
                export_params={},
                clear_before_write=True,
            )
            def join_prim_basel_cust_id():
                pass


            @task.parquet(
                task_id="join_basel_acct_id",
                duckdb_conn_id="duckdb-conn",
                target="{{ task_instance.xcom_pull(task_ids='handle_month_context', 'RUNDIR') }}/sq008/join_basel_acct_id.parquet",
                sql="""
                SELECT j.*, b.BASEL_ACCT_ID
                FROM '{{ task_instance.xcom_pull(task_ids='handle_month_context', 'RUNDIR') }}/sq008/join_prim_basel_cust_id.parquet' j
                LEFT JOIN ingestion.BASEL_ACCT_DIM b
                ON j.MORT_NUM = b.ACCT_NUM
                """,
                export_params={},
                clear_before_write=True,
            )
            def join_basel_acct_id():
                pass


            @task.parquet(
                task_id="join_serv_br_ou_id",
                duckdb_conn_id="duckdb-conn",
                target="{{ task_instance.xcom_pull(task_ids='handle_month_context', 'RUNDIR') }}/sq008/join_serv_br_ou_id.parquet",
                sql="""
                SELECT * EXCLUDE(SERV_BR_OU_ID),
                (CASE
                    WHEN SERV_BR_OU_ID IS NULL OR SERV_BR_OU_ID = 0 THEN -1
                    ELSE SERV_BR_OU_ID
                END) AS SERV_BR_OU_ID,
                FROM (
                SELECT j.*, o.ORG_UNIT_ID as SERV_BR_OU_ID
                FROM '{{ task_instance.xcom_pull(task_ids='handle_month_context', 'RUNDIR') }}/sq008/join_basel_acct_id.parquet' j
                LEFT JOIN ingestion.ORG_UNIT_DIM o
                ON o.TRNST_NUM = j.SERV_BR_TRNST_NUM
                )
                """,
                export_params={},
                clear_before_write=True,
            )
            def join_serv_br_ou_id():
                pass


            @task.parquet(
                task_id="join_proc_br_ou_id",
                duckdb_conn_id="duckdb-conn",
                target="{{ task_instance.xcom_pull(task_ids='handle_month_context', 'RUNDIR') }}/sq008/join_proc_br_ou_id.parquet",
                sql="""
                SELECT * EXCLUDE(PROC_BR_OU_ID),
                (CASE
                    WHEN PROC_BR_OU_ID IS NULL OR PROC_BR_OU_ID = 0 THEN -1
                    ELSE PROC_BR_OU_ID
                END) AS PROC_BR_OU_ID,
                FROM (
                SELECT j.*, o.ORG_UNIT_ID as PROC_BR_OU_ID
                FROM '{{ task_instance.xcom_pull(task_ids='handle_month_context', 'RUNDIR') }}/sq008/join_serv_br_ou_id.parquet' j
                LEFT JOIN ingestion.ORG_UNIT_DIM o
                ON o.TRNST_NUM = j.PROC_BR_TRNST_NUM
                )
                """,
                export_params={},
                clear_before_write=True,
            )
            def join_proc_br_ou_id():
                pass


            @task.parquet(
                task_id="make_basel_mort_mth_snapshot",
                duckdb_conn_id="duckdb-conn",
                target="{{ task_instance.xcom_pull(task_ids='handle_month_context', 'RUNDIR') }}/sq008/initial_basel_mort_mth_snapshot.parquet",
                sql="""
                SELECT
                    CONCAT(MORT_NUM, MTH_TM_ID) AS BASEL_MORT_MTH_SNAPSHOT_ID,
                    MTH_TM_ID,
                    STEP_PLN_SNAPSHOT_ID,
                    PRIM_BASEL_CUST_ID,
                    BASEL_ACCT_ID,
                    FLOAT_CD,
                    left(cast(MORT_NUM as character(10)), 7) as MORT_NUM,
                    PD_OFF_F,
                    FRCLSR_F,
                    MTH_IN_ARRS_CNT,
                    INSUR_CL_CD,
                    PRPTY_CD,
                    FUND_CD,
                    INTR_ADJ_DT,
                    CRNT_BAL_AMT,
                    SERV_BR_TRNST_NUM,
                    SERV_BR_OU_ID,
                    PROC_BR_TRNST_NUM,
                    PROC_BR_OU_ID,
                    cast(MORT_AUTH_DT as date) as MORT_AUTH_DT,
                    cast(PD_OFF_DT as date) as PD_OFF_DT,
                    cast(CRNT_TERM_MAT_DT as date) as CRNT_TERM_MAT_DT,
                    cast(LAST_RNEW_DT as date) as LAST_RNEW_DT,
                    AUTH_AMT,
                    INTR_DUE_AMT,
                    LND_VAL,
                    TAX_CRNT_BAL_AMT,
                    cast(INTR_ACCR_AMT AS DECIMAL(17,3)) as INTR_ACCR_AMT,
                    TOT_ADVNC_AMT,
                    BRWER_CD,
                    APRSD_LAST_BUILDING_VAL,
                    APRSD_LAND_VAL,
                    APRSD_LAST_LAND_VAL,
                    APRSD_BUILDING_VAL,
                    APRSD_ORIG_LAND_VAL,
                    APRSD_ORIG_BUILDING_VAL,
                    cast(FINAL_ADVNC_DT as date) as FINAL_ADVNC_DT,
                    LIFE_INSUR_CD,
                    cast(WK_FRST_UNPAID_DT as date) as WK_FRST_UNPAID_DT,
                    BUS_SRC_CD,
                    SCRTY_TP_2,
                    FRST_UNPAID_DT,
                    MONTREAL_TRUST_DSBLTY_STAT_CD,
                    SALE_DT_VAL,
                    MRKTING_1_CD,
                    MRKTING_2_CD,
                    MRKTING_3_CD,
                    MRKTING_4_CD,
                    MRKTING_5_CD,
                    CRI_CD,
                    ARI_CD,
                    PVSN_AMT,
                    SCOTIA_TOT_EQTY_PLN_F,
                    cast(LOAN_AUTH_DT as date) as LOAN_AUTH_DT,
                    HLTH_CRSIS_PRTCTN_INSUR_STAT_CD,
                    TOT_SUSP_BAL_AMT,
                    ACCT_TP_CD,
                    PRPTY_PROV_CD,
                    cast(RENEWED_DT as date) as RENEWED_DT,
                    MORT_TERM_MTH,
                    MAX_INTR_RT,
                    YR_5_MTH_6_F,
                    rpad(trim(CUST_ID), 20, ' ') as PRIM_CUST_CID,
                    STEP_PLN_AGRMNT_NUM,
                    current_timestamp as INSRT_PROCESS_TMSTMP,
                    current_timestamp as UPDT_PROCESS_TMSTMP,
                    cast(YTD_PRPY_AMT as DECIMAL(17,3)) as YTD_PRPY_AMT,
                    AMORT_MTH,
                    PRPTY_ADDR,
                    GL_ACCT_NUM,
                    GL_TRNST_NUM,
                    PRD_GRP_CD,
                    UNIT_CNT,
                    COMM_TP
                from '{{ task_instance.xcom_pull(task_ids='handle_month_context', 'RUNDIR') }}/sq008/join_proc_br_ou_id.parquet'
                """,
                export_params={},
                clear_before_write=True,
            )
            def make_basel_mort_mth_snapshot():
                pass


            @task.update(
                task_id="update_basel_mort_mth_snapshot",
                duckdb_conn_id="duckdb-conn",
                target="{{ task_instance.xcom_pull(task_ids='handle_month_context', 'RUNDIR') }}/sq008/basel_mort_mth_snapshot.parquet",
                source="{{ task_instance.xcom_pull(task_ids='handle_month_context', 'RUNDIR') }}/sq008/initial_basel_mort_mth_snapshot.parquet",
                sql="""
                SET
                    FRCLSR_F = (CASE WHEN TRIM(FRCLSR_F) = '' THEN NULL ELSE FRCLSR_F END),
                    PRPTY_CD = (CASE WHEN TRIM(PRPTY_CD) = '' THEN NULL ELSE PRPTY_CD END),
                    PRPTY_PROV_CD = (CASE WHEN TRIM(PRPTY_PROV_CD) = '' THEN NULL ELSE PRPTY_PROV_CD END),
                    SCRTY_TP_2 = (CASE WHEN TRIM(SCRTY_TP_2) = '' THEN NULL ELSE SCRTY_TP_2 END),
                    MONTREAL_TRUST_DSBLTY_STAT_CD = (CASE WHEN TRIM(MONTREAL_TRUST_DSBLTY_STAT_CD) = '' THEN NULL ELSE MONTREAL_TRUST_DSBLTY_STAT_CD END),
                    MRKTING_1_CD = (CASE WHEN TRIM(MRKTING_1_CD) = '' THEN NULL ELSE MRKTING_1_CD END),
                    MRKTING_2_CD = (CASE WHEN TRIM(MRKTING_2_CD) = '' THEN NULL ELSE MRKTING_2_CD END),
                    MRKTING_3_CD = (CASE WHEN TRIM(MRKTING_3_CD) = '' THEN NULL ELSE MRKTING_3_CD END),
                    MRKTING_4_CD = (CASE WHEN TRIM(MRKTING_4_CD) = '' THEN NULL ELSE MRKTING_4_CD END),
                    MRKTING_5_CD = (CASE WHEN TRIM(MRKTING_5_CD) = '' THEN NULL ELSE MRKTING_5_CD END),
                    CRI_CD = COALESCE(CRI_CD, '          '),
                    ARI_CD = COALESCE(ARI_CD, '          '),
                    YR_5_MTH_6_F = (CASE WHEN TRIM(YR_5_MTH_6_F) = '' THEN NULL ELSE YR_5_MTH_6_F END),
                    COMM_TP = RPAD(COMM_TP, 20, ' '),
                    GL_TRNST_NUM = (CASE WHEN TRIM(GL_TRNST_NUM) = '' THEN NULL ELSE GL_TRNST_NUM END)
                WHERE
                    MTH_TM_ID = {{ task_instance.xcom_pull(task_ids='handle_month_context', 'MTH_TM_ID') }}
                """,
                export_params={},
                clear_before_write=True,
            )
            def update_basel_mort_mth_snapshot():
                pass


            @task.duckdb(
                task_id="load_basel_mort_mth_snapshot",
                duckdb_conn_id="duckdb-conn",
                sql="""
                INSERT INTO ingestion.BASEL_MORT_MTH_SNAPSHOT BY NAME
                SELECT * FROM '{{ task_instance.xcom_pull(task_ids='handle_month_context', 'RUNDIR') }}/sq008/basel_mort_mth_snapshot.parquet'
                """,
            )
            def load_basel_mort_mth_snapshot():
                pass


            """ TaskFlow function definitions """
            join_step_pln_snapshot_id = join_step_pln_snapshot_id()
            join_basel_cust_id = join_basel_cust_id()
            join_prim_basel_cust_id = join_prim_basel_cust_id()
            join_basel_acct_id = join_basel_acct_id()
            join_serv_br_ou_id = join_serv_br_ou_id()
            join_proc_br_ou_id = join_proc_br_ou_id()
            make_basel_mort_mth_snapshot = make_basel_mort_mth_snapshot()
            update_basel_mort_mth_snapshot = update_basel_mort_mth_snapshot()
            load_basel_mort_mth_snapshot = load_basel_mort_mth_snapshot()

            """ Dependency chaining """
            [
                join_step_pln_snapshot_id,
                join_basel_cust_id
            ] >> join_prim_basel_cust_id
            join_prim_basel_cust_id >> join_basel_acct_id
            join_basel_acct_id >> join_serv_br_ou_id
            join_serv_br_ou_id >> join_proc_br_ou_id
            join_proc_br_ou_id >> make_basel_mort_mth_snapshot
            make_basel_mort_mth_snapshot >> update_basel_mort_mth_snapshot
            update_basel_mort_mth_snapshot >> load_basel_mort_mth_snapshot
        sq008_source_group = sq008_source_group()
        sq008_enrichment_group = sq008_enrichment_group()

        sq008_source_group >> sq008_enrichment_group


    @task(outlets=[AssetAlias("basel_mort_mth_snapshot")])
    def basel_mort_mth_snapshot(*, outlet_events):
        outlet_events[AssetAlias("basel_mort_mth_snapshot")].add(
            Asset("ingestion.BASEL_MORT_MTH_SNAPSHOT", extra={})
        )


    sq008 = sq008_group()
    sq008_start = sq008_start()
    basel_mort_mth_snapshot = basel_mort_mth_snapshot()

    sq008_start >> sq008 >> basel_mort_mth_snapshot
    @task
    def sq011_start():
        """ Manual approval task to start sq011 """
        raise AirflowException("Please mark this task successful to start sequence sq011.")


    @task_group(group_id="sq011")
    def sq011_group():
        """
        TaskGroup for sequence sq011.
        """

        @task_group(group_id="sq011_source")
        def sq011_source_group():
            """
            TaskGroup for source tasks from EDL in sequence sq011.
            """
            # Import of source_group.py
            @task
            def create_sq011_rundir():
                """Create sq011 run directory."""
                context = get_current_context()
                rundir = context["ti"].xcom_pull(task_ids="handle_month_context", key="RUNDIR")
                sq011_rundir = f"{rundir}/sq011"
                os.makedirs(sq011_rundir, exist_ok=True)


            @task.beeline(
                task_id="get_tng_cpd2_customer_portfolio_summary_1",
                beeline_conn_id="edlr-conn",
                sql="""
                    select
                        customer_id,
                        customer_key,
                        loan_decline_ind,
                        mtg_decline_ind,
                        deposit_accts_cnt,
                        deposit_accts_amt,
                        loan_accts_cnt,
                        loan_accts_amt,
                        max_dep_balance,
                        concat(substr(max_dep_balance_dt, 1, 4), '-',
                            substr(max_dep_balance_dt, 5, 2), '-',
                            substr(max_dep_balance_dt, 7, 2)) AS max_dep_balance_dt,
                        credit_score,
                        concat(substr(credit_score_dt, 1, 4), '-',
                            substr(credit_score_dt, 5, 2), '-',
                            substr(credit_score_dt, 7, 2)) AS credit_score_dt,
                        access_to_funds_amt,
                        high_freq_caller_ind,
                        concat(substr(high_freq_caller_dt, 1, 4), '-',
                            substr(high_freq_caller_dt, 5, 2), '-',
                            substr(high_freq_caller_dt, 7, 2)) AS high_freq_caller_dt,
                        concat(substr(month_end_dt, 1, 4), '-',
                            substr(month_end_dt, 5, 2), '-',
                            substr(month_end_dt, 7, 2)) AS month_end_dt
                    FROM {{ var.value.TSZ_SCHEMA }}.TNG_CPD2_CUSTOMER_PORTFOLIO_SUMMARY_1
                    WHERE customer_key is not NULL
                    AND businesseffectivedate='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="MTH_END_DT") }}';
                """,
                rundir="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq011",
                tmpfileloc="/bns/rrap/data/tmp",
                target="tng_cpd2_customer_portfolio_summary_1.parquet",
                to_parquet=True,
                schema=pa.schema([
                    ('customer_id', pa.string()),
                    ('customer_key', pa.int64()),
                    ('loan_decline_ind', pa.string()),
                    ('mtg_decline_ind', pa.string()),
                    ('deposit_accts_cnt', pa.int64()),
                    ('deposit_accts_amt', pa.float64()),
                    ('loan_accts_cnt', pa.int64()),
                    ('loan_accts_amt', pa.float64()),
                    ('max_dep_balance', pa.float64()),
                    ('max_dep_balance_dt', pa.date64()),
                    ('credit_score', pa.int64()),
                    ('credit_score_dt', pa.date64()),
                    ('access_to_funds_amt', pa.float64()),
                    ('high_freq_caller_ind', pa.string()),
                    ('high_freq_caller_dt', pa.date64()),
                    ('month_end_dt', pa.date64()),
                ]),
            )
            def get_tng_cpd2_customer_portfolio_summary_1():
                """
                Extract Tangerine customer portfolio summary data.
    
                Extracts customer portfolio summary including loan/mortgage decline indicators,
                account counts and amounts, max deposit balance, credit score, caller frequency,
                and month-end date from TNG_CPD2_CUSTOMER_PORTFOLIO_SUMMARY_1 for the specified
                business effective date where customer key is not NULL.
                """
                pass


            rundir_task = create_sq011_rundir()
            extract_task = get_tng_cpd2_customer_portfolio_summary_1()

            rundir_task >> extract_task
        @task_group(group_id="sq011_enrichment")
        def sq011_enrichment_group():
            """
            TaskGroup for enrichment tasks in sequence sq011.
            """
            @task.duckdb(
                task_id="load_tng_cust_mo",
                duckdb_conn_id="duckdb-conn",
                sql="""
                    INSERT INTO ingestion.TNG_CUST_MO BY NAME
                    SELECT * FROM '{{ task_instance.xcom_pull(task_ids='handle_month_context', 'RUNDIR') }}/sq011/tng_cpd2_customer_portfolio_summary_1.parquet'
                """,
            )
            def load_tng_cust_mo():
                pass
        sq011_source_group = sq011_source_group()
        sq011_enrichment_group = sq011_enrichment_group()

        sq011_source_group >> sq011_enrichment_group


    @task(outlets=[AssetAlias("tng_cust_mo")])
    def tng_cust_mo(*, outlet_events):
        outlet_events[AssetAlias("tng_cust_mo")].add(
            Asset("ingestion.TNG_CUST_MO", extra={})
        )


    sq011 = sq011_group()
    sq011_start = sq011_start()
    tng_cust_mo = tng_cust_mo()

    sq011_start >> sq011 >> tng_cust_mo
    @task
    def sq015_start():
        """ Manual approval task to start sq015 """
        raise AirflowException("Please mark this task successful to start sequence sq015.")


    @task_group(group_id="sq015")
    def sq015_group():
        """
        TaskGroup for sequence sq015.
        """

        @task_group(group_id="sq015_source")
        def sq015_source_group():
            """
            TaskGroup for source tasks from EDL in sequence sq015.
            """
            # Import of source_group.py
            @task
            def create_sq015_rundir():
                """Create sq015 run directory."""
                context = get_current_context()
                rundir = context["ti"].xcom_pull(task_ids="handle_month_context", key="RUNDIR")
                sq015_rundir = f"{rundir}/sq015"
                os.makedirs(sq015_rundir, exist_ok=True)


            @task.beeline(
                task_id="get_rcrr_tnif_hierarchy",
                beeline_conn_id="edlr-conn",
                sql="""
                    SELECT
                        LPAD(TRANSIT, 5, '0') as TRNST_NUM,
                        TRANSIT_NM as ORG_UNIT_NM,
                        CITY_NM as CITY,
                        TRANSIT_OPEN_DT as OPN_DT,
                        TRANSIT_CLS_DT as CLS_DT,
                        TEL_NUM as PH_NUM,
                        POSTAL_ZIP_CD,
                        PROVINCE_COUNTRY_NM as PROV_STATE,
                        STRT_NM as STRT_ADDR
                    FROM {{ var.value.RCRR_SCHEMA }}.tnif_hierarchy
                    WHERE crnt_f = 'Y';
                """,
                rundir="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq015",
                to_parquet=True,
                tmpfileloc="/bns/rrap/data/tmp",
                target="rcrr_tnif_hierarchy.parquet",
                schema=pa.schema([
                    ('TRNST_NUM', pa.string()),
                    ('ORG_UNIT_NM', pa.string()),
                    ('CITY', pa.string()),
                    ('OPN_DT', pa.date64()),
                    ('CLS_DT', pa.date64()),
                    ('PH_NUM', pa.string()),
                    ('POSTAL_ZIP_CD', pa.string()),
                    ('PROV_STATE', pa.string()),
                    ('STRT_ADDR', pa.string())
                ]),
            )
            def get_rcrr_tnif_hierarchy():
                """
                Extract organizational unit dimension data.
    
                Extracts transit/organizational unit hierarchy information including
                transit number, name, city, open/close dates, phone number, postal code,
                province/state, and street address from RCRR_TNIF_HIERARCHY for current
                records (crnt_f = 'Y').
                """
                pass


            """Source layer for sq015."""
            rundir_task = create_sq015_rundir()
            extract_task = get_rcrr_tnif_hierarchy()

            rundir_task >> extract_task
        @task_group(group_id="sq015_enrichment")
        def sq015_enrichment_group():
            """
            TaskGroup for enrichment tasks in sequence sq015.
            """
            @task
            def get_max_org_unit_id():
                """ This task gets the max ORG_UNIT_ID from ingestion table to be used for incrementing new ORG_UNIT_ID's. """
                hook = DuckLakeHook(duckdb_conn_id="duckdb-conn")
                result = hook.sql("""
                    SELECT MAX(ORG_UNIT_ID) as max_id
                    FROM ingestion.ORG_UNIT_DIM 
                """)
                return result.to_df()["max_id"][0]


            @task.parquet(
                task_id="join_org_unit_id",
                duckdb_conn_id="duckdb-conn",
                target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq015/join_org_unit_id.parquet",
                sql="""
                    SELECT
                        o.ORG_UNIT_ID,
                        o.ORG_UNIT_LVL,
                        o.INSRT_PROCESS_TMSTMP,
                        rcrr.TRNST_NUM,
                        rcrr.ORG_UNIT_NM,
                        rcrr.CITY,
                        rcrr.OPN_DT,
                        rcrr.CLS_DT,
                        rcrr.PH_NUM,
                        rcrr.POSTAL_ZIP_CD,
                        rcrr.PROV_STATE,
                        rcrr.STRT_ADDR,
                        o.ORG_UNIT_NM as ORG_UNIT_NM_NZ,
                        o.CITY as CITY_NZ,
                        o.OPN_DT as OPN_DT_NZ,
                        o.CLS_DT as CLS_DT_NZ,
                        o.PH_NUM as PH_NUM_NZ,
                        o.POSTAL_ZIP_CD as POSTAL_ZIP_CD_NZ
                    FROM '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq015/rcrr_tnif_hierarchy.parquet' rcrr
                    LEFT OUTER JOIN ingestion.ORG_UNIT_DIM o
                    ON rcrr.TRNST_NUM = o.trnst_num
                """,
                export_params={},
                clear_before_write=True,
            )
            def join_org_unit_id():
                """ 
                This task left outer joins numerous columns from the parquet created in source_group and ingestion.ORG_UNIT_DIM on the transit number column. 
                This data is written to sq015_join_org_unit_id.parquet. 
                """
                pass


            @task.parquet(
                task_id="org_unit_dim_update",
                duckdb_conn_id="duckdb-conn",
                target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq015/org_unit_dim_update.parquet",
                sql="""
                    SELECT
                        ORG_UNIT_ID,
                        ORG_UNIT_LVL,
                        INSRT_PROCESS_TMSTMP,
                        NOW() AS UPDT_PROCESS_TMSTMP,
                        TRNST_NUM,
                        ORG_UNIT_NM,
                        CITY,
                        (CASE WHEN OPN_DT = '0001-01-01' THEN NULL ELSE OPN_DT END) AS OPN_DT,
                        (CASE WHEN CLS_DT = '0001-01-01' THEN NULL ELSE CLS_DT END) AS CLS_DT,
                        trim(replace(PH_NUM, '-', '')) as PH_NUM,
                        POSTAL_ZIP_CD,
                        PROV_STATE,
                        STRT_ADDR
                    FROM '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq015/join_org_unit_id.parquet'
                    WHERE ORG_UNIT_ID IS NOT NULL
                    AND
                        (
                            TRIM(ORG_UNIT_NM) != TRIM(ORG_UNIT_NM_NZ)
                            OR TRIM(CITY) != TRIM(CITY_NZ)
                            OR OPN_DT != OPN_DT_NZ
                            OR CLS_DT != CLS_DT_NZ
                            OR TRIM(PH_NUM) != TRIM(PH_NUM_NZ)
                            OR TRIM(POSTAL_ZIP_CD) != TRIM(POSTAL_ZIP_CD_NZ)
                        )
                    -- Exclude records where both version of the attribute are NULL
                    AND NOT (
                        (ORG_UNIT_NM IS NULL AND ORG_UNIT_NM_NZ IS NULL)
                        AND (CITY IS NULL AND CITY_NZ IS NULL)
                        AND (OPN_DT IS NULL AND OPN_DT_NZ IS NULL)
                        AND (CLS_DT IS NULL AND CLS_DT_NZ IS NULL)
                        AND (PH_NUM IS NULL AND PH_NUM_NZ IS NULL)
                        AND (POSTAL_ZIP_CD IS NULL AND POSTAL_ZIP_CD_NZ IS NULL)
                    )
                """,
                export_params={},
                clear_before_write=True,
            )
            def org_unit_dim_update():
                """ This task extracts numerous updated records from join_org_unit_id.parquet where ORG_UNIT_ID is not NULL and other conditions. 
                This data is written to org_unit_dim_update.parquet. """
                pass


            @task.parquet(
                task_id="org_unit_dim_new",
                duckdb_conn_id="duckdb-conn",
                target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq015/org_unit_dim_new.parquet",
                sql="""
                    SELECT 
                        {{ task_instance.xcom_pull(task_ids='sq015.sq015_enrichment.get_max_org_unit_id') }} + ROW_NUMBER() over (order by TRNST_NUM) as ORG_UNIT_ID,
                        NULL AS ORG_UNIT_LVL,
                        NOW() AS INSRT_PROCESS_TMSTMP,
                        NULL AS UPDT_PROCESS_TMSTMP,
                        TRNST_NUM,
                        ORG_UNIT_NM,
                        CITY,
                        (CASE WHEN OPN_DT = '0001-01-01' THEN NULL ELSE OPN_DT END) AS OPN_DT,
                        (CASE WHEN CLS_DT = '0001-01-01' THEN NULL ELSE CLS_DT END) AS CLS_DT,
                        trim(replace(PH_NUM, '-', '')) as PH_NUM,
                        POSTAL_ZIP_CD,
                        PROV_STATE,
                        STRT_ADDR
                    FROM '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq015/join_org_unit_id.parquet'
                    WHERE ORG_UNIT_ID IS NULL
                """,
                export_params={},
                clear_before_write=True,
            )
            def org_unit_dim_new():
                """ This task extracts new records from join_org_unit_id.parquet where ORG_UNIT_ID is NULL. This data is written to org_unit_dim_new.parquet. """
                pass


            @task.duckdb(
                task_id="delete_where_org_unit_dim_update",
                duckdb_conn_id="duckdb-conn",
                sql="""
                    DELETE FROM ingestion.ORG_UNIT_DIM
                    WHERE TRNST_NUM IN (
                        SELECT TRNST_NUM 
                        FROM '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq015/org_unit_dim_update.parquet'
                    )
                """,
            )
            def delete_where_org_unit_dim_update():
                """ This task deletes records from ingestion.ORG_UNIT_DIM where transit number is in org_unit_dim_update.parquet. 
                This is done to prepare for re-inserting updated records with same ORG_UNIT_ID's. """
                pass


            @task.duckdb(
                task_id="insert_updated_records",
                duckdb_conn_id="duckdb-conn",
                sql="""
                    INSERT INTO ingestion.ORG_UNIT_DIM BY NAME
                    SELECT * FROM '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq015/org_unit_dim_update.parquet'
                """,
            )
            def insert_updated_records():
                """ This task inserts updated records from org_unit_dim_update.parquet into ingestion.ORG_UNIT_DIM. """
                pass


            @task.duckdb(
                task_id="insert_new_records",
                duckdb_conn_id="duckdb-conn",
                sql="""
                    INSERT INTO ingestion.ORG_UNIT_DIM BY NAME
                    SELECT * FROM '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq015/org_unit_dim_new.parquet'
                 """,
            )
            def insert_new_records():
                """ This task inserts new records from org_unit_dim_new.parquet into ingestion.ORG_UNIT_DIM. """
                pass


            """ TaskFlow function definitions """
            get_max_org_unit_id = get_max_org_unit_id()
            join_org_unit_id = join_org_unit_id()
            org_unit_dim_update = org_unit_dim_update()
            org_unit_dim_new = org_unit_dim_new()
            delete_where_org_unit_dim_update = delete_where_org_unit_dim_update()
            insert_updated_records = insert_updated_records()
            insert_new_records = insert_new_records()

            """ Dependency chaining """
            get_max_org_unit_id >> org_unit_dim_new
            join_org_unit_id >> [ 
                org_unit_dim_update,
                org_unit_dim_new
            ]
            org_unit_dim_update >> delete_where_org_unit_dim_update
            delete_where_org_unit_dim_update >> insert_updated_records
            insert_updated_records >> insert_new_records
        sq015_source_group = sq015_source_group()
        sq015_enrichment_group = sq015_enrichment_group()

        sq015_source_group >> sq015_enrichment_group


    @task(outlets=[AssetAlias("org_unit_dim")])
    def org_unit_dim(*, outlet_events):
        outlet_events[AssetAlias("org_unit_dim")].add(
            Asset("ingestion.ORG_UNIT_DIM", extra={})
        )


    sq015 = sq015_group()
    sq015_start = sq015_start()
    org_unit_dim = org_unit_dim()

    sq015_start >> sq015 >> org_unit_dim
    @task
    def sq016_start():
        """ Manual approval task to start sq016 """
        raise AirflowException("Please mark this task successful to start sequence sq016.")


    @task_group(group_id="sq016")
    def sq016_group():
        """
        TaskGroup for sequence sq016.
        """

        @task_group(group_id="sq016_source")
        def sq016_source_group():
            """
            TaskGroup for source tasks from EDL in sequence sq016.
            """
            # Import of source_group.py
            @task
            def create_sq016_rundir():
                """Create sq016 run directory."""
                context = get_current_context()
                rundir = context["ti"].xcom_pull(task_ids="handle_month_context", key="RUNDIR")
                sq016_rundir = f"{rundir}/sq016"
                os.makedirs(sq016_rundir, exist_ok=True)


            @task.beeline(
                task_id="get_rcrr_psnl_loan_subv_mth_snapshot",
                beeline_conn_id="edlr-conn",
                sql="""
                    select
                        acct_num,
                        orig_cab_transit,
                        loan_num,
                        application_num,
                        subvention_ind,
                        region,
                        mth_end_dt
                    FROM {{ var.value.RCRR_SCHEMA }}.ALSCOM_LOAN_SUBV_MTH_SNAPSHOT
                    WHERE mth_end_dt ='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="MTH_END_DT") }}'
                    AND subvention_ind = 'Y';
                """,
                rundir="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq016",
                to_parquet=True,
                tmpfileloc="/bns/rrap/data/tmp",
                target="rcrr_psnl_loan_subv_mth_snapshot.parquet",
                schema=pa.schema([
                    ('acct_num', pa.string()),
                    ('orig_cab_transit', pa.string()),
                    ('loan_num', pa.string()),
                    ('application_num', pa.string()),
                    ('subvention_ind', pa.string()),
                    ('region', pa.string()),
                    ('mth_end_dt', pa.date64()),
                ]),
            )
            def get_rcrr_psnl_loan_subv_mth_snapshot():
                """
                Extract personal loan subvention month-end snapshot.
    
                Extracts account-level information from RCRR's ALSCOM_LOAN_SUBV_MTH_SNAPSHOT
                including account number, transit, loan number, application number, subvention
                indicator, region, and month-end date for subvented loans (subvention_ind = 'Y')
                on the current month-end date.
                """
                pass


            """Source layer for sq016."""
            rundir_task = create_sq016_rundir()
            extract_task = get_rcrr_psnl_loan_subv_mth_snapshot()

            rundir_task >> extract_task
        @task_group(group_id="sq016_enrichment")
        def sq016_enrichment_group():
            """
            TaskGroup for enrichment tasks in sequence sq016.
            """
            @task.parquet(
                task_id="join_1_mth_tm",
                duckdb_conn_id="duckdb-conn",
                target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq016/join_1_mth_tm.parquet",
                sql="""
                SELECT A.* , {{ task_instance.xcom_pull(task_ids='handle_month_context', key='MTH_TM_ID') }} as MTH_TM_ID
                FROM '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq016/rcrr_psnl_loan_subv_mth_snapshot.parquet' A
                """,
                export_params={},
                clear_before_write=True,
            )
            def join_1_mth_tm():
                """ This includes MTH_TM_ID for rcrr_psnl_loan_subv_mth_snapshot.parquet """
                pass


            @task.parquet(
                task_id="join_2_basel_acct_id",
                duckdb_conn_id="duckdb-conn",
                target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq016/join_2_basel_acct_id.parquet",
                sql="""
                SELECT A.basel_acct_id, A.acct_num, B.*    -- note that the join_1 acct_num will show up as acct_num_1
                FROM ingestion.BASEL_ACCT_DIM A
                LEFT JOIN '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq016/join_1_mth_tm.parquet' B
                ON lpad(trim(A.acct_num), 23, '0') = lpad(trim(B.acct_num), 23, '0')
                """,
                export_params={},
                clear_before_write=True,
            )
            def join_2_basel_acct_id():
                """ This joins the basel_acct_dim with the output of join_1_mth_tm to get basel_acct_id for as many records as possible (based on acct_num) """
                pass


            @task.parquet(
                task_id="filter_null_basel_acct_id",
                duckdb_conn_id="duckdb-conn",
                target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq016/filter_null_basel_acct_id.parquet",
                sql="""
                SELECT 
                    (
                        CASE
                            WHEN basel_acct_id IS NULL OR basel_acct_id = 0 THEN -1
                            ELSE basel_acct_id
                        END
                    ) AS basel_acct_id,
                    acct_num,
                    orig_cab_transit,
                    loan_num,
                    application_num,
                    subvention_ind,
                    region,
                    mth_end_dt,
                    mth_tm_id,
                    CURRENT_TIMESTAMP AS insrt_process_tmstmp,
                    CURRENT_TIMESTAMP AS updt_process_tmstmp
                FROM '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq016/join_2_basel_acct_id.parquet'
                WHERE acct_num IS NOT NULL  -- column "acct_num" comes from join_1_mth_tm: null if does not exist in BASEL_ACCT_DIM, so reject these.
                """,
                export_params={},
                clear_before_write=True,
            )
            def filter_null_basel_acct_id():
                """ This filters out records where acct_num doesn't appear in the 'jb0161_RCRR_PSNL_LOAN_SUBV_MTH_SNAPSHOT.parquet' table, 
                and sets basel_acct_id to -1 when it is NULL.
                """


            @task.parquet(
                task_id="get_duplicate_basel_acct_id",
                duckdb_conn_id="duckdb-conn",
                target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq016/dupe_check_basel_acct_id.parquet",
                sql="""
                    SELECT BASEL_ACCT_ID, COUNT(*) as cnt
                    FROM '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq016/basel_psnl_loan_mth_snapshot.parquet'
                    WHERE BASEL_ACCT_ID IS NOT NULL
                    GROUP BY BASEL_ACCT_ID
                    HAVING COUNT(*) > 1
                """,
                export_params={},
                clear_before_write=True,
            )
            def check_duplicate_basel_acct_id():
                pass


            @task.parquet(
                task_id="get_duplicate_acct_num",
                duckdb_conn_id="duckdb-conn",
                target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq016/dupe_check_acct_num.parquet",
                sql="""
                    SELECT ACCT_NUM, COUNT(*) as cnt
                    FROM '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq016/basel_psnl_loan_mth_snapshot.parquet'
                    WHERE ACCT_NUM IS NOT NULL
                    GROUP BY ACCT_NUM
                    HAVING COUNT(*) > 1
                """,
                export_params={},
                clear_before_write=True,
            )
            def check_duplicate_acct_num():
                pass


            @task.parquet(
                task_id="get_duplicate_orig_cab_transit_loan_num",
                duckdb_conn_id="duckdb-conn",
                target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq016/dupe_check_orig_cab_transit_loan_num.parquet",
                sql="""
                    SELECT ORIG_CAB_TRANSIT, LOAN_NUM, COUNT(*) as cnt
                    FROM '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq016/basel_psnl_loan_mth_snapshot.parquet'
                    WHERE ORIG_CAB_TRANSIT IS NOT NULL AND LOAN_NUM IS NOT NULL
                    GROUP BY ORIG_CAB_TRANSIT, LOAN_NUM
                    HAVING COUNT(*) > 1
                """,
                export_params={},
                clear_before_write=True,
            )
            def check_duplicate_orig_cab_transit_loan_num():
                pass


            @task
            def check_duplicate_results():
                """ Check the results of the duplicate check tasks and raise an exception if duplicates are found."""
                hook = DuckLakeHook(duckdb_conn_id="duckdb-conn")
                results = hook.sql("""
                SELECT COUNT(*) as cnt FROM (
                    SELECT * FROM '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq016/dupe_check_basel_acct_id.parquet'
                    UNION ALL
                    SELECT * FROM '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq016/dupe_check_acct_num.parquet'
                    UNION ALL
                    SELECT * FROM '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq016/dupe_check_orig_cab_transit_loan_num.parquet'
                )""")
                count = results.to_df()["cnt"][0]
                if count > 0:
                    raise AirflowException(f"Duplicate records found in duplicate check tasks. Total duplicates: {count}")


            @task.duckdb(
                task_id="load_basel_psnl_ln_subv_mst_snapsht_new",
                duckdb_conn_id="duckdb-conn",
                sql="""
                INSERT INTO ingestion.BASEL_PSNL_LN_SUBV_MST_SNAPSHT_NEW BY NAME
                SELECT * FROM '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq016/filter_null_basel_acct_id.parquet'
                """,
            )
            def load_basel_psnl_ln_subv_mst_snapsht_new():
                """ This loads the final output into the target table ingestion.BASEL_PSNL_LN_SUBV_MST_SNAPSHT_NEW """
                pass


            """ TaskFlow function definitions """
            join_1_mth_tm = join_1_mth_tm()
            join_2_basel_acct_id = join_2_basel_acct_id()
            filter_null_basel_acct_id = filter_null_basel_acct_id()
            check_duplicate_basel_acct_id = check_duplicate_basel_acct_id()
            check_duplicate_acct_num = check_duplicate_acct_num()
            check_duplicate_orig_cab_transit_loan_num = check_duplicate_orig_cab_transit_loan_num()
            check_duplicate_results = check_duplicate_results()
            load_basel_psnl_ln_subv_mst_snapsht_new = load_basel_psnl_ln_subv_mst_snapsht_new()

            """ Dependency chaining """
            join_1_mth_tm >> join_2_basel_acct_id >> filter_null_basel_acct_id
            filter_null_basel_acct_id >> [
                check_duplicate_basel_acct_id,
                check_duplicate_acct_num,
                check_duplicate_orig_cab_transit_loan_num,
            ] >> check_duplicate_results >> load_basel_psnl_ln_subv_mst_snapsht_new
        sq016_source_group = sq016_source_group()
        sq016_enrichment_group = sq016_enrichment_group()

        sq016_source_group >> sq016_enrichment_group


    @task(outlets=[AssetAlias("basel_psnl_ln_subv_mst_snapsht_new")])
    def basel_psnl_ln_subv_mst_snapsht_new(*, outlet_events):
        outlet_events[AssetAlias("basel_psnl_ln_subv_mst_snapsht_new")].add(
            Asset("ingestion.BASEL_PSNL_LN_SUBV_MST_SNAPSHT_NEW", extra={})
        )


    sq016 = sq016_group()
    sq016_start = sq016_start()
    basel_psnl_ln_subv_mst_snapsht_new = basel_psnl_ln_subv_mst_snapsht_new()

    sq016_start >> sq016 >> basel_psnl_ln_subv_mst_snapsht_new
    @task
    def sq018_start():
        """ Manual approval task to start sq018 """
        raise AirflowException("Please mark this task successful to start sequence sq018.")


    @task_group(group_id="sq018")
    def sq018_group():
        """
        TaskGroup for sequence sq018.
        """

        @task_group(group_id="sq018_source")
        def sq018_source_group():
            """
            TaskGroup for source tasks from EDL in sequence sq018.
            """
            # Import of source_group.py
            @task
            def create_sq018_rundir():
                """Create sq018 run directory."""
                context = get_current_context()
                rundir = context["ti"].xcom_pull(task_ids="handle_month_context", key="RUNDIR")
                sq018_rundir = f"{rundir}/sq018"
                os.makedirs(sq018_rundir, exist_ok=True)


            @task.beeline(
                task_id="get_tng_acct_writeoff_snapshot",
                beeline_conn_id="edlr-conn",
                sql="""
                    select
                        mth_end_dt as month_end_dt,
                        mort_num as mtg_num,
                        trim(mort_provider_desc) as provider,
                        insur_type_desc as insurance_type,
                        wof_dt as writeoff_date,
                        abs(wof_amt) as writeoff_amt,
                        insurer_desc,
                        first_deflt_dt as last_default_dt,
                        fraud_ind
                    FROM {{ var.value.RCRR_SCHEMA }}.TNG_ACCT_WOF_SNAPSHOT
                    WHERE src_sys_cd = 'TNG_MTG'
                    AND mth_end_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="MTH_END_DT") }}';
                """,
                rundir="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq018",
                to_parquet=True,
                tmpfileloc="/bns/rrap/data/tmp",
                target="tng_acct_writeoff_snapshot.parquet",
                schema=pa.schema([
                    ('month_end_dt', pa.date64()),
                    ('mtg_num', pa.string()),
                    ('provider', pa.string()),
                    ('insurance_type', pa.string()),
                    ('writeoff_date', pa.date64()),
                    ('writeoff_amt', pa.float64()),
                    ('insurer_desc', pa.string()),
                    ('last_default_dt', pa.date64()),
                    ('fraud_ind', pa.string()),
                ]),
            )
            def get_tng_acct_writeoff_snapshot():
                """
                Extract Tangerine account writeoff snapshot.
    
                Extracts writeoff information including month-end date, mortgage number,
                provider, insurance type, writeoff date/amount, insurer description,
                last default date, and fraud indicator from TNG_ACCT_WOF_SNAPSHOT for
                Tangerine mortgage accounts (src_sys_cd = 'TNG_MTG') on the current month-end.
                """
                pass


            """Source layer for sq018."""
            rundir_task = create_sq018_rundir()
            extract_task = get_tng_acct_writeoff_snapshot()

            rundir_task >> extract_task
        @task_group(group_id="sq018_enrichment")
        def sq018_enrichment_group():
            """
            TaskGroup for enrichment tasks in sequence sq018.
            """
            @task.duckdb(
                task_id="delete_if_exists_tng_acct_writeoff",
                duckdb_conn_id="duckdb-conn",
                sql="""
                    DELETE FROM ingestion.TNG_ACCT_WRITEOFF
                    WHERE MONTH_END_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="MTH_END_DT") }}';
                """,
            )
            def delete_if_exists_tng_acct_writeoff():
                """
                Deletes existing records from ingestion.TNG_ACCT_WRITEOFF for the month end date being processed, if they exist. 
                This is to ensure that if there are any existing records for the month end date being processed, 
                they will be removed before new records are inserted from the parquet file.
                """
                pass


            @task.duckdb(
                task_id="insert_into_tng_acct_writeoff",
                duckdb_conn_id="duckdb-conn",
                sql="""
                    INSERT INTO ingestion.TNG_ACCT_WRITEOFF BY NAME
                    SELECT * FROM '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="RUNDIR") }}/sq018/tng_acct_writeoff_snapshot.parquet';
                """,
            )
            def insert_into_tng_acct_writeoff():
                """
                Inserts records into ingestion.TNG_ACCT_WRITEOFF from the parquet file generated in the enrichment step. 
                The parquet file is expected to be located in the RUNDIR under the sq018 folder and named tng_acct_writeoff_snapshot.parquet.
                """
                pass


            """ TaskFlow function definitions """
            delete_if_exists_tng_acct_writeoff = delete_if_exists_tng_acct_writeoff()
            insert_into_tng_acct_writeoff = insert_into_tng_acct_writeoff()

            """ Dependency chaining """
            delete_if_exists_tng_acct_writeoff >> insert_into_tng_acct_writeoff
        sq018_source_group = sq018_source_group()
        sq018_enrichment_group = sq018_enrichment_group()

        sq018_source_group >> sq018_enrichment_group


    @task(outlets=[AssetAlias("tng_acct_writeoff")])
    def tng_acct_writeoff(*, outlet_events):
        outlet_events[AssetAlias("tng_acct_writeoff")].add(
            Asset("ingestion.TNG_ACCT_WRITEOFF", extra={})
        )


    sq018 = sq018_group()
    sq018_start = sq018_start()
    tng_acct_writeoff = tng_acct_writeoff()

    sq018_start >> sq018 >> tng_acct_writeoff
    @task
    def sq019_start():
        """ Manual approval task to start sq019 """
        raise AirflowException("Please mark this task successful to start sequence sq019.")


    @task_group(group_id="sq019")
    def sq019_group():
        """
        TaskGroup for sequence sq019.
        """

        @task_group(group_id="sq019_source")
        def sq019_source_group():
            """
            TaskGroup for source tasks from EDL in sequence sq019.
            """
            # Import of source_group.py
            @task
            def create_sq019_rundir():
                """Create sq019 run directory."""
                context = get_current_context()
                rundir = context["ti"].xcom_pull(task_ids="handle_month_context", key="RUNDIR")
                sq019_rundir = f"{rundir}/sq019"
                os.makedirs(sq019_rundir, exist_ok=True)


            @task.beeline(
                task_id="get_tng_cpd3_trans_union_data_1",
                beeline_conn_id="edlr-conn",
                sql="""
                    SELECT
                        date_format(current_timestamp, 'yyyy-MM-dd HH:mm:ss.SSSSSS') AS INSRT_PROCESS_TMSTMP,
                        '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="MTH_END_DT") }}' as MONTH_END_DT,
                        ACCOUNT_ID ,
                        date_format(from_unixtime(unix_timestamp(eff_from_dt, 'yyyyMMdd')), 'yyyy-MM-dd') as eff_from_dt,
                        CUSTOMER_ID ,
                        ACCOUNT_KEY ,
                        CUSTOMER_KEY ,
                        MTG_PROVIDER_KEY ,
                        date_format(from_unixtime(unix_timestamp(eff_to_dt, 'yyyyMMdd')), 'yyyy-MM-dd') as eff_to_dt,
                        CURRENT_IND ,
                        TRADES_CNT,
                        TRADES_ACTIVE_CNT,
                        TRADES_STSFCT_CNT,
                        TRADES_OPENED_3M_CNT,
                        TRADES_OPENED_6M_CNT,
                        TRADES_OPENED_12M_CNT,
                        TRADES_OPENED_18M_CNT,
                        TRADES_OPENED_24M_CNT,
                        TRADES_REVISED_3M_CNT,
                        TRADES_REVISED_6M_CNT,
                        TRADES_REVISED_12M_CNT,
                        TRADES_REVISED_18M_CNT,
                        TRADES_REVISED_24M_CNT,
                        HIT_NOHIT_EDIT_REJECT_IND ,
                        TRADES_WORSE_DPD30_CNT,
                        TRADES_WORSE_DPD60_CNT,
                        TRADES_WORSE_DPD90_CNT,
                        TRADES_ACTIVE_PAST_DUE_CNT,
                        OCCUR_DPD60_12M_CNT,
                        OLDEST_TRADE_MONTHS_CNT,
                        MOST_RECENT_TRADE_MONTHS_CNT,
                        TRADES_OPENED_12M_BAL_CNT,
                        TRADES_STSFCT_3M_CNT,
                        TRADES_STSFCT_6M_CNT,
                        TRADES_STSFCT_12M_CNT,
                        TRADES_STSFCT_18M_CNT,
                        TRADES_STSFCT_24M_CNT,
                        TOTAL_HC_CL,
                        TRADES_BAL_CNT,
                        TRADES_NO_BAL_CNT,
                        REPOSSESSIONS_CNT,
                        CREDIT_COUNSELLING_CNT,
                        TRADES_TOT_BALANCE_AMT,
                        TRADES_TOT_BAL_HC_CL_RATIO,
                        TRADES_AVG_BALANCE_AMT,
                        LATEST_DELNQ_MON_CNT,
                        TRADES_BAL_5000_CNT,
                        TRADES_BAL_1000_CNT,
                        TRADES_BAL_500_CNT,
                        TRADES_BAL_0_CNT,
                        DPD30_12M_CNT,
                        DPD60_12M_CNT,
                        DPD90_12M_CNT,
                        DPD60_EVER_CNT,
                        DPD90_EVER_CNT,
                        CAST(TOT_MONTHLY_PYMTS AS INTEGER) TOT_MONTHLY_PYMTS,
                        HIGHEST_OUTSTND_BAL,
                        HIGHEST_HC_CL_AMT,
                        TRADES_ACTIVE_30D_RATING_CNT,
                        TRADES_ACTIVE_60D_RATING_CNT,
                        TRADES_ACTIVE_90D_RATING_CNT,
                        CAST(OUTSTND_BAL_30D_RATING AS INTEGER) OUTSTND_BAL_30D_RATING,
                        CAST(OUTSTND_BAL_60D_RATING AS INTEGER) OUTSTND_BAL_60D_RATING,
                        CAST(OUTSTND_BAL_90D_RATING AS INTEGER) OUTSTND_BAL_90D_RATING,
                        CAST(HC_CL_30D_RATING AS INTEGER) HC_CL_30D_RATING,
                        CAST(HC_CL_60D_RATING AS INTEGER) HC_CL_60D_RATING,
                        CAST(HC_CL_90D_RATING AS INTEGER) HC_CL_90D_RATING,
                        TRADES_DPD30_12M_CNT,
                        TRADES_DPD60_12M_CNT,
                        TRADES_DPD90_12M_CNT,
                        TRADES_DPD30_6M_CNT,
                        TRADES_DPD60_6M_CNT,
                        TRADES_DPD90_6M_CNT,
                        TRADES_DPD30_CNT,
                        TRADES_DPD60_CNT,
                        TRADES_DPD90_CNT,
                        TRADES_DPD30_24M_CNT,
                        TRADES_DPD60_24M_CNT,
                        TRADES_DPD90_24M_CNT,
                        TOT_PAST_DUE_AMT,
                        LAST_30D_DELNQ_MONTHS_CNT,
                        LAST_60D_DELNQ_MONTHS_CNT,
                        LAST_90D_DELNQ_MONTHS_CNT,
                        TRADES_IN_COLLECT_EVER_CNT,
                        TRADES_IN_COLLECT_EVER_AMT,
                        TRADES_120D_RATING_CNT,
                        TRADES_OPEN_CNT,
                        TRADES_OPEN_AVG_BAL_AMT,
                        OLDEST_TRADE_LINE_AGE_MONTHS,
                        AVAIL_CREDIT_NOT_UTLZD_AMT,
                        TRADES_90D_DPD_36M_CNT,
                        TRADES_NEGATIVE_CNT,
                        LAST_ACTIVITY_MONTHS_CNT,
                        TRADES_PAST_DUE_3M_BAL25_CNT,
                        TRADES_PAST_DUE_3M_BAL_CNT,
                        DPD60_36M_CNT,
                        DPD90_36M_CNT,
                        TRADES_DISPUTED_CNT,
                        TRADES_R9_BAL500_INST_REVL_CNT,
                        TRADES_R8_BAL200_INST_REVL_CNT,
                        TRADES_R7_BAL200_INST_REVL_CNT,
                        TRADES_R5_BAL200_INST_REVL_CNT,
                        TRADES_R4_BAL200_INST_REVL_CNT,
                        TRADES_R3_BAL200_INST_REVL_CNT,
                        TRADES_R2_BAL200_INST_REVL_CNT,
                        TRADES_R3_6M_CNT,
                        TRADES_R4_WITHIN_60M_CNT,
                        TRADES_R4_GREATER_60M_CNT,
                        TRADES_R3_WITHIN_60M_CNT,
                        TRADES_R3_GREATER_60M_CNT,
                        TRADES_R7_WITHIN_60M_CNT,
                        TRADES_R7_GREATER_60M_CNT,
                        TRADES_OPEN_R9_BAL500_CNT,
                        TRADES_OPEN_OPENED_12M_BAL_CNT,
                        TRADES_STSFCT_2_DPD30_CNT,
                        TRADES_PYMNT_NOT_MON_CNT,
                        TRADES_CLOSED_6M_CNT,
                        TRADES_OPEN_UTIL_0_5_CNT,
                        TRADES_OPEN_UTIL_5_50_CNT,
                        TRADES_OPEN_UTIL_50_70_CNT,
                        TRADES_OPEN_UTIL_70_90_CNT,
                        TRADES_OPEN_UTIL_90_CNT,
                        TRADES_ACTIVE_TOT_BAL_AMT,
                        TRADES_ACTIVE_TOT_HC_CL_AMT,
                        TRADES_ACTIVE_UTIL_PCT,
                        TRADES_R4_36M_BAL250_CNT,
                        BANKRUPTCIES_CNT,
                        JUDGEMENTS_CNT,
                        JUDGEMENTS_AMT,
                        COLLECTIONS_CNT,
                        DEROGATORY_PUBLIC_REC_CNT,
                        COLLECTIONS_60M_CNT,
                        COLLECTIONS_OLDER_60M_CNT,
                        CREDIT_BUREAU_WORST_RATING,
                        TRADES_NEVER_DELNQ_PCT,
                        TRADES_DPD90_24M_2_TIMES_CNT,
                        TRADES_DPD60_BAL100_CNT,
                        TOTAL_INCOME,
                        HK_POTENTIAL_IND ,
                        HK_CONFIRMED_IND ,
                        HK_COMPROMISED_IND ,
                        ACCT_NUM
                    FROM {{ var.value.TSZ_SCHEMA }}.tng_cpd3_trans_union_data_1
                    WHERE date_format(from_unixtime(unix_timestamp(month_end_dt, 'yyyyMMdd')), 'yyyy-MM-dd') = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="MTH_END_DT") }}';
                """,
                rundir="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq019",
                to_parquet=True,
                tmpfileloc="/bns/rrap/data/tmp",
                target="tng_cust_tu_snapshot.parquet",
                schema=pa.schema([
                    ('INSRT_PROCESS_TMSTMP', pa.string()),
                    ('MONTH_END_DT', pa.string()),
                    ('ACCOUNT_ID', pa.string()),
                    ('EFF_FROM_DT', pa.string()),
                    ('CUSTOMER_ID', pa.string()),
                    ('ACCOUNT_KEY', pa.int64()),
                    ('CUSTOMER_KEY', pa.int64()),
                    ('MTG_PROVIDER_KEY', pa.int64()),
                    ('EFF_TO_DT', pa.string()),
                    ('CURRENT_IND', pa.string()),
                    ('TRADES_CNT', pa.int64()),
                    ('TRADES_ACTIVE_CNT', pa.int64()),
                    ('TRADES_STSFCT_CNT', pa.int64()),
                    ('TRADES_OPENED_3M_CNT', pa.int64()),
                    ('TRADES_OPENED_6M_CNT', pa.int64()),
                    ('TRADES_OPENED_12M_CNT', pa.int64()),
                    ('TRADES_OPENED_18M_CNT', pa.int64()),
                    ('TRADES_OPENED_24M_CNT', pa.int64()),
                    ('TRADES_REVISED_3M_CNT', pa.int64()),
                    ('TRADES_REVISED_6M_CNT', pa.int64()),
                    ('TRADES_REVISED_12M_CNT', pa.int64()),
                    ('TRADES_REVISED_18M_CNT', pa.int64()),
                    ('TRADES_REVISED_24M_CNT', pa.int64()),
                    ('HIT_NOHIT_EDIT_REJECT_IND', pa.string()),
                    ('TRADES_WORSE_DPD30_CNT', pa.int64()),
                    ('TRADES_WORSE_DPD60_CNT', pa.int64()),
                    ('TRADES_WORSE_DPD90_CNT', pa.int64()),
                    ('TRADES_ACTIVE_PAST_DUE_CNT', pa.int64()),
                    ('OCCUR_DPD60_12M_CNT', pa.int64()),
                    ('OLDEST_TRADE_MONTHS_CNT', pa.int64()),
                    ('MOST_RECENT_TRADE_MONTHS_CNT', pa.int64()),
                    ('TRADES_OPENED_12M_BAL_CNT', pa.int64()),
                    ('TRADES_STSFCT_3M_CNT', pa.int64()),
                    ('TRADES_STSFCT_6M_CNT', pa.int64()),
                    ('TRADES_STSFCT_12M_CNT', pa.int64()),
                    ('TRADES_STSFCT_18M_CNT', pa.int64()),
                    ('TRADES_STSFCT_24M_CNT', pa.int64()),
                    ('TOTAL_HC_CL', pa.int64()),
                    ('TRADES_BAL_CNT', pa.int64()),
                    ('TRADES_NO_BAL_CNT', pa.int64()),
                    ('REPOSSESSIONS_CNT', pa.int64()),
                    ('CREDIT_COUNSELLING_CNT', pa.int64()),
                    ('TRADES_TOT_BALANCE_AMT', pa.float64()),
                    ('TRADES_TOT_BAL_HC_CL_RATIO', pa.float64()),
                    ('TRADES_AVG_BALANCE_AMT', pa.float64()),
                    ('LATEST_DELNQ_MON_CNT', pa.int64()),
                    ('TRADES_BAL_5000_CNT', pa.int64()),
                    ('TRADES_BAL_1000_CNT', pa.int64()),
                    ('TRADES_BAL_500_CNT', pa.int64()),
                    ('TRADES_BAL_0_CNT', pa.int64()),
                    ('DPD30_12M_CNT', pa.int64()),
                    ('DPD60_12M_CNT', pa.int64()),
                    ('DPD90_12M_CNT', pa.int64()),
                    ('DPD60_EVER_CNT', pa.int64()),
                    ('DPD90_EVER_CNT', pa.int64()),
                    ('TOT_MONTHLY_PYMTS', pa.int64()),
                    ('HIGHEST_OUTSTND_BAL', pa.float64()),
                    ('HIGHEST_HC_CL_AMT', pa.float64()),
                    ('TRADES_ACTIVE_30D_RATING_CNT', pa.int64()),
                    ('TRADES_ACTIVE_60D_RATING_CNT', pa.int64()),
                    ('TRADES_ACTIVE_90D_RATING_CNT', pa.int64()),
                    ('OUTSTND_BAL_30D_RATING', pa.int64()),
                    ('OUTSTND_BAL_60D_RATING', pa.int64()),
                    ('OUTSTND_BAL_90D_RATING', pa.int64()),
                    ('HC_CL_30D_RATING', pa.int64()),
                    ('HC_CL_60D_RATING', pa.int64()),
                    ('HC_CL_90D_RATING', pa.int64()),
                    ('TRADES_DPD30_12M_CNT', pa.int64()),
                    ('TRADES_DPD60_12M_CNT', pa.int64()),
                    ('TRADES_DPD90_12M_CNT', pa.int64()),
                    ('TRADES_DPD30_6M_CNT', pa.int64()),
                    ('TRADES_DPD60_6M_CNT', pa.int64()),
                    ('TRADES_DPD90_6M_CNT', pa.int64()),
                    ('TRADES_DPD30_CNT', pa.int64()),
                    ('TRADES_DPD60_CNT', pa.int64()),
                    ('TRADES_DPD90_CNT', pa.int64()),
                    ('TRADES_DPD30_24M_CNT', pa.int64()),
                    ('TRADES_DPD60_24M_CNT', pa.int64()),
                    ('TRADES_DPD90_24M_CNT', pa.int64()),
                    ('TOT_PAST_DUE_AMT', pa.float64()),
                    ('LAST_30D_DELNQ_MONTHS_CNT', pa.int64()),
                    ('LAST_60D_DELNQ_MONTHS_CNT', pa.int64()),
                    ('LAST_90D_DELNQ_MONTHS_CNT', pa.int64()),
                    ('TRADES_IN_COLLECT_EVER_CNT', pa.int64()),
                    ('TRADES_IN_COLLECT_EVER_AMT', pa.float64()),
                    ('TRADES_120D_RATING_CNT', pa.int64()),
                    ('TRADES_OPEN_CNT', pa.int64()),
                    ('TRADES_OPEN_AVG_BAL_AMT', pa.float64()),
                    ('OLDEST_TRADE_LINE_AGE_MONTHS', pa.int64()),
                    ('AVAIL_CREDIT_NOT_UTLZD_AMT', pa.float64()),
                    ('TRADES_90D_DPD_36M_CNT', pa.int64()),
                    ('TRADES_NEGATIVE_CNT', pa.int64()),
                    ('LAST_ACTIVITY_MONTHS_CNT', pa.int64()),
                    ('TRADES_PAST_DUE_3M_BAL25_CNT', pa.int64()),
                    ('TRADES_PAST_DUE_3M_BAL_CNT', pa.int64()),
                    ('DPD60_36M_CNT', pa.int64()),
                    ('DPD90_36M_CNT', pa.int64()),
                    ('TRADES_DISPUTED_CNT', pa.int64()),
                    ('TRADES_R9_BAL500_INST_REVL_CNT', pa.int64()),
                    ('TRADES_R8_BAL200_INST_REVL_CNT', pa.int64()),
                    ('TRADES_R7_BAL200_INST_REVL_CNT', pa.int64()),
                    ('TRADES_R5_BAL200_INST_REVL_CNT', pa.int64()),
                    ('TRADES_R4_BAL200_INST_REVL_CNT', pa.int64()),
                    ('TRADES_R3_BAL200_INST_REVL_CNT', pa.int64()),
                    ('TRADES_R2_BAL200_INST_REVL_CNT', pa.int64()),
                    ('TRADES_R3_6M_CNT', pa.int64()),
                    ('TRADES_R4_WITHIN_60M_CNT', pa.int64()),
                    ('TRADES_R4_GREATER_60M_CNT', pa.int64()),
                    ('TRADES_R3_WITHIN_60M_CNT', pa.int64()),
                    ('TRADES_R3_GREATER_60M_CNT', pa.int64()),
                    ('TRADES_R7_WITHIN_60M_CNT', pa.int64()),
                    ('TRADES_R7_GREATER_60M_CNT', pa.int64()),
                    ('TRADES_OPEN_R9_BAL500_CNT', pa.int64()),
                    ('TRADES_OPEN_OPENED_12M_BAL_CNT', pa.int64()),
                    ('TRADES_STSFCT_2_DPD30_CNT', pa.int64()),
                    ('TRADES_PYMNT_NOT_MON_CNT', pa.int64()),
                    ('TRADES_CLOSED_6M_CNT', pa.int64()),
                    ('TRADES_OPEN_UTIL_0_5_CNT', pa.int64()),
                    ('TRADES_OPEN_UTIL_5_50_CNT', pa.int64()),
                    ('TRADES_OPEN_UTIL_50_70_CNT', pa.int64()),
                    ('TRADES_OPEN_UTIL_70_90_CNT', pa.int64()),
                    ('TRADES_OPEN_UTIL_90_CNT', pa.int64()),
                    ('TRADES_ACTIVE_TOT_BAL_AMT', pa.float64()),
                    ('TRADES_ACTIVE_TOT_HC_CL_AMT', pa.float64()),
                    ('TRADES_ACTIVE_UTIL_PCT', pa.float64()),
                    ('TRADES_R4_36M_BAL250_CNT', pa.int64()),
                    ('BANKRUPTCIES_CNT', pa.int64()),
                    ('JUDGEMENTS_CNT', pa.int64()),
                    ('JUDGEMENTS_AMT', pa.float64()),
                    ('COLLECTIONS_CNT', pa.int64()),
                    ('DEROGATORY_PUBLIC_REC_CNT', pa.int64()),
                    ('COLLECTIONS_60M_CNT', pa.int64()),
                    ('COLLECTIONS_OLDER_60M_CNT', pa.int64()),
                    ('CREDIT_BUREAU_WORST_RATING', pa.int64()),
                    ('TRADES_NEVER_DELNQ_PCT', pa.float64()),
                    ('TRADES_DPD90_24M_2_TIMES_CNT', pa.int64()),
                    ('TRADES_DPD60_BAL100_CNT', pa.int64()),
                    ('TOTAL_INCOME', pa.float64()),
                    ('HK_POTENTIAL_IND', pa.string()),
                    ('HK_CONFIRMED_IND', pa.string()),
                    ('HK_COMPROMISED_IND', pa.string()),
                    ('ACCT_NUM', pa.string())
                ]),
            )
            def get_tng_cpd3_trans_union_data_1():
                """
                Extract Tangerine credit union trade data.
    
                Extracts comprehensive credit union trade information including timestamp,
                account/customer/provider keys, trade counts (active, satisfied, opened/revised
                in various timeframes), delinquency metrics (DPD30/60/90 in various periods),
                balance information, credit ratings, income, and fraud indicators from
                TNG_CPD3_TRANS_UNION_DATA_1 for the current month-end.
                """
                pass


            """Source layer for sq019."""
            rundir_task = create_sq019_rundir()
            extract_task = get_tng_cpd3_trans_union_data_1()

            rundir_task >> extract_task
        @task_group(group_id="sq019_enrichment")
        def sq019_enrichment_group():
            """
            TaskGroup for enrichment tasks in sequence sq019.
            """
            @task.duckdb(
                task_id="delete_if_exists_tng_cust_tu",
                duckdb_conn_id="duckdb-conn",
                sql="""
                    DELETE FROM ingestion.TNG_CUST_TU
                    WHERE MONTH_END_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="MTH_END_DT") }}';
                """,
            )
            def delete_if_exists_tng_cust_tu():
                """
                Deletes existing records from ingestion.TNG_CUST_TU for the month end date being processed, if they exist. 
                This is to ensure that if there are any existing records for the month end date being processed, 
                they will be removed before new records are inserted from the parquet file.
                """
                pass


            @task.duckdb(
                task_id="insert_into_tng_cust_tu",
                duckdb_conn_id="duckdb-conn",
                sql="""
                    INSERT INTO ingestion.TNG_CUST_TU BY NAME
                    SELECT * FROM '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="RUNDIR") }}/sq019/tng_cust_tu_snapshot.parquet';
                """,
            )
            def insert_into_tng_cust_tu():
                """
                Inserts records into ingestion.TNG_CUST_TU from the parquet file generated in the enrichment step. 
                The parquet file is expected to be located in the RUNDIR under the sq019 folder and named tng_cust_tu_snapshot.parquet.
                """
                pass


            """ TaskFlow function definitions """
            delete_if_exists_tng_cust_tu = delete_if_exists_tng_cust_tu()
            insert_into_tng_cust_tu = insert_into_tng_cust_tu()

            """ Dependency chaining """
            delete_if_exists_tng_cust_tu >> insert_into_tng_cust_tu
        sq019_source_group = sq019_source_group()
        sq019_enrichment_group = sq019_enrichment_group()

        sq019_source_group >> sq019_enrichment_group


    @task(outlets=[AssetAlias("tng_cust_tu")])
    def tng_cust_tu(*, outlet_events):
        outlet_events[AssetAlias("tng_cust_tu")].add(
            Asset("ingestion.TNG_CUST_TU", extra={})
        )


    sq019 = sq019_group()
    sq019_start = sq019_start()
    tng_cust_tu = tng_cust_tu()

    sq019_start >> sq019 >> tng_cust_tu
    @task
    def sq020_start():
        """ Manual approval task to start sq020 """
        raise AirflowException("Please mark this task successful to start sequence sq020.")


    @task_group(group_id="sq020")
    def sq020_group():
        """
        TaskGroup for sequence sq020.
        """

        @task_group(group_id="sq020_source")
        def sq020_source_group():
            """
            TaskGroup for source tasks from EDL in sequence sq020.
            """
            # Import of source_group.py
            @task
            def create_sq020_rundir():
                """Create sq020 run directory."""
                context = get_current_context()
                rundir = context["ti"].xcom_pull(task_ids="handle_month_context", key="RUNDIR")
                sq020_rundir = f"{rundir}/sq020"
                os.makedirs(sq020_rundir, exist_ok=True)


            @task
            def compute_prior_year():
                """Compute prior year from month-end date."""
                context = get_current_context()
                mth_end_dt = context["ti"].xcom_pull(task_ids="handle_month_context", key="MTH_END_DT")
                current_year = datetime.strptime(mth_end_dt, '%Y-%m-%d').year - 1
                return current_year


            @task.beeline(
                task_id="get_tng_cpd10_indirect_cost",
                beeline_conn_id="edlr-conn",
                sql="""
                    select
                        year,
                        cost_type,
                        cost_amount as amount
                    FROM {{ var.value.TSZ_SCHEMA }}.TNG_CPD10_INDIRECT_COST
                    WHERE year = CAST('{{ task_instance.xcom_pull(task_ids="sq020_source.compute_prior_year") }}' AS INTEGER) 
                    AND cost_type NOT IN ('Cost per account', 'Defaulted accounts');
                """,
                rundir="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq020",
                to_parquet=True,
                tmpfileloc="/bns/rrap/data/tmp",
                target="get_tng_cpd10_indirect_cost.parquet",
                schema=pa.schema([
                    ('year', pa.int64()),
                    ('cost_type', pa.string()),
                    ('amount', pa.float64())
                ]),
            )
            def get_tng_cpd10_indirect_cost():
                """
                Extract Tangerine indirect cost data.
    
                Extracts year, cost type, and cost amount from TNG_CPD10_INDIRECT_COST
                for the year prior to current month-end, excluding 'Cost per account' and
                'Defaulted accounts' cost types which are recalculated separately.
                """
                pass


            @task.beeline(
                task_id="get_rcrr_tng_mort_acct_mth_snapshot",
                beeline_conn_id="edlr-conn",
                sql="""
                    select
                        mth_end_dt,
                        day_arrs_cnt,
                        acct_id
                    FROM {{ var.value.RCRR_SCHEMA }}.tng_mort_acct_mth_snapshot
                    WHERE day_arrs_cnt > 0 
                    AND mth_end_dt = CONCAT(CAST('{{ task_instance.xcom_pull(task_ids="sq020_source.compute_prior_year") }}' AS STRING), '-01-31');
                """,
                rundir="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq020",
                to_parquet=True,
                tmpfileloc="/bns/rrap/data/tmp",
                target="get_rcrr_tng_mort_acct_mth_snapshot.parquet",
                schema=pa.schema([
                    ('mth_end_dt', pa.date64()),
                    ('day_arrs_cnt', pa.int64()),
                    ('acct_id', pa.string())
                ]),
            )
            def get_rcrr_tng_mort_acct_mth_snapshot():
                """
                Extract Tangerine mortgage account delinquency snapshot.
    
                Extracts account IDs and days in arrears from TNG_MORT_ACCT_MTH_SNAPSHOT
                for January 31st of the year prior to current month-end, where days in
                arrears count is greater than 0.
                """
                pass


            @task.parquet(
                task_id="get_new_estimated_costs",
                duckdb_conn_id="duckdb-conn",
                export_params={},
                clear_before_write=True,
                sql="""
                    SELECT '{{ task_instance.xcom_pull(task_ids="sq020_source.compute_prior_year") }}' as year, 
                    'Estimated costs' as cost_type, 
                    SUM(amount) as amount 
                    FROM '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="RUNDIR") }}/sq020/get_tng_cpd10_indirect_cost.parquet'
                    WHERE cost_type IN ('Salaries', 'Benefits at 20%', 'Operating costs')
                    AND NOT EXISTS (
                        SELECT 1 
                        FROM '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="RUNDIR") }}/sq020/get_tng_cpd10_indirect_cost.parquet' 
                        WHERE cost_type = 'Estimated costs'
                    )
                """,
                target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq020/get_new_estimated_costs.parquet",
            )
            def get_new_estimated_costs():
                """
                Compute new estimated costs if not already recorded.
    
                Sums costs where cost_type IN ('Salaries', 'Benefits at 20%', 'Operating costs')
                if 'Estimated costs' record does not already exist.
                """
                pass


            @task.parquet(
                task_id="combine_costs_with_estimated",
                duckdb_conn_id="duckdb-conn",
                export_params={},
                clear_before_write=True,
                sql="""
                    (
                        SELECT year, cost_type, amount
                        FROM '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="RUNDIR") }}/sq020/get_tng_cpd10_indirect_cost.parquet'
                    )
                    UNION
                    (
                        SELECT year, cost_type, amount
                        FROM '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="RUNDIR") }}/sq020/get_new_estimated_costs.parquet'
                    )
                """,
                target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq020/combine_costs_with_estimated.parquet",
            )
            def combine_costs_with_estimated():
                """
                Combine cost data with newly computed estimated costs.
    
                Unions cost-related data from TNG_CPD10_INDIRECT_COST with newly
                computed estimated costs.
                """
                pass


            @task.parquet(
                task_id="get_cost_per_dlqnt_acct",
                duckdb_conn_id="duckdb-conn",
                export_params={},
                clear_before_write=True,
                sql="""
                    (SELECT
                        CAST( 
                        (SELECT amount FROM '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="RUNDIR") }}/sq020/combine_costs_with_estimated.parquet' WHERE cost_type = 'Estimated costs') 
                        / 
                        (SELECT CAST(count(distinct acct_id) as double) FROM '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="RUNDIR") }}/sq020/get_rcrr_tng_mort_acct_mth_snapshot.parquet') 
                        AS DECIMAL(15,7)) as amount,
                        'Cost per account' as cost_type,
                        '{{ task_instance.xcom_pull(task_ids="sq020_source.compute_prior_year") }}' as year)
                    UNION
                    (SELECT 
                        CAST(count(distinct acct_id) as double) amount, 
                        'Delinquent Accounts' as cost_type,
                        '{{ task_instance.xcom_pull(task_ids="sq020_source.compute_prior_year") }}' as year
                    FROM '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="RUNDIR") }}/sq020/get_rcrr_tng_mort_acct_mth_snapshot.parquet')
                """,
                target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq020/get_cost_per_dlqnt_acct.parquet",
            )
            def get_cost_per_dlqnt_acct():
                """
                Compute cost per delinquent account.
    
                Calculates average cost per delinquent account (estimated costs / distinct
                delinquent accounts) and counts distinct delinquent accounts with nonzero
                days in arrears.
                """
                pass


            @task.parquet(
                task_id="combine_costs_with_dlqnt",
                duckdb_conn_id="duckdb-conn",
                export_params={},
                clear_before_write=True,
                sql="""
                    (
                        SELECT year, cost_type, amount
                        FROM '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="RUNDIR") }}/sq020/combine_costs_with_estimated.parquet'
                        WHERE cost_type <> 'Benefits at 30%'
                    )
                    UNION
                    (
                        SELECT year, cost_type, amount
                        FROM '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="RUNDIR") }}/sq020/get_cost_per_dlqnt_acct.parquet'
                    )
                """,
                target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq020/combine_costs_with_dlqnt.parquet",
            )
            def combine_costs_with_dlqnt():
                """
                Combine cost data with delinquent account costs.
    
                Final union of cost data, excluding 'Benefits at 30%' records, with
                cost per delinquent account and delinquent accounts count.
                """
                pass


            """Source layer for sq020."""
            rundir_task = create_sq020_rundir()
            prior_year_task = compute_prior_year()

            extract_1 = get_tng_cpd10_indirect_cost()
            extract_2 = get_rcrr_tng_mort_acct_mth_snapshot()

            transform_1 = get_new_estimated_costs()
            combine_1 = combine_costs_with_estimated()
            transform_2 = get_cost_per_dlqnt_acct()
            combine_2 = combine_costs_with_dlqnt()

            # Dependency chain
            rundir_task >> prior_year_task
            prior_year_task >> [extract_1, extract_2]
            extract_1 >> transform_1
            [extract_1, transform_1] >> combine_1
            [extract_2, combine_1] >> transform_2
            [combine_1, transform_2] >> combine_2
        @task_group(group_id="sq020_enrichment")
        def sq020_enrichment_group():
            """
            TaskGroup for enrichment tasks in sequence sq020.
            """
            @task.duckdb(
                task_id="delete_if_exists_tng_cust_tu",
                duckdb_conn_id="duckdb-conn",
                sql="""
                    DELETE FROM ingestion.TNG_ACCT_INDCOST
                    WHERE MONTH_END_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="MTH_END_DT") }}';
                """,
            )
            def delete_if_exists_tng_acct_indcost():
                """
                Deletes existing records from ingestion.TNG_ACCT_INDCOST for the month end date being processed, if they exist. 
                This is to ensure that if there are any existing records for the month end date being processed, 
                they will be removed before new records are inserted from the parquet file.
                """
                pass


            @task.duckdb(
                task_id="insert_into_tng_acct_indcost",
                duckdb_conn_id="duckdb-conn",
                sql="""
                    INSERT INTO ingestion.TNG_ACCT_INDCOST BY NAME
                    SELECT * FROM '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="RUNDIR") }}/sq020/combine_costs_with_dlqnt.parquet';
                """,
            )
            def insert_into_tng_acct_indcost():
                """
                Inserts records into ingestion.TNG_ACCT_INDCOST from the parquet file generated in the enrichment step. 
                The parquet file is expected to be located in the RUNDIR under the sq020 folder and named combine_costs_with_dlqnt.parquet.
                """
                pass


            """ TaskFlow function definitions """
            delete_if_exists_tng_acct_indcost = delete_if_exists_tng_acct_indcost()
            insert_into_tng_acct_indcost = insert_into_tng_acct_indcost()

            """ Dependency chaining """
            delete_if_exists_tng_acct_indcost >> insert_into_tng_acct_indcost
        sq020_source_group = sq020_source_group()
        sq020_enrichment_group = sq020_enrichment_group()

        sq020_source_group >> sq020_enrichment_group


    @task(outlets=[AssetAlias("tng_acct_indcost")])
    def tng_acct_indcost(*, outlet_events):
        outlet_events[AssetAlias("tng_acct_indcost")].add(
            Asset("ingestion.TNG_ACCT_INDCOST", extra={})
        )


    sq020 = sq020_group()
    sq020_start = sq020_start()
    tng_acct_indcost = tng_acct_indcost()

    sq020_start >> sq020 >> tng_acct_indcost
    @task
    def sq023_start():
        """ Manual approval task to start sq023 """
        raise AirflowException("Please mark this task successful to start sequence sq023.")


    @task_group(group_id="sq023")
    def sq023_group():
        """
        TaskGroup for sequence sq023.
        """

        @task_group(group_id="sq023_source")
        def sq023_source_group():
            """
            TaskGroup for source tasks from EDL in sequence sq023.
            """
            # Import of source_group.py
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
        @task_group(group_id="sq023_enrichment")
        def sq023_enrichment_group():
            """
            TaskGroup for enrichment tasks in sequence sq023.
            """
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
        sq023_source_group = sq023_source_group()
        sq023_enrichment_group = sq023_enrichment_group()

        sq023_source_group >> sq023_enrichment_group


    @task(outlets=[AssetAlias("baselayer_mor")])
    def baselayer_mor(*, outlet_events):
        outlet_events[AssetAlias("baselayer_mor")].add(
            Asset("ingestion.BASELAYER_MOR", extra={})
        )


    sq023 = sq023_group()
    sq023_start = sq023_start()
    baselayer_mor = baselayer_mor() 

    sq023_start >> sq023 >> baselayer_mor
    @task
    def sq033_start():
        """ Manual approval task to start sq033 """
        raise AirflowException("Please mark this task successful to start sequence sq033.")


    @task_group(group_id="sq033")
    def sq033_group():
        """
        TaskGroup for sequence sq033.
        """

        @task_group(group_id="sq033_source")
        def sq033_source_group():
            """
            TaskGroup for source tasks from EDL in sequence sq033.
            """
            # Import of source_group.py
            @task
            def create_sq033_rundir():
                """Create sq033 run directory."""
                context = get_current_context()
                rundir = context["ti"].xcom_pull(task_ids="handle_month_context", key="RUNDIR")
                sq033_rundir = f"{rundir}/sq033"
                os.makedirs(sq033_rundir, exist_ok=True)


            @task.beeline(
                task_id="make_airb_tangrn_acct_colctn_txn",
                beeline_conn_id="edlr-conn",
                sql="""
                    select
                        cast('{{ task_instance.xcom_pull(task_ids="handle_month_context", key="MTH_END_DT") }}' as DATE) as MTH_END_DT,
                        case
                            when
                                MORTGAGE_NUMBER not like '%[^0-9]%' or MORTGAGE_NUMBER = ''
                            then
                                cast(MORTGAGE_NUMBER as BIGINT)
                            else
                                0
                        end as MORT_NUM,
                        case 
                            when
                                trim(TXN_DATE) = '' or TXN_DATE is NULL
                            then
                                ''
                            else
                                concat(
                                    substring(TXN_DATE,1,4), '-',
                                    substring(TXN_DATE,5,2), '-',
                                    substring(TXN_DATE,7,2)
                                )
                        end as TXN_DT,
                        TXN_AMOUNT as TXN_AMT,
                        TXN_COMMENT as TXN_CMNT,
                        TXN_TYPE_CATEGORY as TXN_TP_CAT
                    from
                        {{ var.value.TSZ_SCHEMA }}.TNG_CPD8_MORTGAGE_COLLECTION_TRANSACTION
                    where
                        businesseffectivedate = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="MTH_END_DT") }}'
                    ;
                """,
                rundir="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq033",
                to_parquet=True,
                tmpfileloc="/bns/rrap/data/tmp",
                target="make_airb_tangrn_acct_colctn_txn.parquet",
                schema=pa.schema([
                    ("MTH_END_DT", pa.date64()),
                    ("MORT_NUM", pa.int64()),
                    ("TXN_DT", pa.string()),
                    ("TXN_AMT", pa.float64()),
                    ("TXN_CMNT", pa.string()),
                    ("TXN_TP_CAT", pa.string()),
                ]),
            )
            def make_airb_tangrn_acct_colctn_txn():
                """
                Extract Tangerine mortgage collection transaction data.
    
                Extracts mortgage collection transaction information including month-end date,
                mortgage number (with numeric validation), transaction date (formatted from yyyymmdd),
                transaction amount, comment, and transaction type category from
                TNG_CPD8_MORTGAGE_COLLECTION_TRANSACTION for the current month-end date.
                """
                pass


            """Source layer for sq033."""
            rundir_task = create_sq033_rundir()
            extract_task = make_airb_tangrn_acct_colctn_txn()

            rundir_task >> extract_task
        @task_group(group_id="sq033_enrichment")
        def sq033_enrichment_group():
            """
            TaskGroup for enrichment tasks in sequence sq033.
            """
            @task.parquet(
                task_id="make_tng_acct_collecttrst",
                duckdb_conn_id="duckdb-conn",
                target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq033/tng_acct_collecttrst.parquet",
                sql="""
                    SELECT
                        cast(MTH_END_DT as DATE) as MONTH_END_DT,
                        cast(MORT_NUM as DECIMAL(12)) as MTG_NUM,
                        case
                            when TXN_DT = ''
                            then NULL
                            else cast(TXN_DT as DATE)
                        end as TXN_DATE,
                        cast(TXN_AMT as DECIMAL(12,2)) as TXN_AMOUNT,
                        case
                            when TXN_CMNT = ''
                            then NULL
                            else cast(TXN_CMNT as VARCHAR)
                        end as TXN_COMMENT,
                        cast(TXN_TP_CAT as VARCHAR) as TXN_TYPE_CATEGORY
                    FROM
                        '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq033/make_airb_tangrn_acct_colctn_txn.parquet'
                    WHERE
                        MTH_END_DT = '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='MTH_END_DT') }}'
                """,
                export_params={},
                clear_before_write=True,
            )
            def make_tng_acct_collecttrst():
                """This task transforms the previous month's account collection transactions extracted from make_airb_tangrn_acct_colctn_txn.parquet to make_tng_acct_collecttrst.parquet."""
                pass


            @task.duckdb(
                task_id="delete_if_exists",
                duckdb_conn_id="duckdb-conn",
                sql="""
                    DELETE FROM ingestion.TNG_ACCT_COLLECTTRST
                    WHERE MONTH_END_DT = '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='MTH_END_DT') }}'
                """,
            )
            def delete_if_exists():
                """This task deletes existing records for the month end date in TNG_ACCT_COLLECTTRST to prevent duplicates before upload."""
                pass


            @task.duckdb(
                task_id="insert_tng_acct_collecttrst",
                duckdb_conn_id="duckdb-conn",
                sql="""
                    INSERT INTO ingestion.TNG_ACCT_COLLECTTRST BY NAME
                    SELECT * FROM '{{ task_instance.xcom_pull(task_ids='sq033.sq033_enrichment.make_tng_acct_collecttrst', key='parquet') }}'
                """,
            )
            def insert_tng_acct_collecttrst():
                """This task inserts the transformed data into TNG_ACCT_COLLECTTRST."""
                pass


            """ TaskFlow function definitons """
            make_tng_acct_collecttrst = make_tng_acct_collecttrst()
            delete_if_exists = delete_if_exists()
            insert_tng_acct_collecttrst = insert_tng_acct_collecttrst()

            """ Dependency chaining """
            [
                make_tng_acct_collecttrst,
                delete_if_exists
            ] >> insert_tng_acct_collecttrst
        sq033_source_group = sq033_source_group()
        sq033_enrichment_group = sq033_enrichment_group()

        sq033_source_group >> sq033_enrichment_group


    @task(outlets=[AssetAlias("tng_acct_collecttrst")])
    def tng_acct_collecttrst(*, outlet_events):
        outlet_events[AssetAlias("tng_acct_collecttrst")].add(
            Asset("ingestion.TNG_ACCT_COLLECTTRST", extra={})
        )


    sq033 = sq033_group()
    sq033_start = sq033_start() 
    tng_acct_collecttrst = tng_acct_collecttrst()

    sq033_start >> sq033 >> tng_acct_collecttrst
    @task
    def sq034_start():
        """ Manual approval task to start sq034 """
        raise AirflowException("Please mark this task successful to start sequence sq034.")


    @task_group(group_id="sq034")
    def sq034_group():
        """
        TaskGroup for sequence sq034.
        """

        @task_group(group_id="sq034_source")
        def sq034_source_group():
            """
            TaskGroup for source tasks from EDL in sequence sq034.
            """
            # Import of source_group.py
            @task
            def create_sq034_rundir():
                """Create sq034 run directory."""
                context = get_current_context()
                rundir = context["ti"].xcom_pull(task_ids="handle_month_context", key="RUNDIR")
                sq034_rundir = f"{rundir}/sq034"
                os.makedirs(sq034_rundir, exist_ok=True)


            @task.beeline(
                task_id="make_airb_tangrn_mort_acct",
                beeline_conn_id="edlr-conn",
                sql="""
                    select
                        date_format(current_timestamp, 'yyyy-MM-dd HH:mm:ss.SSSSSS') as INSRT_PROCESS_TMSTMP,
                        cast(ACCT_ID as VARCHAR(80)) as ACCT_ID,
                        cast(MTH_ST_DT as DATE) as MTH_ST_DT,
                        '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="MTH_END_DT") }}' as MTH_END_DT,
                        cast(MATURITY_DT as DATE) as MAT_DT,
                        cast(OPEN_DT as DATE) as OPN_DT,
                        cast(COMMITTED_AMT as DECIMAL(22,2)) as COMMTTD_AMT,
                        cast(INTR_ARRS_AMT as  DECIMAL(22,2)) as INTR_ARRS_AMT,
                        cast(PRNCPL_ARRS_AMT as DECIMAL(22,2)) as PRNCPL_ARRS_AMT,
                        cast(ESCROW_ARRS_AMT as DECIMAL(22,2)) as ESCROW_ARRS_AMT,
                        cast(TOT_ARRS_AMT as DECIMAL(22,2)) as TOT_ARRS_AMT,
                        cast(NOMINAL_INTR_RATE as DECIMAL(11,4)) as NOMNL_INTR_RT,
                        cast(CRNT_LTV_RTO as DECIMAL(11,4)) as CRNT_LOAN_TO_VAL_RTO,
                        cast(RMNG_TERM as INTEGER) as RMNG_TERM,
                        cast(NON_PERFORMING_IND as VARCHAR(1)) as NON_PRFRMNG_IND,
                        cast(NAL_IND as CHAR(1)) as NON_ACCRL_IND,
                        cast(DAY_ARRS_CNT as INTEGER) as DAYS_ARRS_CNT,
                        cast(PYMTS_ARRS_CNT as INTEGER) as PYMTS_ARRS_CNT,
                        cast(NSF_CNT as INTEGER) as NSF_CNT,
                        cast(NSF_YTD_CNT as INTEGER) as NSF_YTD_CNT,
                        cast(NSF_LIFE_CNT as INTEGER) as NSF_LIFE_CNT,
                        cast(ST_PRNCPL_BAL_AMT as DECIMAL(22,2)) as ST_PRNCPL_BAL_AMT,
                        cast(OS_BAL_COA_AMT as DECIMAL(22,2)) as END_PRNCPL_BAL_AMT,
                        cast(SUNDRY_BAL_AMT as DECIMAL(22,2)) as SUNDRY_BAL_AMT,
                        cast(ESCROW_BAL_AMT as DECIMAL(22,2)) as ESCROW_BAL_AMT,
                        cast(TOT_ADVNC_AMT as DECIMAL(22,2)) as TOT_ADVNC_AMT,
                        cast(RMNG_AMORT as INTEGER) as RMNG_AMORT,
                        cast(ACCT_KEY as BIGINT) as ACCT_KEY,
                        cast(MORT_APPLICATION_KEY as BIGINT) as MORT_APP_KEY,
                        cast(POOL_KEY as BIGINT) as POOL_KEY,
                        cast(CUST_KEY as BIGINT) as CUST_KEY,
                        cast(AMORT_DT as DATE) as AMORT_DT,
                        cast(LAST_NSF_PYMT_RETURN_DT as DATE) as LAST_NSF_PYMT_RTN_DT,
                        cast(LAST_PYMT_DT as DATE) as LAST_PYMT_DT,
                        cast(NON_PERFORMING_DT as DATE) as NON_PRFRMNG_DT,
                        cast(LATEST_90_DT_PST_DUE_DT as DATE) as LATEST_90_DT_PD_DT,
                        cast(TOT_SCHED_PYMT_AMT as DECIMAL(22,2)) as TOT_SCHED_PYMT_AMT,
                        cast(LAST_KNOWN_COVER_PCTG as DECIMAL(11,4)) as LAST_KNOWN_COVER_PCTG,
                        cast(EVER_ARRS_CNT as INTEGER) as EVER_ARRS_CNT,
                        cast(EVER_30_CNT_PST_DUE_CNT as INTEGER) as EVER_30_CNT_PD_CNT,
                        cast(EVER_60_CNT_PST_DUE_CNT as INTEGER) as EVER_60_CNT_PD_CNT,
                        cast(EVER_90_CNT_PST_DUE_CNT as INTEGER) as EVER_90_CNT_PD_CNT,
                        cast(DEFLT_TYPE_CD as CHAR(20)) as DFT_TP_CD,
                        cast(MORT_ORGNTN_KEY as BIGINT) as MORT_ORGNTN_KEY,
                        cast(ORIG_TERM as INTEGER) as ORIG_TERM,
                        cast(RATE_TYPE_DESC as VARCHAR(80)) as RT_TP_DESC,
                        cast(ACCELERATED_PYMT_IND as VARCHAR(1)) as ACCELERATED_PYMT_IND,
                        cast(ANNUAL_FACTR as DECIMAL(11,4)) as ANUL_FACTR,
                        cast(MORT_PROVIDER_DESC as VARCHAR(255)) as MORT_PROVIDER_DESC,
                        cast(PRPTY_TYPE_DESC as VARCHAR(255)) as PRPTY_TP_DESC,
                        cast(TENURE_DESC as VARCHAR(255)) as TENURE_DESC,
                        cast(STAT_DESC as VARCHAR(255)) as STAT_DESC,
                        cast(INSURER_DESC as VARCHAR(255)) as INSURER_DESC,
                        cast(POOL_DESC as VARCHAR(255)) as POOL_DESC,
                        cast(PURPOSE_DESC as VARCHAR(255)) as PRPS_DESC,
                        cast(OCCUPANCY_TYPE_DESC as VARCHAR(255)) as OCPNY_TP_DESC,
                        cast(DWELLING_TYPE_CD as VARCHAR(20)) as DWELLING_TP_CD,
                        cast(OCCUPATION_INDSTR_CD as VARCHAR(100)) as OCP_INDSTR_CD,
                        cast(EARLY_RENEW_IND as VARCHAR(1)) as EARLY_RNEW_IND,
                        cast(ORIG_ADJ_BUREAU_SCORE as INTEGER) as ORIG_ADJUSTED_BUREAU_SCORE,
                        cast(SUBMIT_DT as DATE) as SUBMIT_DT,
                        cast(ORIG_GDSR as DECIMAL(22,2)) as ORIG_GDSR,
                        cast(ORIG_TDSR as DECIMAL(22,2)) as ORIG_TDSR,
                        cast(FIRST_PYMT_RETURN_DT as DATE) as FRST_PYMT_RTN_DT,
                        cast(FIRST_90_DAY_PST_DUE_DT as DATE) as FRST_90_DAYS_PD_DT,
                        cast(GUAR_IND as VARCHAR(1)) as GRNT_IND,
                        cast(ORIG_LTV_RTO as DECIMAL(11,4)) as ORIG_LOAN_TO_VAL_RTO,
                        cast(AMORT_PERIOD as INTEGER) as AMORT_PRD,
                        cast(ORIG_ADVNC_AMT as DECIMAL(22,2)) as ORIG_ADVNC_AMT,
                        cast(DIRECT_IND as VARCHAR(1)) as DRC_IND,
                        cast(FIRST_DEFLT_DT as DATE) as FRST_DFT_DT,
                        cast(FIRST_ADVNC_DT as DATE) as FRST_ADVNC_DT,
                        cast(LTV_RTO_AT_FIRST_DEFLT as DECIMAL(11,4)) as LTV_RTO_AT_FRST_DFT,
                        cast(LATEST_INTR_ADJ_DT as DATE) as LATEST_INTR_ADJ_DT,
                        cast(ADVNC_EFF_DT as DATE) as ADVNC_EFF_DT,
                        cast(PRPTY_CITY as VARCHAR(255)) as PRPTY_CITY,
                        cast(PRPTY_CNTRY_CD as VARCHAR(2)) as PRPTY_CNTRY_CD,
                        cast(PRPTY_POSTAL_CD as VARCHAR(20)) as PRPTY_POST_CD,
                        cast(PRPTY_PROV_CD as VARCHAR(2)) as PRPTY_PROV_CD,
                        cast(LAST_PRPTY_APPRAISAL_VAL as DECIMAL(22,2)) as LAST_PRPTY_APPRSL_VAL,
                        cast(FSA as VARCHAR(3)) as FSA,
                        cast(LAST_PRPTY_APPRAISAL_DT as DATE) as LAST_PRPTY_APPRSL_DT,
                        cast(LIEN_PRIORITY_NUM as INTEGER) as LIEN_PRIORITY_NUM,
                        cast(PRPTY_BUILDNG_TYPE as VARCHAR(20)) as PRPTY_BUILDING_TP,
                        cast(ORIG_COVER_EXEC_VAL as DECIMAL(22,2)) as ORIG_COVER_EXECUTION_VAL,
                        cast(PRPTY_USAGE_TYPE as VARCHAR(20)) as PRPTY_USAGE_TP,
                        cast(SCRTY_OWN_TYPE as VARCHAR(255)) as SCRTY_OWN_TP,
                        cast(SCRTY_PROVIDER as VARCHAR(255)) as SCRTY_PROVIDER,
                        cast(SCRTY_RGSTRN_BEGIN_DT as DATE) as SCRTY_RGSTRN_BEGIN_DT,
                        cast(SCRTY_RGSTRN_END_DT as DATE) as SCRTY_RGSTRN_END_DT,
                        cast(SCRTY_RGSTRN_NUM as VARCHAR(255)) as SCRTY_RGSTRN_NUM,
                        cast(ASSET_TYPE_DESC as VARCHAR(255)) as ASST_TP_DESC,
                        cast(ORIG_PRPTY_APPRAISAL_VAL as DECIMAL(22,2)) as ORIG_PRPTY_APPRSL_VAL,
                        cast(ORIG_PRPTY_APPRAISAL_DT as DATE) as ORIG_PRPTY_APPRSL_DT,
                        cast(PRPTY_PRCH_AMT as DECIMAL(22,2)) as PRPTY_PRCH_AMT,
                        cast(PRPTY_PRCH_DT as DATE) as PRPTY_PRCH_DT,
                        cast(ORIG_TOT_INCM as INTEGER) as ORIG_TOT_INCM,
                        cast(PRNCPL_INTR_PYMT_AMT as DECIMAL(22,2)) as PRNCPL_INTR_PYMT_AMT,
                        cast(NEXT_INTR_RESET_DT as DATE) as NEXT_INTR_RESET_DT,
                        cast(RATE_MODIFIER as DECIMAL(11,4)) as RT_MODFR,
                        cast(DOWN_PYMT_SRC_DESC as VARCHAR(255)) as DN_PYMT_SRC_DESC,
                        cast(CLIENT_DT as DATE) as CLIENT_DT,
                        cast(CIF_CREATED_ON_DT as DATE) as CIF_CREATD_ON_DT,
                        cast(DEFLT_IND as VARCHAR(1)) as DFT_IND,
                        cast(STATED_INCM_IND as VARCHAR(1)) as STATED_INCM_IND,
                        cast(ACCR_INTR_AMT as DECIMAL(22,2)) as ACCR_INTR_AMT,
                        cast(CLS_DT as DATE) as CLS_DT,
                        cast(BULK_INSURER_DESC as VARCHAR(255)) as BULK_INSURER_DESC,
                        cast(SECURITIZATION_IND as VARCHAR(1)) as SECRTZTN_IND,
                        cast(CIF_TYPE_DESC as VARCHAR(255)) as CIF_TP_DESC,
                        cast(CUST_TYPE_2 as VARCHAR(20)) as CUST_TP_2,
                        cast(INTR_PYMTS_AMT as DECIMAL(22,2)) as INTR_PYMTS_AMT,
                        cast(INTR_COMPOUNDING_FREQ as INTEGER) as INTR_CMPNDNG_FREQ,
                        cast(OPEN_CLS_TERM as VARCHAR(6)) as OPN_CLSD_TERM,
                        cast(PAYOUT_REASON as VARCHAR(6)) as PYT_RSN,
                        cast(LAST_RENEW_DT as DATE) as LAST_RNEW_DT,
                        cast(ACCT_NUM as VARCHAR(50)) as ACCT_NUM,
                        cast(GL_ACCT_NUM as VARCHAR(7)) as GL_ACCT_NUM,
                        cast(GL_ACCTNG_TRANSIT as VARCHAR(5)) as GL_TRNST_NUM,
                        cast(CURRENCY_CD as VARCHAR(3)) as CRNCY_CD
                    from
                        {{ var.value.RCRR_SCHEMA }}.TNG_MORT_ACCT_MTH_SNAPSHOT as MAIN
                    where
                        MAIN.MTH_END_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="MTH_END_DT") }}'
                    ;
                """,
                rundir="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq034",
                to_parquet=True,
                tmpfileloc="/bns/rrap/data/tmp",
                target="make_airb_tangrn_mort_acct.parquet",
                schema=pa.schema([
                    ("INSRT_PROCESS_TMSTMP", pa.string()),
                    ("ACCT_ID", pa.string()),
                    ("MTH_ST_DT", pa.date64()),
                    ("MTH_END_DT", pa.string()),
                    ("MAT_DT", pa.date64()),
                    ("OPN_DT", pa.date64()),
                    ("COMMTTD_AMT", pa.float64()),
                    ("INTR_ARRS_AMT", pa.float64()),
                    ("PRNCPL_ARRS_AMT", pa.float64()),
                    ("ESCROW_ARRS_AMT", pa.float64()),
                    ("TOT_ARRS_AMT", pa.float64()),
                    ("NOMNL_INTR_RT", pa.float64()),
                    ("CRNT_LOAN_TO_VAL_RTO", pa.float64()),
                    ("RMNG_TERM", pa.int64()),
                    ("NON_PRFRMNG_IND", pa.string()),
                    ("NON_ACCRL_IND", pa.string()),
                    ("DAYS_ARRS_CNT", pa.int64()),
                    ("PYMTS_ARRS_CNT", pa.int64()),
                    ("NSF_CNT", pa.int64()),
                    ("NSF_YTD_CNT", pa.int64()),
                    ("NSF_LIFE_CNT", pa.int64()),
                    ("ST_PRNCPL_BAL_AMT", pa.float64()),
                    ("END_PRNCPL_BAL_AMT", pa.float64()),
                    ("SUNDRY_BAL_AMT", pa.float64()),
                    ("ESCROW_BAL_AMT", pa.float64()),
                    ("TOT_ADVNC_AMT", pa.float64()),
                    ("RMNG_AMORT", pa.int64()),
                    ("ACCT_KEY", pa.int64()),
                    ("MORT_APP_KEY", pa.int64()),
                    ("POOL_KEY", pa.int64()),
                    ("CUST_KEY", pa.int64()),
                    ("AMORT_DT", pa.date64()),
                    ("LAST_NSF_PYMT_RTN_DT", pa.date64()),
                    ("LAST_PYMT_DT", pa.date64()),
                    ("NON_PRFRMNG_DT", pa.date64()),
                    ("LATEST_90_DT_PD_DT", pa.date64()),
                    ("TOT_SCHED_PYMT_AMT", pa.float64()),
                    ("LAST_KNOWN_COVER_PCTG", pa.float64()),
                    ("EVER_ARRS_CNT", pa.int64()),
                    ("EVER_30_CNT_PD_CNT", pa.int64()),
                    ("EVER_60_CNT_PD_CNT", pa.int64()),
                    ("EVER_90_CNT_PD_CNT", pa.int64()),
                    ("DFT_TP_CD", pa.string()),
                    ("MORT_ORGNTN_KEY", pa.int64()),
                    ("ORIG_TERM", pa.int64()),
                    ("RT_TP_DESC", pa.string()),
                    ("ACCELERATED_PYMT_IND", pa.string()),
                    ("ANUL_FACTR", pa.float64()),
                    ("MORT_PROVIDER_DESC", pa.string()),
                    ("PRPTY_TP_DESC", pa.string()),
                    ("TENURE_DESC", pa.string()),
                    ("STAT_DESC", pa.string()),
                    ("INSURER_DESC", pa.string()),
                    ("POOL_DESC", pa.string()),
                    ("PRPS_DESC", pa.string()),
                    ("OCPNY_TP_DESC", pa.string()),
                    ("DWELLING_TP_CD", pa.string()),
                    ("OCP_INDSTR_CD", pa.string()),
                    ("EARLY_RNEW_IND", pa.string()),
                    ("ORIG_ADJUSTED_BUREAU_SCORE", pa.int64()),
                    ("SUBMIT_DT", pa.date64()),
                    ("ORIG_GDSR", pa.float64()),
                    ("ORIG_TDSR", pa.float64()),
                    ("FRST_PYMT_RTN_DT", pa.date64()),
                    ("FRST_90_DAYS_PD_DT", pa.date64()),
                    ("GRNT_IND", pa.string()),
                    ("ORIG_LOAN_TO_VAL_RTO", pa.float64()),
                    ("AMORT_PRD", pa.int64()),
                    ("ORIG_ADVNC_AMT", pa.float64()),
                    ("DRC_IND", pa.string()),
                    ("FRST_DFT_DT", pa.date64()),
                    ("FRST_ADVNC_DT", pa.date64()),
                    ("LTV_RTO_AT_FRST_DFT", pa.float64()),
                    ("LATEST_INTR_ADJ_DT", pa.date64()),
                    ("ADVNC_EFF_DT", pa.date64()),
                    ("PRPTY_CITY", pa.string()),
                    ("PRPTY_CNTRY_CD", pa.string()),
                    ("PRPTY_POST_CD", pa.string()),
                    ("PRPTY_PROV_CD", pa.string()),
                    ("LAST_PRPTY_APPRSL_VAL", pa.float64()),
                    ("FSA", pa.string()),
                    ("LAST_PRPTY_APPRSL_DT", pa.date64()),
                    ("LIEN_PRIORITY_NUM", pa.int64()),
                    ("PRPTY_BUILDING_TP", pa.string()),
                    ("ORIG_COVER_EXECUTION_VAL", pa.float64()),
                    ("PRPTY_USAGE_TP", pa.string()),
                    ("SCRTY_OWN_TP", pa.string()),
                    ("SCRTY_PROVIDER", pa.string()),
                    ("SCRTY_RGSTRN_BEGIN_DT", pa.date64()),
                    ("SCRTY_RGSTRN_END_DT", pa.date64()),
                    ("SCRTY_RGSTRN_NUM", pa.string()),
                    ("ASST_TP_DESC", pa.string()),
                    ("ORIG_PRPTY_APPRSL_VAL", pa.float64()),
                    ("ORIG_PRPTY_APPRSL_DT", pa.date64()),
                    ("PRPTY_PRCH_AMT", pa.float64()),
                    ("PRPTY_PRCH_DT", pa.date64()),
                    ("ORIG_TOT_INCM", pa.int64()),
                    ("PRNCPL_INTR_PYMT_AMT", pa.float64()),
                    ("NEXT_INTR_RESET_DT", pa.date64()),
                    ("RT_MODFR", pa.float64()),
                    ("DN_PYMT_SRC_DESC", pa.string()),
                    ("CLIENT_DT", pa.date64()),
                    ("CIF_CREATD_ON_DT", pa.date64()),
                    ("DFT_IND", pa.string()),
                    ("STATED_INCM_IND", pa.string()),
                    ("ACCR_INTR_AMT", pa.float64()),
                    ("CLS_DT", pa.date64()),
                    ("BULK_INSURER_DESC", pa.string()),
                    ("SECRTZTN_IND", pa.string()),
                    ("CIF_TP_DESC", pa.string()),
                    ("CUST_TP_2", pa.string()),
                    ("INTR_PYMTS_AMT", pa.float64()),
                    ("INTR_CMPNDNG_FREQ", pa.int64()),
                    ("OPN_CLSD_TERM", pa.string()),
                    ("PYT_RSN", pa.string()),
                    ("LAST_RNEW_DT", pa.date64()),
                    ("ACCT_NUM", pa.string()),
                    ("GL_ACCT_NUM", pa.string()),
                    ("GL_TRNST_NUM", pa.string()),
                    ("CRNCY_CD", pa.string()),
                ]),
            )
            def make_airb_tangrn_mort_acct():
                """
                Extract Tangerine mortgage account month-end snapshot.
    
                Extracts comprehensive mortgage account data including balances, delinquency,
                rates, property details, security information, income, payments, and various
                indicators from TNG_MORT_ACCT_MTH_SNAPSHOT for the current month-end date.
                103 columns covering complete account lifecycle and characteristics.
                """
                pass


            """Source layer for sq034."""
            rundir_task = create_sq034_rundir()
            extract_task = make_airb_tangrn_mort_acct()

            rundir_task >> extract_task
        @task_group(group_id="sq034_enrichment")
        def sq034_enrichment_group():
            """
            TaskGroup for enrichment tasks in sequence sq034.
            """
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
        sq034_source_group = sq034_source_group()
        sq034_enrichment_group = sq034_enrichment_group()

        sq034_source_group >> sq034_enrichment_group


    @task(outlets=[AssetAlias("tng_acct_mo")])
    def tng_acct_mo(*, outlet_events):
        outlet_events[AssetAlias("tng_acct_mo")].add(
            Asset("ingestion.TNG_ACCT_MO ", extra={})
        )


    sq034 = sq034_group()
    sq034_start = sq034_start()
    tng_acct_mo = tng_acct_mo()

    sq034_start >> sq034 >> tng_acct_mo
    @task
    def sq035_start():
        """ Manual approval task to start sq035 """
        raise AirflowException("Please mark this task successful to start sequence sq035.")


    @task_group(group_id="sq035")
    def sq035_group():
        """
        TaskGroup for sequence sq035.
        """

        @task_group(group_id="sq035_source")
        def sq035_source_group():
            """
            TaskGroup for source tasks from EDL in sequence sq035.
            """
            # Import of source_group.py
            @task
            def create_sq035_rundir():
                """Create sq035 run directory."""
                context = get_current_context()
                rundir = context["ti"].xcom_pull(task_ids="handle_month_context", key="RUNDIR")
                sq035_rundir = f"{rundir}/sq035"
                os.makedirs(sq035_rundir, exist_ok=True)


            @task.beeline(
                task_id="get_airb_asst_src",
                beeline_conn_id="edlr-conn",
                sql="""
                    select
                        BASEL_ID,
                        ASST_NUM AS ASSETNUMBER,
                        REPLACE(ACCT_NUM, '\t', ' ') AS ACCOUNTNUMBER,
                        TRNST_NUM AS TRANSITNUMBER,
                        AGRMNT_TP AS AGREEMENTTYPE,
                        PGM_TP AS PROGRAMTYPE,
                        CAST(BAL_OWNG AS DOUBLE) AS BALANCEOWING,
                        CAST(PRNCPL_OWNG_AT_ASGNMNT AS DOUBLE) AS PRINCIPALOWINGATASSIGNMENT,
                        CAST(ACCR_INTR_AT_ASGNMNT AS DOUBLE) AS ACCRUEDINTERESTATASSIGNMENT,
                        CAST(ADD_ON_COSTS_AT_ASGNMNT AS DOUBLE) AS ADDONCOSTSATASSIGNMENT,
                        (CASE WHEN UPPER(ASGNMNT_DT) = 'NULL' THEN NULL ELSE ASGNMNT_DT END) AS ASSIGNMENTDATE,
                        INSTRCTN AS INSTRUCTION,
                        regexp_replace(stat, '\\u2013', '\\u001A')  AS STATUS,
                        ACCT_STAT AS ACCOUNTSTATUS,
                        (CASE WHEN UPPER(MTH_END_DT) = 'NULL' THEN NULL ELSE MTH_END_DT END) AS MONTHENDDATE,
                        JDGMNT_IND AS JUDGEMENTINDICATOR,
                        CAST(LEGAL_COSTS AS DOUBLE) AS LEGALCOSTS,
                        CAST(PRPTY_MGT_COSTS AS DOUBLE) AS PROPERTYMANAGEMENTCOSTS,
                        CAST(INSPCTN_FEES AS DOUBLE) AS INSPECTIONFEES,
                        CAST(ENVMNTL_FEES AS DOUBLE) AS ENVIRONMENTALFEES,
                        CAST(GST_ON_INCM AS DOUBLE) AS GSTONINCOME,
                        CAST(UTLTS AS DOUBLE) AS UTILITIES,
                        CAST(REPAIRS AS DOUBLE) AS REPAIRS,
                        CAST(CR_RPTG_COSTS AS DOUBLE) AS CREDITREPORTINGCOSTS,
                        CAST(CORP_RISK_INSUR AS DOUBLE) AS CORPORATERISKINSURANCE,
                        CAST(TAXES AS DOUBLE) AS TAXES,
                        CAST(APPRSL_FEES AS DOUBLE) AS APPRAISALFEES,
                        CAST(CNDMNM_FEES AS DOUBLE) AS CONDOMINIUMFEES,
                        CAST(MISCLNS_FEES AS DOUBLE) AS MISCELLANEOUSFEES,
                        CAST(LOAD_FEE AS DOUBLE) AS LOADFEE,
                        CAST(CMMSNS AS DOUBLE) AS COMMISIONS,
                        CAST(TOT_COSTS_OR_EXPNSS AS DOUBLE) AS TOTALCOSTSOREXPENSES,
                        CAST(DLLRS_RCVRD_RECVD AS DOUBLE) AS DOLLARSRECOVEREDRECEIVED,
                        CAST(PRCD_TO_PAY_FOR_EXPNSS AS DOUBLE) AS PROCEEDSTOPAYFOREXP,
                        CAST(TOT_RCVRS AS DOUBLE) AS TOTALRECOVERIES,
                        CAST(HLDBCKS AS DOUBLE) AS HOLDBACKS,
                        CAST(APPRSL AS DOUBLE) AS APPRAISAL,
                        CAST(MTH_END_PRNCPL_BAL_OWNG AS DOUBLE) AS MONTHENDPRINCIPALBALOWING,
                        CAST(MTH_END_ACCR_INTR_OWNG AS DOUBLE) AS MONTHENDACCRUEDINTERESTOWING,
                        CAST(MTH_END_ADD_ON_COST AS DOUBLE) AS MONTHENDADDONCOST,
                        HGHWY AS HIGHWAY,
                        STATE,
                        (CASE WHEN UPPER(CLSD_DT) = 'NULL' THEN NULL ELSE CLSD_DT END) AS CLOSEDDATE,
                        HOST_MNEMONIC,
                        date_format(current_timestamp, 'yyyy-MM-dd HH:mm:ss.SSSSSS') as INSRT_PROCESS_TMSTMP,
                        NULL as UPDT_PROCESS_TMSTMP
                    FROM {{ var.value.TSZ_B9XF_RRAP_SCHEMA }}.airb_asst_src
                    WHERE businesseffectivedate = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="MTH_END_DT") }}';
                """,
                rundir="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq035",
                to_parquet=True,
                tmpfileloc="/bns/rrap/data/tmp",
                target="get_airb_asst_src.parquet",
                schema=pa.schema([
                    ('BASID', pa.int64()),
                    ('ASSETNUMBER', pa.int64()),
                    ('ACCOUNTNUMBER', pa.string()),
                    ('TRANSITNUMBER', pa.string()),
                    ('AGREEMENTTYPE', pa.string()),
                    ('PROGRAMTYPE', pa.string()),
                    ('BALANCEOWING', pa.float64()),
                    ('PRINCIPALOWINGATASSIGNMENT', pa.float64()),
                    ('ACCRUEDINTERESTATASSIGNMENT', pa.float64()),
                    ('ADDONCOSTSATASSIGNMENT', pa.float64()),
                    ('ASSIGNMENTDATE', pa.timestamp('us')),
                    ('INSTRUCTION', pa.string()),
                    ('STATUS', pa.string()),
                    ('ACCOUNTSTATUS', pa.string()),
                    ('MONTHENDDATE', pa.timestamp('us')),
                    ('JUDGEMENTINDICATOR', pa.string()),
                    ('LEGALCOSTS', pa.float64()),
                    ('PROPERTYMANAGEMENTCOSTS', pa.float64()),
                    ('INSPECTIONFEES', pa.float64()),
                    ('ENVIRONMENTALFEES', pa.float64()),
                    ('GSTONINCOME', pa.float64()),
                    ('UTILITIES', pa.float64()),
                    ('REPAIRS', pa.float64()),
                    ('CREDITREPORTINGCOSTS', pa.float64()),
                    ('CORPORATERISKINSURANCE', pa.float64()),
                    ('TAXES', pa.float64()),
                    ('APPRAISALFEES', pa.float64()),
                    ('CONDOMINIUMFEES', pa.float64()),
                    ('MISCELLANEOUSFEES', pa.float64()),
                    ('LOADFEE', pa.float64()),
                    ('COMMISIONS', pa.float64()),
                    ('TOTALCOSTSOREXPENSES', pa.float64()),
                    ('DOLLARSRECOVEREDRECEIVED', pa.float64()),
                    ('PROCEEDSTOPAYFOREXP', pa.float64()),
                    ('TOTALRECOVERIES', pa.float64()),
                    ('HOLDBACKS', pa.float64()),
                    ('APPRAISAL', pa.float64()),
                    ('MONTHENDPRINCIPALBALOWING', pa.float64()),
                    ('MONTHENDACCRUEDINTERESTOWING', pa.float64()),
                    ('MONTHENDADDONCOST', pa.float64()),
                    ('HIGHWAY', pa.string()),
                    ('STATE', pa.string()),
                    ('CLOSEDDATE', pa.timestamp('us')),
                    ('HOST_MNEMONIC', pa.string()),
                    ('INSRT_PROCESS_TMSTMP', pa.timestamp('us')),
                    ('UPDT_PROCESS_TMSTMP', pa.timestamp('us')),
                ]),
            )
            def get_airb_asst_src():
                """
                Extract asset source data.
    
                Extracts asset information including asset/account/transit numbers, balance
                and cost details, recovery metrics, property details, and administrative
                timestamps from AIRB_ASST_SRC for the current business effective date.
                Applies transformations: replaces tab characters in account numbers, handles
                null date strings, and casts numeric fields.
                """
                pass


            rundir_task = create_sq035_rundir()
            extract_task = get_airb_asst_src()

            rundir_task >> extract_task
        @task_group(group_id="sq035_enrichment")
        def sq035_enrichment_group():
            """
            TaskGroup for enrichment tasks in sequence sq035.
            """
            @task.update(
                task_id="update_asset_src_curr",
                duckdb_conn_id="duckdb-conn",
                target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq035/airb_asst_src.parquet",
                source="{{ task_instance.xcom_pull(task_ids='sq035.sq035_source.get_airb_asst_src', key='parquet') }}",
                sql="""
                    SET
                        JUDGEMENTINDICATOR = (CASE WHEN TRIM(JUDGEMENTINDICATOR) = '' THEN NULL ELSE JUDGEMENTINDICATOR END),
                        TRANSITNUMBER = (CASE WHEN TRIM(TRANSITNUMBER) = '' THEN NULL ELSE TRANSITNUMBER END),
                        HOST_MNEMONIC = (CASE WHEN TRIM(HOST_MNEMONIC) = '' THEN NULL ELSE HOST_MNEMONIC END)
                """,
                export_params={},
                clear_before_write=True,
            )
            def update_asset_src_curr():
                """
                Update asset source data by replacing blanks with NULLs in specific columns.
                """
                pass


            @task.duckdb(
                task_id="insert_asset_src_curr",
                duckdb_conn_id="duckdb-conn",
                sql="""
                    INSERT INTO ingestion.ASSET_SRC_CURR BY NAME
                    SELECT * FROM '{{ task_instance.xcom_pull(task_ids="sq035.sq035_enrichment.update_asset_src_curr", key="parquet") }}'
                """,
            )
            def insert_asset_src_curr():
                """ Insert asset source data into the target DuckLake table. """    
                pass


            """ TaskFlow function definitions """
            update_asset_src_curr = update_asset_src_curr()
            insert_asset_src_curr = insert_asset_src_curr()

            """ Dependency chaining """
            update_asset_src_curr >> insert_asset_src_curr
        sq035_source_group = sq035_source_group()
        sq035_enrichment_group = sq035_enrichment_group()

        sq035_source_group >> sq035_enrichment_group


    @task(outlets=[AssetAlias("asset_src_curr")])
    def asset_src_curr(*, outlet_events):
        outlet_events[AssetAlias("asset_src_curr")].add(
            Asset("ingestion.ASSET_SRC_CURR", extra={})
        )


    sq035 = sq035_group()
    sq035_start = sq035_start()
    asset_src_curr = asset_src_curr()

    sq035_start >> sq035 >> asset_src_curr
    @task
    def sq036_start():
        """ Manual approval task to start sq036 """
        raise AirflowException("Please mark this task successful to start sequence sq036.")


    @task_group(group_id="sq036")
    def sq036_group():
        """
        TaskGroup for sequence sq036.
        """

        @task_group(group_id="sq036_source")
        def sq036_source_group():
            """
            TaskGroup for source tasks from EDL in sequence sq036.
            """
            # Import of source_group.py
            @task
            def create_sq036_rundir():
                """Create sq036 run directory."""
                context = get_current_context()
                rundir = context["ti"].xcom_pull(task_ids="handle_month_context", key="RUNDIR")
                sq036_rundir = f"{rundir}/sq036"
                os.makedirs(sq036_rundir, exist_ok=True)


            @task.beeline(
                task_id="get_region_list",
                beeline_conn_id="edlr-conn",
                sql="""
                    select
                        region_id,
                        prpty_type,
                        `desc`,
                        prov_cd
                    from {{ var.value.RCRR_SCHEMA }}.region_snapshot
                    where mth_end_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="PREV_MTH_END_DT") }}'
                """,
                rundir="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq036",
                to_parquet=True,
                tmpfileloc="/bns/rrap/data/tmp",
                target="region_list.parquet",
                schema=pa.schema([
                    ("region_id", pa.string()),
                    ("prpty_type", pa.string()),
                    ("desc", pa.string()),
                    ("prov_cd", pa.string()),
                ]),
            )
            def get_region_list():
                """Extract region snapshot records for previous month-end."""
                pass


            @task.beeline(
                task_id="get_hpi_monthly_data",
                beeline_conn_id="edlr-conn",
                sql="""
                    select
                        year,
                        month,
                        region_type,
                        region_id,
                        prpty_type,
                        paircount,
                        valueraw,
                        valuesmoothed,
                        mth_end_dt
                    from {{ var.value.RCRR_SCHEMA }}.hpi_mnthly_snapshot
                """,
                rundir="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq036",
                to_parquet=True,
                tmpfileloc="/bns/rrap/data/tmp",
                target="hpi_monthly_data.parquet",
                schema=pa.schema([
                    ("year", pa.int16()),
                    ("month", pa.int8()),
                    ("region_type", pa.string()),
                    ("region_id", pa.string()),
                    ("prpty_type", pa.string()),
                    ("paircount", pa.string()),
                    ("valueraw", pa.string()),
                    ("valuesmoothed", pa.string()),
                    ("mth_end_dt", pa.string()),
                ]),
            )
            def get_hpi_monthly_data():
                """Extract HPI monthly snapshot source records."""
                pass


            @task.parquet(
                task_id="load_region_list",
                duckdb_conn_id="duckdb-conn",
                export_params={},
                clear_before_write=True,
                sql="""
                    select
                        region_id as regionid,
                        prpty_type as propertytype,
                        `desc` as description,
                        prov_cd as province
                    from '{{ task_instance.xcom_pull(task_ids="sq036.sq036_source.get_region_list", key="parquet") }}'
                """,
                target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq036/load_region_list.parquet",
            )
            def load_region_list():
                """Normalize region list field names."""
                pass


            @task.parquet(
                task_id="load_hpi_monthly_data",
                duckdb_conn_id="duckdb-conn",
                export_params={},
                clear_before_write=True,
                sql="""
                    select
                        year,
                        month,
                        region_type as regiontype,
                        region_id as regionid,
                        prpty_type as propertytype,
                        paircount,
                        valueraw,
                        valuesmoothed,
                        mth_end_dt
                    from '{{ task_instance.xcom_pull(task_ids="sq036.sq036_source.get_hpi_monthly_data", key="parquet") }}'
                """,
                target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq036/load_hpi_monthly_data.parquet",
            )
            def load_hpi_monthly_data():
                """Normalize monthly HPI field names."""
                pass


            @task.parquet(
                task_id="consolidate_hpi_data",
                duckdb_conn_id="duckdb-conn",
                export_params={},
                clear_before_write=True,
                sql="""
                    with prev_dt as (
                        select '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="PREV_MTH_END_DT") }}' as dt
                    ),
                    current_rows as (
                        select
                            m.year,
                            m.month,
                            m.regiontype,
                            m.regionid,
                            m.propertytype,
                            m.paircount,
                            m.valueraw,
                            m.valuesmoothed,
                            r.province,
                            r.description,
                            case when m.regionid in (
                                'CMA_935','CMA_933','CMA_825','CMA_835','CMA_602','CMA_537',
                                'CMA_535','CMA_505','CMA_462','CMA_421','CMA_205','CA_1'
                            ) then 'Y' else 'N' end as cma11flag
                        from '{{ task_instance.xcom_pull(task_ids="sq036.sq036_source.load_hpi_monthly_data", key="parquet") }}' m
                        join '{{ task_instance.xcom_pull(task_ids="sq036.sq036_source.load_region_list", key="parquet") }}' r
                            on r.regionid = m.regionid
                        join prev_dt p on 1=1
                        where r.regionid like '%C%'
                          and r.propertytype like 'ALL%'
                          and m.propertytype = 'ALL'
                          and m.mth_end_dt = p.dt
                    ),
                    latest_fallback as (
                        select
                            m.year,
                            m.month,
                            m.regiontype,
                            m.regionid,
                            m.propertytype,
                            m.paircount,
                            m.valueraw,
                            m.valuesmoothed,
                            r.province,
                            r.description,
                            'N' as cma11flag,
                            row_number() over (partition by m.regionid order by m.year desc, m.month desc) as rn
                        from '{{ task_instance.xcom_pull(task_ids="sq036.sq036_source.load_hpi_monthly_data", key="parquet") }}' m
                        join '{{ task_instance.xcom_pull(task_ids="sq036.sq036_source.load_region_list", key="parquet") }}' r
                            on r.regionid = m.regionid
                        where m.regionid in ('CMA_001','CMA_305','CMA_310')
                    ),
                    missing_regions as (
                        select regionid from (values ('CMA_001'),('CMA_305'),('CMA_310')) t(regionid)
                        where regionid not in (select distinct regionid from current_rows)
                    )
                    select * from current_rows
                    union all
                    select
                        f.year,
                        f.month,
                        f.regiontype,
                        f.regionid,
                        f.propertytype,
                        f.paircount,
                        f.valueraw,
                        f.valuesmoothed,
                        f.province,
                        f.description,
                        f.cma11flag
                    from latest_fallback f
                    join missing_regions mr on mr.regionid = f.regionid
                    where f.rn = 1
                """,
                target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq036/consolidate_hpi_data.parquet",
            )
            def consolidate_hpi_data():
                """Build consolidated HPI data with fallback rows for quarterly-only CMA regions."""
                pass


            """ TaskFlow function definitions """
            rundir_task = create_sq036_rundir()
            region_task = get_region_list()
            monthly_task = get_hpi_monthly_data()
            region_norm_task = load_region_list()
            monthly_norm_task = load_hpi_monthly_data()
            consolidate_task = consolidate_hpi_data()

            """ Dependency chaining """
            rundir_task >> [region_task, monthly_task]
            region_task >> region_norm_task
            monthly_task >> monthly_norm_task
            [region_norm_task, monthly_norm_task] >> consolidate_task

            consolidate_task
        @task_group(group_id="sq036_enrichment")
        def sq036_enrichment_group():
            """
            TaskGroup for enrichment tasks in sequence sq036.
            """
            # Utility functions to strip accents and unwanted characters
            def strip_accents_arrow(array):
                def strip_accents_python(s):
                    return ''.join(
                        c for c in unicodedata.normalize('NFD', s)
                        if unicodedata.category(c) != 'Mn'
                    )

                return pa.array([strip_accents_python(s.as_py()) for s in array])


            def remove_unwanted_string(table, column_name, char_to_rmv:str, char_to_replace:str):
                if column_name in table.column_names:
                    column = table[column_name]
                    if pa.types.is_string(column.type):
                        trimmed_column = pc.utf8_trim_whitespace(column)
                        no_accent = strip_accents_arrow(trimmed_column)
                        new_column = pc.replace_substring(no_accent, pattern=char_to_rmv, replacement=char_to_replace)
                        return table.set_column(table.column_names.index(column_name), column_name, new_column)
                return table


            @task.duckdb(
                task_id="load_consolidate_hpi_data",
                duckdb_conn_id="duckdb-conn",
                sql="""
                INSERT INTO ingestion.RRAP_TERANET_CONSOLIDATED_CMA_DATA BY NAME
                SELECT * FROM '{{ task_instance.xcom_pull(task_ids="sq036.sq036_source.consolidate_hpi_data", key="parquet") }}'
                """,
            )
            def load_consolidated_hpi_data():
                """
                This task loads the consolidated HPI data into the target table in DuckDB.
                """
                pass


            @task.parquet(
                task_id="create_hpi_data",
                duckdb_conn_id="duckdb-conn",
                export_params={},
                clear_before_write=True,
                sql="""
                    select
                        year,
                        month,
                        regiontype,
                        regionid,
                        propertytype,
                        paircount,
                        valueraw,
                        valuesmoothed,
                        mth_end_dt
                    from '{{ task_instance.xcom_pull(task_ids="sq036.sq036_source.consolidate_hpi_data", key="parquet") }}'
                    where regiontype = 'CMA' and propertytype = 'Total'
                """,
                target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq036/hpi_data.parquet",
            )
            def create_hpi_data():
                """
                This task creates a Parquet file filtering the consolidated HPI data for records where the 
                region type is 'CMA' and the property type is 'Total'.
                """
                pass


            @task
            def transform_cma32_monthly_data():
                """
                This task applies necessary transformations to the CMA32 monthly data, such as stripping accents and removing unwanted characters.
                """
                hook = DuckLakeHook(duckdb_conn_id="duckdb-conn")
                context = get_current_context()

                # Setup rundir for transformation outputs
                rundir = context['task_instance'].xcom_pull(task_ids='handle_month_context', key='RUNDIR')
                curr_mth_pqt = f"{rundir}/sq036/cma32_monthly.parquet"
                prev_mth_pqt = f"{rundir}/sq036/cma32_monthly_prev.parquet"
                tmp_pqt = f"{rundir}/sq036/tmp_cma32_monthly.parquet"
                final_output_pqt = f"{rundir}/sq036/cma32_data.parquet"

                # setup dt variables for queries
                mth_tm_id = context['task_instance'].xcom_pull(task_ids='handle_month_context', key='MTH_TM_ID')
                prev_mth_tm_id = context['task_instance'].xcom_pull(task_ids='handle_month_context', key='PREV_MTH_TM_ID')
                txn_dt = context['task_instance'].xcom_pull(task_ids='handle_month_context', key='MTH_END_DT').strftime('%Y-%m-%d')
                pub_dt = context['task_instance'].xcom_pull(task_ids='handle_month_context', key='MTH_END_DT').add(months=1).strftime('%Y-%m-%d')

                hook.sql(f"""
                    COPY (
                        SELECT {mth_tm_id} AS MTH_TM_ID,
                        CASE WHEN PROVINCE = 'CA' THEN 'COMPOSITE' ELSE PROVINCE END AS LABEL_1,
                        CASE 
                            WHEN DESCRIPTION = 'National Composite' THEN '11' 
                            WHEN DESCRIPTION LIKE 'Ottawa%' THEN UPPER('Ottawa - Gatineau')
                            ELSE UPPER(DESCRIPTION) 
                        END AS LABEL_2,
                        '{txn_dt}' AS  TXN_DT,
                        '{pub_dt}' AS PUBLCT_DT, 
                        VALUESMOOTHED AS INDEX,
            			PAIRCOUNT AS SLS_PAIR_CNT, 
                        CURRENT_TIMESTAMP AS INSRT_PROCESS_TMSTMP, 
            			CURRENT_TIMESTAMP AS UPDT_PROCESS_TMSTMP
            			FROM  '{rundir}/sq036/hpi_data.parquet'
                        WHERE CMA11Flag = 'Y'
                    ) TO '{curr_mth_pqt}' (FORMAT PARQUET)
                """)

                hook.sql(f"""
                    COPY (
                        SELECT 
                        {prev_mth_tm_id} AS MTH_TM_ID, 
            			CASE WHEN PROVINCE = 'CA' THEN 'COMPOSITE' ELSE PROVINCE END AS LABEL_1,
                        CASE 
                            WHEN DESCRIPTION = 'National Composite' THEN '11' 
                            WHEN DESCRIPTION LIKE 'Ottawa%' THEN UPPER('Ottawa_Gatineau')
                            ELSE DESCRIPTION 
                        END AS LABEL_2, 
                        CASE 
                            WHEN DESCRIPTION = 'National Composite' THEN '11' 
                            WHEN DESCRIPTION LIKE 'Ottawa%' THEN UPPER('Ottawa - Gatineau')
                            ELSE UPPER(DESCRIPTION)  
                        END AS LABEL_2_ORIG,
                        VALUESMOOTHED AS INDEX,
            			PAIRCOUNT AS SLS_PAIR_CNT, 
                        CURRENT_TIMESTAMP AS INSRT_PROCESS_TMSTMP, 
            			CURRENT_TIMESTAMP AS UPDT_PROCESS_TMSTMP
            
            			FROM  '{rundir}/sq036/hpi_data.parquet'
                    ) TO '{prev_mth_pqt}' (FORMAT PARQUET)
                """)

                files = [ curr_mth_pqt, prev_mth_pqt ]

                schema = pq.ParquetFile(files[0]).schema_arrow
                with pq.ParquetWriter(tmp_pqt, schema=schema) as writer:
                    for file in files:
                        table = pq.read_table(file, schema=schema)
                        writer.write_table(table)

                table=pq.read_table(tmp_pqt)
                column_to_update = 'LABEL_2_ORIG'
                second_column_to_update = 'LABEL_2'
    
                remove_slash = remove_unwanted_string(table, column_to_update, char_to_rmv=' / ', char_to_replace='_')
                remove_underscore = remove_unwanted_string(remove_slash, column_to_update, char_to_rmv=' - ', char_to_replace='_')
                remove_period = remove_unwanted_string(remove_underscore, column_to_update, char_to_rmv='. ', char_to_replace='_')
                remove_space = remove_unwanted_string(remove_period, column_to_update, char_to_rmv=' ', char_to_replace='_')
                remove_quote = remove_unwanted_string(remove_space, column_to_update, char_to_rmv='\'', char_to_replace='')
                remove_second_quote = remove_unwanted_string(remove_quote, second_column_to_update, char_to_rmv='\'', char_to_replace='')
                remove_dash = remove_unwanted_string(remove_second_quote, column_to_update, char_to_rmv='-', char_to_replace='_')
                new_table = remove_dash

                #insert composite 6 row
                current_time = pendulum.now()
                composite_6 = {
                    "MTH_TM_ID": [mth_tm_id],
                    "LABEL_1": ['COMPOSITE'],
                    "LABEL_2": ['6'],
                    "LABEL_2_ORIG": ['6'],
                    "INDEX":[None],
                    "SLS_PAIR_CNT":[None],
                    "INSRT_PROCESS_TMSTMP" : [current_time],
                    "UPDT_PROCESS_TMSTMP" : [current_time]

                }
                prev_composite_6 = {
                    "MTH_TM_ID": [prev_mth_tm_id],
                    "LABEL_1": ['COMPOSITE'],
                    "LABEL_2": ['6'],
                    "LABEL_2_ORIG": ['6'],
                    "INDEX":[None],
                    "SLS_PAIR_CNT":[None],
                    "INSRT_PROCESS_TMSTMP" : [current_time],
                    "UPDT_PROCESS_TMSTMP" : [current_time]

                }

                new_row_1 = pa.RecordBatch.from_pydict(composite_6)
                new_row_2 = pa.RecordBatch.from_pydict(prev_composite_6)
    

                new_row_table = pa.Table.from_batches([new_row_1, new_row_2])
                new_row_table = new_row_table.cast(new_table.schema)
                combined_table = pa.concat_tables([new_table, new_row_table])
                pq.write_table(combined_table, final_output_pqt)

                context['task_instance'].xcom_push(key='parquet', value=final_output_pqt)


            @task.duckdb(
                task_id="delete_if_exists_cma32",
                duckdb_conn_id="duckdb-conn",
                sql="""
                DELETE FROM ingestion.TERANET_HOUSE_PRC_INDEX_CMA
                WHERE MTH_TM_ID IN ( 
                    {{ task_instance.xcom_pull(task_ids='handle_month_context', key='MTH_TM_ID') }},
                    {{ task_instance.xcom_pull(task_ids='handle_month_context', key='PREV_MTH_TM_ID') }}
                )
                """,
            )
            def delete_if_exists_cma32():
                """ This task deletes existing records for the current month and previous month from the ingestion table. """
                pass    


            @task.duckdb(
                task_id="load_cma32",
                duckdb_conn_id="duckdb-conn",
                sql="""
                INSERT INTO ingestion.TERANET_HOUSE_PRC_INDEX_CMA BY NAME
                SELECT * FROM '{{ task_instance.xcom_pull(task_ids="sq036.sq036_enrichment.create_parquet_cma32", key="parquet") }}'
                """,
            )
            def load_cma32():
                """
                This task loads the CMA32 data into the target table in DuckDB.
                """
                pass


            @task
            def transform_cma11_monthly_data():
                """
                This task applies necessary transformations to the CMA11 monthly data, such as stripping accents and removing unwanted characters.
                """
                hook = DuckLakeHook(duckdb_conn_id="duckdb-conn")
                context = get_current_context()

                # Setup rundir for transformation outputs
                rundir = context['task_instance'].xcom_pull(task_ids='handle_month_context', key='RUNDIR')
                curr_mth_pqt = f"{rundir}/sq036/cma11_monthly.parquet"
                prev_mth_pqt = f"{rundir}/sq036/cma11_monthly_prev.parquet"
                tmp_pqt = f"{rundir}/sq036/tmp_cma11_monthly.parquet"
                final_output_pqt = f"{rundir}/sq036/cma11_data.parquet"

                # setup dt variables for queries
                mth_tm_id = context['task_instance'].xcom_pull(task_ids='handle_month_context', key='MTH_TM_ID')
                prev_mth_tm_id = context['task_instance'].xcom_pull(task_ids='handle_month_context', key='PREV_MTH_TM_ID')
                txn_dt = context['task_instance'].xcom_pull(task_ids='handle_month_context', key='MTH_END_DT').strftime('%Y-%m-%d')
                pub_dt = context['task_instance'].xcom_pull(task_ids='handle_month_context', key='MTH_END_DT').add(months=1).strftime('%Y-%m-%d')

                hook.sql(f"""
                    COPY (
                        SELECT {mth_tm_id} AS MTH_TM_ID, 
                        CASE WHEN PROVINCE = 'CA' THEN 'COMPOSITE' ELSE PROVINCE END AS LABEL_1,
                        CASE 
                            WHEN DESCRIPTION = 'National Composite' THEN '11' 
                            WHEN DESCRIPTION LIKE 'Ottawa%' THEN UPPER('Ottawa - Gatineau')
                            ELSE UPPER(DESCRIPTION) 
                        END AS LABEL_2,
                        '{txn_dt}' AS  TXN_DT,
                        '{pub_dt}' AS PUBLCT_DT, 
                        VALUESMOOTHED AS INDEX,
                        PAIRCOUNT AS SLS_PAIR_CNT, 
                        CURRENT_TIMESTAMP AS INSRT_PROCESS_TMSTMP, 
                        CURRENT_TIMESTAMP AS UPDT_PROCESS_TMSTMP
                        FROM  '{rundir}/sq036/hpi_data.parquet'
                        WHERE CMA11Flag = 'Y'
                    ) TO '{curr_mth_pqt}' (FORMAT PARQUET)
                """)

                hook.sql(f"""
                    COPY (
                        SELECT {prev_mth_tm_id} AS MTH_TM_ID, 
                        CASE WHEN PROVINCE = 'CA' THEN 'COMPOSITE' ELSE PROVINCE END AS LABEL_1,
                        CASE 
                            WHEN DESCRIPTION = 'National Composite' THEN '11' 
                            WHEN DESCRIPTION LIKE 'Ottawa%' THEN UPPER('Ottawa - Gatineau')
                            ELSE UPPER(DESCRIPTION) 
                        END AS LABEL_2,
                        '{txn_dt}' AS  TXN_DT,
                        '{pub_dt}' AS PUBLCT_DT, 
                        VALUESMOOTHED AS INDEX,
                        PAIRCOUNT AS SLS_PAIR_CNT, 
                        CURRENT_TIMESTAMP AS INSRT_PROCESS_TMSTMP, 
                        CURRENT_TIMESTAMP AS UPDT_PROCESS_TMSTMP
                        FROM  '{rundir}/sq036/hpi_data.parquet'
                        WHERE CMA11Flag = 'Y'
                    ) TO '{prev_mth_pqt}' (FORMAT PARQUET)"""
                )

                files = [curr_mth_pqt, prev_mth_pqt]

                schema = pq.ParquetFile(files[0]).schema_arrow
                with pq.ParquetWriter(tmp_pqt, schema=schema) as writer:
                    for file in files:
                        writer.write_table(pq.read_table(file, schema=schema))
                #need to transform some of the data
                table=pq.read_table(tmp_pqt)
                column_to_update = 'LABEL_2'

                #clean up the data

                remove_slash = remove_unwanted_string(table, column_to_update, char_to_rmv=' / ', char_to_replace='_')
                remove_underscore = remove_unwanted_string(remove_slash, column_to_update, char_to_rmv=' - ', char_to_replace='_')
                remove_period = remove_unwanted_string(remove_underscore, column_to_update, char_to_rmv='. ', char_to_replace='_')
                remove_space = remove_unwanted_string(remove_period, column_to_update, char_to_rmv=' ', char_to_replace='_')
                remove_quote = remove_unwanted_string(remove_space, column_to_update, char_to_rmv='\'', char_to_replace='')
                remove_dash = remove_unwanted_string(remove_quote, column_to_update, char_to_rmv='-', char_to_replace='_')
                new_table = remove_dash
            
                pq.write_table(new_table, final_output_pqt)

                context['task_instance'].xcom_push(key='parquet', value=final_output_pqt)


            @task.duckdb(
                task_id="delete_if_exists_cma11",
                duckdb_conn_id="duckdb-conn",
                sql="""
                DELETE FROM ingestion.TERANET_HOUSE_PRC_INDEX
                WHERE MTH_TM_ID IN ( 
                    {{ task_instance.xcom_pull(task_ids='handle_month_context', key='MTH_TM_ID') }},
                    {{ task_instance.xcom_pull(task_ids='handle_month_context', key='PREV_MTH_TM_ID') }}
                )
                """,
            )
            def delete_if_exists_cma11():
                """ This task deletes existing records for the current month and previous month from the ingestion table. """
                pass


            @task.duckdb(
                task_id="load_cma11",
                duckdb_conn_id="duckdb-conn",
                sql="""
                INSERT INTO ingestion.TERANET_HOUSE_PRC_INDEX BY NAME
                SELECT * FROM '{{ task_instance.xcom_pull(task_ids="sq036.sq036_enrichment.transform_cma11_monthly_data", key="parquet") }}'
                """,
            )
            def load_cma11():
                """
                This task loads the CMA11 data into the target table in DuckDB.
                """
                pass


            """ TaskFlow function definitions """
            consolidated_load_task = load_consolidated_hpi_data()
            cma32_monthly_task = transform_cma32_monthly_data()
            delete_cma32_task = delete_if_exists_cma32()
            cma32_load_task = load_cma32()
            cma11_monthly_task = transform_cma11_monthly_data()
            delete_cma11_task = delete_if_exists_cma11()
            cma11_load_task = load_cma11()

            """ Dependency chaining """
            consolidated_load_task >> [
                delete_cma32_task,
                delete_cma11_task,
                cma11_monthly_task,
                cma32_monthly_task
            ]

            [
                cma32_monthly_task,
                delete_cma32_task
            ] >> cma32_load_task

            [ 
                delete_cma11_task,
                cma11_monthly_task
            ] >> cma11_load_task
        sq036_source_group = sq036_source_group()
        sq036_enrichment_group = sq036_enrichment_group()

        sq036_source_group >> sq036_enrichment_group


    @task(outlets=[AssetAlias("teranet_house_prc_index")])
    def teranet_house_prc_index(*, outlet_events):
        outlet_events[AssetAlias("teranet_house_prc_index")].add(
            Asset("ingestion.TERANET_HOUSE_PRC_INDEX", extra={})
        )


    @task(outlets=[AssetAlias("teranet_house_prc_index_cma")])
    def teranet_house_prc_index_cma(*, outlet_events):
        outlet_events[AssetAlias("teranet_house_prc_index_cma")].add(
            Asset("ingestion.TERANET_HOUSE_PRC_INDEX_CMA", extra={})
        )


    @task(outlets=[AssetAlias("rrap_teranet_consolidated_cma_data")])
    def rrap_teranet_consolidated_cma_data(*, outlet_events):
        outlet_events[AssetAlias("rrap_teranet_consolidated_cma_data")].add(
            Asset("ingestion.RRAP_TERANET_CONSOLIDATED_CMA_DATA", extra={})
        )


    sq036 = sq036_group()
    sq036_start = sq036_start()
    teranet_house_prc_index = teranet_house_prc_index()
    teranet_house_prc_index_cma = teranet_house_prc_index_cma()
    rrap_teranet_consolidated_cma_data = rrap_teranet_consolidated_cma_data()


    sq036_start >> sq036 >> [
        teranet_house_prc_index,
        teranet_house_prc_index_cma,
        rrap_teranet_consolidated_cma_data
    ]
    @task
    def sq037_start():
        """ Manual approval task to start sq037 """
        raise AirflowException("Please mark this task successful to start sequence sq037.")


    @task_group(group_id="sq037")
    def sq037_group():
        """
        TaskGroup for sequence sq037.
        """

        @task_group(group_id="sq037_source")
        def sq037_source_group():
            """
            TaskGroup for source tasks from EDL in sequence sq037.
            """
            # Import of source_group.py
            @task
            def create_sq037_rundir():
                """Create sq037 run directory."""
                context = get_current_context()
                rundir = context["ti"].xcom_pull(task_ids="handle_month_context", key="RUNDIR")
                sq037_rundir = f"{rundir}/sq037"
                os.makedirs(sq037_rundir, exist_ok=True)


            @task.beeline(
                task_id="get_airb_statcan_unemplymnt_rt_ext",
                beeline_conn_id="edlr-conn",
                sql="""
                    select
                        BUSINESSEFFECTIVEDATE AS TIME_KEY,
                        (CASE
                            WHEN TRIM(GEO) = 'Newfoundland and Labrador' THEN 'NF'
                            WHEN TRIM(GEO) = 'Prince Edward Island' THEN 'PE'
                            WHEN TRIM(GEO) = 'Nova Scotia' THEN 'NS'
                            WHEN TRIM(GEO) = 'New Brunswick' THEN 'NB'
                            WHEN TRIM(GEO) = 'Quebec' THEN 'QC'
                            WHEN TRIM(GEO) = 'Ontario' THEN 'ON'
                            WHEN TRIM(GEO) = 'Manitoba' THEN 'MB'
                            WHEN TRIM(GEO) = 'Saskatchewan' THEN 'SK'
                            WHEN TRIM(GEO) = 'Alberta' THEN 'AB'
                            WHEN TRIM(GEO) = 'British Columbia' THEN 'BC'
                            ELSE '99'
                        END) AS PROVINCE,
                        (CAST(VAL AS DECIMAL(10,3)) / 100) AS URATE,
                        (
                            select val / 100
                            FROM {{ var.value.TSZ_B9XF_RRAP_SCHEMA }}.airb_statcan_unemplymnt_rt_ext
                            WHERE businesseffectivedate ='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="MTH_END_DT") }}'
                            AND trim(geo) = 'Canada'
                        ) AS CAN_URATE
                    FROM {{ var.value.TSZ_B9XF_RRAP_SCHEMA }}.airb_statcan_unemplymnt_rt_ext
                    WHERE businesseffectivedate ='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="MTH_END_DT") }}'
                    AND trim(geo) <> 'Canada';
                """,
                rundir="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq037",
                to_parquet=True,
                tmpfileloc="/bns/rrap/data/tmp",
                target="get_airb_statcan_unemplymnt_rt_ext.parquet",
                schema=pa.schema([
                    ('TIME_KEY', pa.date64()),
                    ('PROVINCE', pa.string()),
                    ('URATE', pa.float64()),
                    ('CAN_URATE', pa.float64())
                ]),
            )
            def get_airb_statcan_unemplymnt_rt_ext():
                """
                Extract Statistics Canada unemployment rate data.
    
                Extracts unemployment rate data with province mappings and Canada-wide
                benchmark rate. Transforms geography to 2-letter province codes, converts
                rates from basis points (divided by 100), and filters out Canada-level records.
                """
                pass


            @task.parquet(
                task_id="get_unemp_ratio",
                duckdb_conn_id="duckdb-conn",
                export_params={},
                clear_before_write=True,
                sql="""
                    select *, 
                    (CAST((URATE - CAN_URATE) / CAN_URATE AS DECIMAL(12,9))) AS RATIO
                    FROM '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="RUNDIR") }}/sq037/get_airb_statcan_unemplymnt_rt_ext.parquet'
                """,
                target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq037/get_unemp_ratio.parquet",
            )
            def get_unemp_ratio():
                """
                Compute unemployment rate ratio.
    
                Calculates provincial vs. Canada unemployment rate differential as a ratio:
                (URATE - CAN_URATE) / CAN_URATE.
                """
                pass


            rundir_task = create_sq037_rundir()
            extract_task = get_airb_statcan_unemplymnt_rt_ext()
            transform_task = get_unemp_ratio()

            rundir_task >> extract_task >> transform_task
        @task_group(group_id="sq037_enrichment")
        def sq037_enrichment_group():
            """
            TaskGroup for enrichment tasks in sequence sq037.
            """
            @task.duckdb(
                task_id="delete_if_exists",
                duckdb_conn_id="duckdb-conn",
                sql="""
                    DELETE FROM ingestion.UNEMP_RATE
                    WHERE TIME_KEY = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="MTH_END_DT") }}'
                """,
            )
            def delete_if_exists():
                """Delete existing records for the given TIME_KEY if replace_rows is set to True."""
                pass


            @task.duckdb(
                task_id="load_umemp_rate",
                duckdb_conn_id="duckdb-conn",
                sql="""
                    INSERT INTO ingestion.UNEMP_RATE BY NAME
                    SELECT * FROM {{ task_instance.xcom_pull(task_ids="sq037.sq037_source.get_unemp_ratio", key="parquet") }}
                """,
            )
            def load_unemp_rate():
                """Load the parquet file generated from sq037_source.get_unemp_ratio to duckdb table ingestion.UNEMP_RATE."""
                pass


            """ TaskFlow function definitions """
            delete_if_exists = delete_if_exists()
            load_unemp_rate = load_unemp_rate()

            """ Dependency chaining """
            delete_if_exists >> load_unemp_rate
        sq037_source_group = sq037_source_group()
        sq037_enrichment_group = sq037_enrichment_group()

        sq037_source_group >> sq037_enrichment_group


    @task(outlets=[AssetAlias("unemp_rate")])
    def unemp_rate(*, outlet_events):
        outlet_events[AssetAlias("unemp_rate")].add(
            Asset("ingestion.UNEMP_RATE", extra={})
        )


    sq037 = sq037_group()
    sq037_start = sq037_start()
    unemp_rate = unemp_rate()


    sq037_start >> sq037 >> unemp_rate
    @task
    def sq043_start():
        """ Manual approval task to start sq043 """
        raise AirflowException("Please mark this task successful to start sequence sq043.")


    @task_group(group_id="sq043")
    def sq043_group():
        """
        TaskGroup for sequence sq043.
        """

        @task_group(group_id="sq043_source")
        def sq043_source_group():
            """
            TaskGroup for source tasks from EDL in sequence sq043.
            """
            # Import of source_group.py
            @task
            def create_sq043_rundir():
                """Create sq043 run directory."""
                context = get_current_context()
                rundir = context["ti"].xcom_pull(task_ids="handle_month_context", key="RUNDIR")
                sq043_rundir = f"{rundir}/sq043"
                os.makedirs(sq043_rundir, exist_ok=True)


            @task.beeline(
                task_id="get_airb_statcan_hh_dspsbl_incm_ext",
                beeline_conn_id="edlr-conn",
                sql="""
                    select
                        cast(
                            (CASE 
                                WHEN SUBSTR(ref_dt,6,2) = '01' then concat(substr(ref_dt,1,4),'-03-31')
                                WHEN SUBSTR(ref_dt,6,2) = '04' then concat(substr(ref_dt,1,4),'-06-30')
                                WHEN SUBSTR(ref_dt,6,2) = '07' then concat(substr(ref_dt,1,4),'-09-30')
                                WHEN SUBSTR(ref_dt,6,2) = '10' then concat(substr(ref_dt,1,4),'-12-31')
                                ELSE '0001-01-01' 
                            END)
                        as varchar(10)) as qtr_end_dt,
                        CONCAT(SUBSTR(businesseffectivedate,1,4), SUBSTR(businesseffectivedate,6,2)) AS EFF_FROM_YR_MTH,
                        val as HH_DSPSBL_INCM_MILLNTH_AMT
                    FROM {{ var.value.TSZ_B9XF_RRAP_SCHEMA }}.airb_statcan_hh_dspsbl_incm_ext
                    WHERE businesseffectivedate ='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="MTH_END_DT") }}';
                """,
                rundir="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq043",
                to_parquet=True,
                tmpfileloc="/bns/rrap/data/tmp",
                target="get_airb_statcan_hh_dspsbl_incm_ext.parquet",
                schema=pa.schema([
                    ('qtr_end_dt', pa.string()),
                    ('EFF_FROM_YR_MTH', pa.string()),
                    ('HH_DSPSBL_INCM_MILLNTH_AMT', pa.int64())
                ]),
            )
            def get_airb_statcan_hh_dspsbl_incm_ext():
                """
                Extract Statistics Canada household disposable income data.
    
                Extracts household disposable income from StatCan data, derives quarter-end
                date from reference date (Q1→03-31, Q2→06-30, Q3→09-30, Q4→12-31), and
                formats effective from year-month from business effective date.
                """
                pass


            @task.parquet(
                task_id="hh_dspsbl_incm_qtr_src_extract",
                duckdb_conn_id="duckdb-conn",
                target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq043/iias_hh_dspsbl_incm_qtr_src.parquet",
                sql="""
                    SELECT
                        MTH_TM_ID,
                        EFF_FROM_YR_MTH,
                        EFF_TO_YR_MTH,
                        HH_DSPSBL_INCM_MILLNTH_AMT,
                        CRNT_F,
                        INSRT_PROCESS_TMSTMP,
                        UPDT_PROCESS_TMSTMP
                    FROM {{ params.EDW_schema_EDRTLRP1D }}.HH_DSPSBL_INCM_QTR
                    WHERE CRNT_F = 'Y';
                """,
                export_params={},
                clear_before_write=True,
            )
            def hh_dspsbl_incm_qtr_src_extract():
                """ DuckLake extraction of household disposable income data. """
                pass


            """ TaskFlow function definitions """
            create_sq043_rundir = create_sq043_rundir()
            get_airb_statcan_hh_dspsbl_incm_ext = get_airb_statcan_hh_dspsbl_incm_ext()
            hh_dspsbl_incm_qtr_src_extract = hh_dspsbl_incm_qtr_src_extract()

            """ Dependency chaining """
            create_sq043_rundir >> [
                get_airb_statcan_hh_dspsbl_incm_ext,
                hh_dspsbl_incm_qtr_src_extract
            ]
        @task_group(group_id="sq043_enrichment")
        def sq043_enrichment_group():
            """
            TaskGroup for enrichment tasks in sequence sq043.
            """
            @task.parquet(
                task_id="include_tm_id",
                duckdb_conn_id="duckdb-conn",
                target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq043/include_tm_id.parquet",
                sql="""
                    SELECT
                        j.* EXCLUDE(qtr_end_dt),
                        {{ task_instance.xcom_pull(task_ids='handle_month_context', key='MTH_TM_ID') }} as MTH_TM_ID
                    FROM {{ task_instance.xcom_pull(task_ids='sq043.sq043_source.get_hh_dspsbl_incm_qtr', key='parquet') }} j
                """,
                export_params={},
                clear_before_write=True,
            )
            def include_tm_id():
                """This task adds the MTH_TM_ID to the dataset. """
                pass


            @task.parquet(
                task_id="join_1",
                duckdb_conn_id="duckdb-conn",
                target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq043/join_1.parquet",
                sql="""
                    SELECT
                        j.EFF_FROM_YR_MTH,
                        i.EFF_TO_YR_MTH,
                        j.HH_DSPSBL_INCM_MILLNTH_AMT,
                        j.MTH_TM_ID,
                        i.mth_tm_id as mth_tm_id_nz,
                        i.eff_from_yr_mth as eff_from_yr_mth_nz,
                        i.eff_to_yr_mth as eff_to_yr_mth_nz,
                        i.HH_DSPSBL_INCM_MILLNTH_AMT as HH_DSPSBL_INCM_MILLNTH_AMT_nz,
                        i.crnt_f as crnt_f_nz,
                        i.insrt_process_tmstmp,
                        i.updt_process_tmstmp
                    from {{ task_instance.xcom_pull(task_ids='sq043.sq043_source.include_tm_id', key='parquet') }} j
                    left join {{ task_instance.xcom_pull(task_ids='sq043.sq043_source.get_hh_dspsbl_incm_qtr', key='parquet') }} i
                    on j.MTH_TM_ID = i.MTH_TM_ID
                """,
                export_params={},
                clear_before_write=True,
            )
            def join_1():
                """Joins main dataset with the DuckLake table data on MTH_TM_ID."""
                pass


            @task.parquet(
                task_id="update_crnt_f",
                duckdb_conn_id="duckdb-conn",
                target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq043/update_crnt_f.parquet",
                sql="""
                    SELECT
                        MTH_TM_ID_NZ as MTH_TM_ID,
                        EFF_FROM_YR_MTH_NZ as EFF_FROM_YR_MTH,
                        EFF_TO_YR_MTH_NZ as EFF_TO_YR_MTH,
                        HH_DSPSBL_INCM_MILLNTH_AMT_NZ as HH_DSPSBL_INCM_MILLNTH_AMT,
                        'N' AS CRNT_F,
                        INSRT_PROCESS_TMSTMP,  -- keep the original INSRT_PROCESS_TMSTMP
                        CURRENT_TIMESTAMP as UPDT_PROCESS_TMSTMP
                    from {{ task_instance.xcom_pull(task_ids='sq043.sq043_enrichment.join_1', key='parquet') }}
                    where mth_tm_id_nz is not null and HH_DSPSBL_INCM_MILLNTH_AMT != HH_DSPSBL_INCM_MILLNTH_AMT_NZ
                """,
                export_params={},
                clear_before_write=True,
            )
            def update_crnt_f():
                """The existing records in IIAS that need updates must have their CRNT_F set to 'N'."""
                pass


            @task.parquet(
                task_id="get_update_records",
                duckdb_conn_id="duckdb-conn",
                target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq043/get_update_records.parquet",
                sql="""select
                        MTH_TM_ID_NZ as MTH_TM_ID,
                        EFF_FROM_YR_MTH_NZ as EFF_FROM_YR_MTH,
                        '999912' as EFF_TO_YR_MTH,
                        HH_DSPSBL_INCM_MILLNTH_AMT,
                        'Y' AS CRNT_F,
                        CURRENT_TIMESTAMP as INSRT_PROCESS_TMSTMP,  -- since we're inserting a NEW VERSION of an EXISTING RECORD, new INSRT_PROCESS_TMSTMP
                        NULL as UPDT_PROCESS_TMSTMP
                    from {{ task_instance.xcom_pull(task_ids='sq043.sq043_enrichment.join_1', key='parquet') }}
                    where mth_tm_id_nz is not null and HH_DSPSBL_INCM_MILLNTH_AMT != HH_DSPSBL_INCM_MILLNTH_AMT_NZ
                """,
                export_params={},
                clear_before_write=True,
            )
            def get_update_records():
                """Gets records for which the `MTH_TM_ID` exists in the DuckLake table, but other columns need update."""
                pass


            @task.parquet(
                task_id="get_new_records",
                duckdb_conn_id="duckdb-conn",
                target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq043/get_new_records.parquet",
                sql="""select
                        MTH_TM_ID,
                        EFF_FROM_YR_MTH,
                        '999912' as EFF_TO_YR_MTH,
                        HH_DSPSBL_INCM_MILLNTH_AMT,
                        'Y' as CRNT_F,
                        CURRENT_TIMESTAMP as INSRT_PROCESS_TMSTMP,
                        null as UPDT_PROCESS_TMSTMP
                    from {{ task_instance.xcom_pull(task_ids='sq043.sq043_enrichment.join_1', key='parquet') }}
                    where mth_tm_id_nz is null
                """,
                export_params={},
                clear_before_write=True,
            )
            def get_new_records():
                """Gets net new records (specifically those with net new `MTH_TM_ID`)."""
                pass


            @task.duckdb(
                task_id="delete_if_exists",
                duckdb_conn_id="duckdb-conn",
                sql="""
                    DELETE FROM ingestion.HH_DSPSBL_INCM_QTR
                    WHERE MTH_TM_ID IN (SELECT MTH_TM_ID FROM {{ task_instance.xcom_pull(task_ids='sq043.sq043_enrichment.get_update_records', key='parquet') }})
                    AND CRNT_F = 'Y';
                """,
            )
            def delete_if_exists():
                """Deletes existing records in IIAS that need to be updated (i.e. have the same MTH_TM_ID as the new records)."""
                pass


            @task.duckdb(
                task_id="load_hh_dspsbl_incm_qtr",
                duckdb_conn_id="duckdb-conn",
                sql="""
                    INSERT INTO ingestion.HH_DSPSBL_INCM_QTR BY NAME
                    SELECT * FROM {{ task_instance.xcom_pull(task_ids='sq043.sq043_enrichment.update_crnt_f', key='parquet') }}
                    UNION ALL
                    SELECT * FROM {{ task_instance.xcom_pull(task_ids='sq043.sq043_enrichment.get_update_records', key='parquet') }}
                    UNION ALL
                    SELECT * FROM {{ task_instance.xcom_pull(task_ids='sq043.sq043_enrichment.get_new_records', key='parquet') }}
                """,
            )
            def load_hh_dspsbl_incm_qtr():
                """Loads new and updated records into IIAS table EDRTLRP1D.HH_DSPSBL_INCM_QTR."""
                pass


            """ TaskFlow function definitions """
            include_tm_id = include_tm_id()
            join_1 = join_1()
            update_crnt_f = update_crnt_f()
            get_update_records = get_update_records()
            get_new_records = get_new_records()
            delete_if_exists = delete_if_exists()
            load_hh_dspsbl_incm_qtr = load_hh_dspsbl_incm_qtr()

            """ Dependency chaining """
            include_tm_id >> join_1
            join_1 >> [
                update_crnt_f, 
                get_update_records,
                get_new_records,
                delete_if_exists 
            ] >> load_hh_dspsbl_incm_qtr
        sq043_source_group = sq043_source_group()
        sq043_enrichment_group = sq043_enrichment_group()

        sq043_source_group >> sq043_enrichment_group


    @task(outlets=[AssetAlias("hh_dspsbl_incm_qtr")])
    def hh_dspsbl_incm_qtr(*, outlet_events):
        outlet_events[AssetAlias("hh_dspsbl_incm_qtr")].add(
            Asset("ingestion.HH_DSPSBL_INCM_QTR", extra={})
        )


    sq043 = sq043_group()
    sq043_start = sq043_start()
    hh_dspsbl_incm_qtr = hh_dspsbl_incm_qtr()

    sq043_start >> sq043 >> hh_dspsbl_incm_qtr
    @task
    def sq044_start():
        """ Manual approval task to start sq044 """
        raise AirflowException("Please mark this task successful to start sequence sq044.")


    @task_group(group_id="sq044")
    def sq044_group():
        """
        TaskGroup for sequence sq044.
        """

        @task_group(group_id="sq044_source")
        def sq044_source_group():
            """
            TaskGroup for source tasks from EDL in sequence sq044.
            """
            # Import of source_group.py
            @task
            def create_sq044_rundir():
                """Create sq044 run directory."""
                context = get_current_context()
                rundir = context["ti"].xcom_pull(task_ids="handle_month_context", key="RUNDIR")
                sq044_rundir = f"{rundir}/sq044"
                os.makedirs(sq044_rundir, exist_ok=True)


            @task.beeline(
                task_id="extract_airb_statcan_popn_ext",
                beeline_conn_id="edlr-conn",
                sql="""
                    select
                        ref_dt,
                        val
                    from {{ var.value.TSZ_B9XF_RRAP_SCHEMA }}.airb_statcan_popn_ext
                    where businesseffectivedate = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="MTH_END_DT") }}'
                """,
                rundir="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq044",
                target="extract_airb_statcan_popn_ext.parquet",
                to_parquet=True,
                tmpfileloc="/bns/rrap/data/tmp",
                schema=pa.schema([
                    ('ref_dt', pa.string()),
                    ('val', pa.string())
                ]),
            )
            def extract_airb_statcan_popn_ext():
                """Extract AIRB_STATCAN_POPN_EXT records for the month-end context."""
                pass


            @task.beeline(
                task_id="extract_tm_dim",
                beeline_conn_id="edlr-conn",
                sql="""
                    select
                        tm_id,
                        tm_lvl_end_dt
                    from {{ var.value.RCRR_SCHEMA }}.tm_dim
                    where tm_lvl = 'Month'
                """,
                rundir="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq044",
                to_parquet=True,
                tmpfileloc="/bns/rrap/data/tmp",
                target="extract_tm_dim.parquet",
                schema=pa.schema([
                    ('tm_id', pa.int64()),
                    ('tm_lvl_end_dt', pa.date64())
                ]),
            )
            def extract_tm_dim():
                """Extract monthly time dimension records."""
                pass


            @task.parquet(
                task_id="format_tm_lvl_end_dt",
                duckdb_conn_id="duckdb-conn",
                export_params={},
                clear_before_write=True,
                sql="""
                    select
                        tm_id,
                        tm_lvl_end_dt,
                        strftime(tm_lvl_end_dt, '%Y/%m') as tm_lvl_end_dt_formatted
                    from '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="RUNDIR") }}/sq044/extract_tm_dim.parquet'
                """,
                target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq044/format_tm_lvl_end_dt.parquet",
            )
            def format_tm_lvl_end_dt():
                """Format TM_DIM month-end date into yyyy/MM."""
                pass


            @task.parquet(
                task_id="generate_mth_end_dt",
                duckdb_conn_id="duckdb-conn",
                export_params={},
                clear_before_write=True,
                sql="""
                    select
                        a.tm_id as MTH_TM_ID,
                        a.tm_lvl_end_dt as mth_end_dt,
                        cast(b.val as double) as candn_popn_thsndth_val
                    from '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="RUNDIR") }}/sq044/format_tm_lvl_end_dt.parquet' as a
                    inner join '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="RUNDIR") }}/sq044/extract_airb_statcan_popn_ext.parquet' as b
                        on a.tm_lvl_end_dt_formatted = b.ref_dt
                """,
                target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq044/generate_mth_end_dt.parquet",
            )
            def generate_mth_end_dt():
                """Join TM_DIM and StatCan population extract to derive month-end context."""
                pass


            @task.parquet(
                task_id="make_airb_candn_popn",
                duckdb_conn_id="duckdb-conn",
                export_params={},
                clear_before_write=True,
                sql="""
                    select
                        now() as insrt_process_tmstmp,
                        '"version_code":"0.0.1","batch_id":"24"' as op_field,
                        a.mth_end_dt,
                        a.candn_popn_thsndth_val,
                        a.MTH_TM_ID,
                        '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="MTH_END_DT") }}' as bus_eff_dt
                    from '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="RUNDIR") }}/sq044/generate_mth_end_dt.parquet' as a
                """,
                target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq044/make_airb_candn_popn.parquet",
            )
            def make_airb_candn_popn():
                """Build final AIRB_CANDN_POPN output dataset."""
                pass


            rundir_task = create_sq044_rundir()
            popn_task = extract_airb_statcan_popn_ext()
            tm_dim_task = extract_tm_dim()
            format_task = format_tm_lvl_end_dt()
            mth_end_task = generate_mth_end_dt()
            final_task = make_airb_candn_popn()

            rundir_task >> [popn_task, tm_dim_task]
            tm_dim_task >> format_task
            [format_task, popn_task] >> mth_end_task >> final_task
        @task_group(group_id="sq044_enrichment")
        def sq044_enrichment_group():
            """
            TaskGroup for enrichment tasks in sequence sq044.
            """
            @task.parquet(
                task_id="generate_existing_data",
                duckdb_conn_id="duckdb-conn",
                target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR' )}}/sq044/existing_data.parquet",
                sql="""
                    SELECT
                        MTH_TM_ID,
                        CANDN_POPN_THSNDTH_VAL,
                        INSRT_PROCESS_TMSTMP,
                        UPDT_PROCESS_TMSTMP
                    FROM ingestion.CANDN_POPN_MTH_SNAPSHOT
                """,
                export_params={},
                clear_before_write=True,
            )
            def generate_existing_data():
                """ Task to generate existing data parquet from source table using duckdb. """
                pass


            @task.parquet(
                task_id="generate_new_data",
                duckdb_conn_id="duckdb-conn",
                target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR' )}}/sq044/new_data.parquet",
                sql="""
                    SELECT
                        b.tm_id AS MTH_TM_ID,
                        a.CANDN_POPN_THSNDTH_VAL,
                        CURRENT_TIMESTAMP AS INSRT_PROCESS_TMSTMP,
                        NULL AS UPDT_PROCESS_TMSTMP
                    FROM '{{ task_instance.xcom_pull(task_ids='sq044.sq044_source.make_airb_candn_popn', key='parquet' )}}' AS a
                    INNER JOIN '{{ task_instance.xcom_pull(task_ids='sq044.sq044_source.extract_tm_dim', key='parquet' )}}' AS b
                        ON a.mth_end_dt = b.tm_lvl_end_dt
                """,
                export_params={},
                clear_before_write=True,
            )
            def generate_new_data():
                """ Task to generate new data parquet by joining source parquet with TM_DIM parquet using duckdb. """
                pass


            @task.parquet(
                task_id="merge_insert_update",
                duckdb_conn_id="duckdb-conn",
                target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR' )}}/sq044/merged_data.parquet",
                sql="""
                    SELECT
                        e.MTH_TM_ID,
                        e.CANDN_POPN_THSNDTH_VAL,
                        e.INSRT_PROCESS_TMSTMP,
                        e.UPDT_PROCESS_TMSTMP
                    FROM '{{ task_instance.xcom_pull(task_ids='sq044.generate_existing_data', key='parquet' )}}' AS e
                    UNION
                    SELECT
                        n.MTH_TM_ID,
                        n.CANDN_POPN_THSNDTH_VAL,
                        n.INSRT_PROCESS_TMSTMP,
                        n.UPDT_PROCESS_TMSTMP
                    FROM '{{ task_instance.xcom_pull(task_ids='sq044.generate_new_data', key='parquet' )}}' AS n
                    WHERE n.MTH_TM_ID NOT IN (
                        SELECT MTH_TM_ID FROM '{{ task_instance.xcom_pull(task_ids='sq044.generate_existing_data', key='parquet' )}}'
                    )
                """,
                export_params={},
                clear_before_write=True,
            )
            def merge_insert_update():
                """ Task to merge new and existing data, inserting new records and keeping existing records that were not updated. """
                pass


            @task.parquet(
                task_id="find_records_to_update",
                duckdb_conn_id="duckdb-conn",
                target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR' )}}/sq044/records_to_update.parquet",
                sql="""
                    SELECT
                        e.MTH_TM_ID,
                        e.CANDN_POPN_THSNDTH_VAL,
                        e.INSRT_PROCESS_TMSTMP,
                        CURRENT_TIMESTAMP AS UPDT_PROCESS_TMSTMP
                    FROM '{{ task_instance.xcom_pull(task_ids='sq044.generate_existing_data', key='parquet' )}}' AS e
                    INNER JOIN '{{ task_instance.xcom_pull(task_ids='sq044.generate_new_data', key='parquet' )}}' AS n
                        ON e.MTH_TM_ID = n.MTH_TM_ID
                    WHERE e.CANDN_POPN_THSNDTH_VAL != n.CANDN_POPN_THSNDTH_VAL
                """,
                export_params={},
                clear_before_write=True,
            )
            def find_records_to_update():
                """ Task to find records that require an update by comparing existing and new data parquets using duckdb. """
                pass


            @task.duckdb(
                task_id="delete_old_records",
                duckdb_conn_id="duckdb-conn",
                sql="""
                    DELETE FROM ingestion.CANDN_POPN_MTH_SNAPSHOT
                    WHERE MTH_TM_ID IN (
                        SELECT MTH_TM_ID FROM '{{ task_instance.xcom_pull(task_ids='sq044.find_records_to_update', key='parquet' )}}'
                    )
                """,
            )
            def delete_old_records():
                """ Task to delete old records from source table for MTH_TM_IDs that require an update. """
                pass


            @task.duckdb(
                task_id="insert_updated_records",
                duckdb_conn_id="duckdb-conn",
                sql="""
                    INSERT INTO ingestion.CANDN_POPN_MTH_SNAPSHOT BY NAME
                    SELECT MTH_TM_ID, CANDN_POPN_THSNDTH_VAL, INSRT_PROCESS_TMSTMP, UPDT_PROCESS_TMSTMP
                    FROM '{{ task_instance.xcom_pull(task_ids='sq044.find_records_to_update', key='parquet' )}}'
                """,
            )
            def insert_updated_records():
                """ Task to update existing records in source table from records_to_update parquet. """
                pass


            @task.duckdb(
                task_id="insert_new_records",
                duckdb_conn_id="duckdb-conn",
                sql="""
                    INSERT INTO ingestion.CANDN_POPN_MTH_SNAPSHOT BY NAME
                    SELECT MTH_TM_ID, CANDN_POPN_THSNDTH_VAL, INSRT_PROCESS_TMSTMP, UPDT_PROCESS_TMSTMP
                    FROM '{{ task_instance.xcom_pull(task_ids='sq044.merge_insert_update', key='parquet' )}}'
                """,
            )
            def insert_new_records():
                """ Task to insert new records into source table from merged parquet. """
                pass


            """ TaskFlow function definition """
            generate_existing_data = generate_existing_data()
            generate_new_data = generate_new_data()
            merge_insert_update = merge_insert_update()
            find_records_to_update = find_records_to_update()
            delete_old_records = delete_old_records()
            insert_updated_records = insert_updated_records()
            insert_new_records = insert_new_records()

            """ Dependency chaining """
            [
                generate_existing_data,
                generate_new_data
            ] >> merge_insert_update

            [
                generate_existing_data,
                generate_new_data
            ] >> find_records_to_update

            [
                find_records_to_update,
                merge_insert_update
            ] >> delete_old_records >> insert_updated_records >> insert_new_records
        sq044_source_group = sq044_source_group()
        sq044_enrichment_group = sq044_enrichment_group()

        sq044_source_group >> sq044_enrichment_group


    @task(outlets=[AssetAlias("candn_popn_mth_snapshot")])
    def candn_popn_mth_snapshot(*, outlet_events):
        outlet_events[AssetAlias("candn_popn_mth_snapshot")].add(
            Asset("ingestion.CANDN_POPN_MTH_SNAPSHOT", extra={})
        )


    sq044 = sq044_group()
    sq044_start = sq044_start()
    candn_popn_mth_snapshot = candn_popn_mth_snapshot()

    sq044_start >> sq044 >> candn_popn_mth_snapshot
    @task
    def sq0051_start():
        """ Manual approval task to start sq0051 """
        raise AirflowException("Please mark this task successful to start sequence sq0051.")


    @task_group(group_id="sq0051")
    def sq0051_group():
        """
        TaskGroup for sequence sq0051.
        """

        @task_group(group_id="sq0051_source")
        def sq0051_source_group():
            """
            TaskGroup for source tasks from EDL in sequence sq0051.
            """
            # Import of source_group.py
            @task
            def create_sq0051_rundir():
                """Create sq0051 run directory."""
                context = get_current_context()
                rundir = context["ti"].xcom_pull(task_ids="handle_month_context", key="RUNDIR")
                sq0051_rundir = f"{rundir}/sq0051"
                os.makedirs(sq0051_rundir, exist_ok=True)


            @task.beeline(
                task_id="make_airb_ifrs9_ecl_profile_fact",
                beeline_conn_id="edlr-conn",
                sql="""
                    select
                        cntry_cd,
                        trim(acct_num) as acct_num,
                        cpp_entity_folio_cd,
                        cpp_prd_folio_cd,
                        cpp_quali_sub_cd,
                        cpp_quanti_sub_cd,
                        pit_stat_cd,
                        stg3_ind,
                        os_bal_amt,
                        final_ecl_stage,
                        final_ecl_cad,
                        final_ecl_cad_drawn,
                        final_ecl_cad_undrawn,
                        crnt_auth_lmt_amt,
                        undrawn_amt,
                        scored_unscored_ind,
                        from_unixtime(unix_timestamp(cast(proc_mth_id as string), 'yyyyMMdd'), 'yyyy-MM-dd') as mth_end_dt,
                        case src_sys_cd
                            when 'GZ' then 'MOR'
                            when 'KQ' then 'KS'
                            when 'SL' then 'SPL'
                            when 'TNG_MTG' then 'TNG-MOR'
                            else src_sys_cd
                        end as src_sys_cd
                    from {{ var.value.CRZ_IFRS9_RTL_SCHEMA }}.ifrs9_acct_ecl_profile_fact_ext
                    where
                        cast(proc_mth_id as string) = regexp_replace('{{ task_instance.xcom_pull(task_ids="handle_month_context", key="PREV_MTH_END_DT") }}', '-', '')
                        and cntry_cd = 'CA'
                    ;
                """,
                rundir="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq0051",
                to_parquet=True,
                tmpfileloc="/bns/rrap/data/tmp",
                target="airb_ifrs9_ecl_profile_fact.parquet",
                schema=pa.schema([
                    ('cntry_cd', pa.string()),
                    ('acct_num', pa.string()),
                    ('cpp_entity_folio_cd', pa.string()),
                    ('cpp_prd_folio_cd', pa.string()),
                    ('cpp_quali_sub_cd', pa.string()),
                    ('cpp_quanti_sub_cd', pa.string()),
                    ('pit_stat_cd', pa.string()),
                    ('stg3_ind', pa.int32()),
                    ('os_bal_amt', pa.float64()),
                    ('final_ecl_stage', pa.int32()),
                    ('final_ecl_cad', pa.float64()),
                    ('final_ecl_cad_drawn', pa.float64()),
                    ('final_ecl_cad_undrawn', pa.float64()),
                    ('crnt_auth_lmt_amt', pa.float64()),
                    ('undrawn_amt', pa.float64()),
                    ('scored_unscored_ind', pa.string()),
                    ('mth_end_dt', pa.string()),
                    ('src_sys_cd', pa.string()),
                ]),
            )
            def make_airb_ifrs9_ecl_profile_fact():
                """Extract account-level IFRS9 ECL attributes for prior month-end."""
                pass


            rundir_task = create_sq0051_rundir()
            extract_task = make_airb_ifrs9_ecl_profile_fact()

            rundir_task >> extract_task
        @task_group(group_id="sq0051_enrichment")
        def sq0051_enrichment_group():
            """
            TaskGroup for enrichment tasks in sequence sq0051.
            """
            pass

        sq0051_source_group = sq0051_source_group()
        sq0051_enrichment_group = sq0051_enrichment_group()

        sq0051_source_group >> sq0051_enrichment_group


    @task(outlets=[AssetAlias("basel_ifrs9_ecl_profile_fact")])
    def basel_ifrs9_ecl_profile_fact(*, outlet_events):
        outlet_events[AssetAlias("basel_ifrs9_ecl_profile_fact")].add(
            Asset("ingestion.BASEL_IFRS9_ECL_PROFILE_FACT", extra={})
        )


    sq0051 = sq0051_group()
    sq0051_start = sq0051_start()
    basel_ifrs9_ecl_profile_fact = basel_ifrs9_ecl_profile_fact()

    sq0051_start >> sq0051 >> basel_ifrs9_ecl_profile_fact
    @task
    def sq083_start():
        """ Manual approval task to start sq083 """
        raise AirflowException("Please mark this task successful to start sequence sq083.")


    @task_group(group_id="sq083")
    def sq083_group():
        """
        TaskGroup for sequence sq083.
        """

        @task_group(group_id="sq083_source")
        def sq083_source_group():
            """
            TaskGroup for source tasks from EDL in sequence sq083.
            """
            # Import of source_group.py
            @task
            def create_sq083_rundir():
                """Create sq083 run directory."""
                context = get_current_context()
                rundir = context["ti"].xcom_pull(task_ids="handle_month_context", key="RUNDIR")
                sq083_rundir = f"{rundir}/sq083"
                os.makedirs(sq083_rundir, exist_ok=True)


            @task.beeline(
                task_id="get_airb_mbr_src",
                beeline_conn_id="edlr-conn",
                sql="""
                    select
                        transit,
                        pplan,
                        alphcurr,
                        decum,
                        ytd,
                        date_format(current_timestamp, 'yyyy-MM-dd HH:mm:ss.SSSSSS') as insrt_process_tmstmp,
                        date_format(current_timestamp, 'yyyy-MM-dd HH:mm:ss.SSSSSS') as updt_process_tmstmp
                    from {{ var.value.TSZ_B9XF_RRAP_SCHEMA }}.airb_mbr_src
                    where businesseffectivedate = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="MTH_END_DT") }}';
                """,
                rundir="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq083",
                to_parquet=True,
                tmpfileloc="/bns/rrap/data/tmp",
                target="airb_mbr_src.parquet",
                schema=pa.schema([
                    ("TRANSIT", pa.string()),
                    ("PPLAN", pa.string()),
                    ("ALPHCURR", pa.string()),
                    ("DECUM", pa.float64()),
                    ("YTD", pa.float64()),
                    ("INSRT_PROCESS_TMSTMP", pa.string()),
                    ("UPDT_PROCESS_TMSTMP", pa.string()),
                ]),
            )
            def get_airb_mbr_src():
                """Extract AIRB MBR source records for month-end."""
                pass


            rundir_task = create_sq083_rundir()
            extract_task = get_airb_mbr_src()

            rundir_task >> extract_task
        @task_group(group_id="sq083_enrichment")
        def sq083_enrichment_group():
            """
            TaskGroup for enrichment tasks in sequence sq083.
            """
            @task.duckdb(
                task_id="load_mbr_src_curr",
                duckdb_conn_id="duckdb-conn",
                sql="""
                    INSERT INTO ingestion.MBR_SRC_CURR BY NAME
                    SELECT * FROM '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq083/airb_mbr_src.parquet'
                """,
            )
            def load_mbr_src_curr():
                """Load AIRB MBR source records into MBR_SRC_CURR."""
                pass

            """ TaskFlow function definitions """
            load_mbr_src_curr_task = load_mbr_src_curr()

            """ Dependency chaining """
        sq083_source_group = sq083_source_group()
        sq083_enrichment_group = sq083_enrichment_group()

        sq083_source_group >> sq083_enrichment_group


    @task(outlets=[AssetAlias("mbr_src_curr")])
    def mbr_src_curr(*, outlet_events):
        outlet_events[AssetAlias("mbr_src_curr")].add(
            Asset("ingestion.MBR_SRC_CURR", extra={})
        )


    sq083 = sq083_group()
    sq083_start = sq083_start()
    mbr_src_curr = mbr_src_curr()

    sq083_start >> sq083 >> mbr_src_curr

    
    @task
    def sq084_start():
        """ Manual approval task to start sq084 """
        raise AirflowException("Please mark this task successful to start sequence sq084.")
    

    @task_group(group_id="sq084")
    def sq084_group():
        """
        TaskGroup for sequence sq084.
        """

        @task_group(group_id="sq084_source")
        def sq084_source_group():
            """
            TaskGroup for source tasks from EDL in sequence sq084.
            """
            # Import of source_group.py
            @task
            def create_sq084_rundir():
                """Create sq084 run directory."""
                context = get_current_context()
                rundir = context["ti"].xcom_pull(task_ids="handle_month_context", key="RUNDIR")
                sq084_rundir = f"{rundir}/sq084"
                os.makedirs(sq084_rundir, exist_ok=True)


            @task.beeline(
                task_id="get_cbs_mdm_flags",
                beeline_conn_id="edlr-conn",
                sql="""
                    select *
                    from crz_cust_scorecard.cbs_mdm_flags
                    where eff_dt = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="MTH_END_DT") }}';
                """,
                rundir="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq084",
                to_parquet=True,
                strings_can_be_null=True,
                tmpfileloc="/bns/rrap/data/tmp",
                target="cbs_mdm_flags.parquet",
                schema=None,
            )
            def get_cbs_mdm_flags():
                """Extract crz_cust_scorecard.cbs_mdm_flags for month-end."""
                pass


            rundir_task = create_sq084_rundir()
            extract_task = get_cbs_mdm_flags()

            rundir_task >> extract_task
        @task_group(group_id="sq084_enrichment")
        def sq084_enrichment_group():
            """
            TaskGroup for enrichment tasks in sequence sq084.
            """
            @task.duckdb(
                task_id="delete_cbs_mdm_flags",
                duckdb_conn_id="duckdb-conn",
                sql="""
                    DELETE FROM emulated.CBS_MDM_FLAGS
                    WHERE EFF_DT = DATE '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="MTH_END_DT") }}'
                      AND STREAM = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
                """,
            )
            def delete_cbs_mdm_flags():
                """Clear the (EFF_DT, STREAM) partition before reload (idempotent re-runs)."""
                pass


            @task.duckdb(
                task_id="load_cbs_mdm_flags",
                duckdb_conn_id="duckdb-conn",
                sql="""
                    INSERT INTO emulated.CBS_MDM_FLAGS BY NAME
                    SELECT
                        * EXCLUDE (op_field),
                        'CBS' AS STREAM,
                        {{ task_instance.xcom_pull(task_ids="handle_month_context", key="MTH_TM_ID") }} AS MTH_TM_ID,
                        CURRENT_TIMESTAMP AS UPDT_PROCESS_TMSTMP
                    FROM '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq084/cbs_mdm_flags.parquet'
                """,
            )
            def load_cbs_mdm_flags():
                """
                Load extracted MDM flags into emulated.CBS_MDM_FLAGS.
                Bare SELECT * from the parquet, minus op_field, plus the target-only columns
                STREAM / MTH_TM_ID / UPDT_PROCESS_TMSTMP. All other Hive columns map by name
                (eff_dt->EFF_DT, bnkrptcy_flag->BNKRPTCY_FLAG, etc.).
                """
                pass


            """ TaskFlow function definitions """
            delete_cbs_mdm_flags_task = delete_cbs_mdm_flags()
            load_cbs_mdm_flags_task = load_cbs_mdm_flags()

            """ Dependency chaining """
            delete_cbs_mdm_flags_task >> load_cbs_mdm_flags_task
        sq084_source_group = sq084_source_group()
        sq084_enrichment_group = sq084_enrichment_group()

        sq084_source_group >> sq084_enrichment_group


    @task(outlets=[AssetAlias("cbs_mdm_flags")])
    def cbs_mdm_flags(*, outlet_events):
        outlet_events[AssetAlias("cbs_mdm_flags")].add(
            Asset("emulated.CBS_MDM_FLAGS", extra={})
        )


    sq084 = sq084_group()
    sq084_start = sq084_start()
    cbs_mdm_flags = cbs_mdm_flags()

    sq084_start >> sq084 >> cbs_mdm_flags
    """
    Dependencies from handle_month_context to the first sequence
    """
    handle_month_context >> [
        sq0051, sq035, sq083, sq043,
        sq044, sq037, sq036, sq002, sq003,
        sq004, sq015, sq033, sq034, sq018,
        sq011, sq020, sq019, sq006, sq016,
        sq005, sq023, sq001, sq008, sq007,
        sq084
    ]

    """ Actual dependencies between sequences based on data."""
    basel_cust_dim >> [
        sq004, 
        sq005,
        sq006,
        sq007
    ]
    basel_acct_dim >> [
        sq004,
        sq006,
        sq007,
        sq008,
        sq016,
        sq0051
    ]
    org_unit_dim >> [
        sq005,
        sq006,
        sq007,
        sq008
    ]
    basel_step_pln_mth_snapshot >> [
        sq006,
        sq007,
        sq008
    ]
    basel_cust_acct_rltnp_snapshot >> [
        sq008
    ]


source_ingestion()
