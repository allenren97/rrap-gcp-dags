from airflow.sdk import task
from airflow.exceptions import AirflowException
from bns.rrap.hooks.duckdb import DuckLakeHook


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
