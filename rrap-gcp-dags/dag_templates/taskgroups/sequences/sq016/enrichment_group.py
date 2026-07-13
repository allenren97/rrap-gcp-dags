from airflow.sdk import task
from airflow.exceptions import AirflowException
from bns.rrap.hooks.duckdb import DuckLakeHook


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
