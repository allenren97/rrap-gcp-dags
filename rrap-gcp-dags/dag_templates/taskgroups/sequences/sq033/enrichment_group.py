from airflow.sdk import task


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
