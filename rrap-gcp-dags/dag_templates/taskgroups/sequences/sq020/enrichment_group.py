from airflow.sdk import task


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
