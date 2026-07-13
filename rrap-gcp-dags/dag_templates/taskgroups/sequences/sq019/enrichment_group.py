from airflow.sdk import task


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
