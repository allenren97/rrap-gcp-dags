from airflow.sdk import task


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
