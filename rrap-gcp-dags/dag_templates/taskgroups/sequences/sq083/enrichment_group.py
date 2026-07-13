from airflow.sdk import task


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