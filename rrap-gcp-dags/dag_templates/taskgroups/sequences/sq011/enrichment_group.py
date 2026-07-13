from airflow.sdk import task


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
