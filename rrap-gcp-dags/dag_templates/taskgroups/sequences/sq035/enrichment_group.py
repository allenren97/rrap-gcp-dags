from airflow.sdk import task


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
