from airflow.sdk import task


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
