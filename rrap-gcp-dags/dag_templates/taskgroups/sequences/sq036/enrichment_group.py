from airflow.sdk import task, get_current_context
import unicodedata
import pyarrow.compute as pc
import pyarrow as pa
import pyarrow.parquet as pq
from bns.rrap.hooks.duckdb import DuckLakeHook
import pendulum


# Utility functions to strip accents and unwanted characters
def strip_accents_arrow(array):
    def strip_accents_python(s):
        return ''.join(
            c for c in unicodedata.normalize('NFD', s)
            if unicodedata.category(c) != 'Mn'
        )

    return pa.array([strip_accents_python(s.as_py()) for s in array])


def remove_unwanted_string(table, column_name, char_to_rmv:str, char_to_replace:str):
    if column_name in table.column_names:
        column = table[column_name]
        if pa.types.is_string(column.type):
            trimmed_column = pc.utf8_trim_whitespace(column)
            no_accent = strip_accents_arrow(trimmed_column)
            new_column = pc.replace_substring(no_accent, pattern=char_to_rmv, replacement=char_to_replace)
            return table.set_column(table.column_names.index(column_name), column_name, new_column)
    return table


@task.duckdb(
    task_id="load_consolidate_hpi_data",
    duckdb_conn_id="duckdb-conn",
    sql="""
    INSERT INTO ingestion.RRAP_TERANET_CONSOLIDATED_CMA_DATA BY NAME
    SELECT * FROM '{{ task_instance.xcom_pull(task_ids="sq036.sq036_source.consolidate_hpi_data", key="parquet") }}'
    """,
)
def load_consolidated_hpi_data():
    """
    This task loads the consolidated HPI data into the target table in DuckDB.
    """
    pass


@task.parquet(
    task_id="create_hpi_data",
    duckdb_conn_id="duckdb-conn",
    export_params={},
    clear_before_write=True,
    sql="""
        select
            year,
            month,
            regiontype,
            regionid,
            propertytype,
            paircount,
            valueraw,
            valuesmoothed,
            mth_end_dt
        from '{{ task_instance.xcom_pull(task_ids="sq036.sq036_source.consolidate_hpi_data", key="parquet") }}'
        where regiontype = 'CMA' and propertytype = 'Total'
    """,
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq036/hpi_data.parquet",
)
def create_hpi_data():
    """
    This task creates a Parquet file filtering the consolidated HPI data for records where the 
    region type is 'CMA' and the property type is 'Total'.
    """
    pass


@task
def transform_cma32_monthly_data():
    """
    This task applies necessary transformations to the CMA32 monthly data, such as stripping accents and removing unwanted characters.
    """
    hook = DuckLakeHook(duckdb_conn_id="duckdb-conn")
    context = get_current_context()

    # Setup rundir for transformation outputs
    rundir = context['task_instance'].xcom_pull(task_ids='handle_month_context', key='RUNDIR')
    curr_mth_pqt = f"{rundir}/sq036/cma32_monthly.parquet"
    prev_mth_pqt = f"{rundir}/sq036/cma32_monthly_prev.parquet"
    tmp_pqt = f"{rundir}/sq036/tmp_cma32_monthly.parquet"
    final_output_pqt = f"{rundir}/sq036/cma32_data.parquet"

    # setup dt variables for queries
    mth_tm_id = context['task_instance'].xcom_pull(task_ids='handle_month_context', key='MTH_TM_ID')
    prev_mth_tm_id = context['task_instance'].xcom_pull(task_ids='handle_month_context', key='PREV_MTH_TM_ID')
    txn_dt = context['task_instance'].xcom_pull(task_ids='handle_month_context', key='MTH_END_DT').strftime('%Y-%m-%d')
    pub_dt = context['task_instance'].xcom_pull(task_ids='handle_month_context', key='MTH_END_DT').add(months=1).strftime('%Y-%m-%d')

    hook.sql(f"""
        COPY (
            SELECT {mth_tm_id} AS MTH_TM_ID,
            CASE WHEN PROVINCE = 'CA' THEN 'COMPOSITE' ELSE PROVINCE END AS LABEL_1,
            CASE 
                WHEN DESCRIPTION = 'National Composite' THEN '11' 
                WHEN DESCRIPTION LIKE 'Ottawa%' THEN UPPER('Ottawa - Gatineau')
                ELSE UPPER(DESCRIPTION) 
            END AS LABEL_2,
            '{txn_dt}' AS  TXN_DT,
            '{pub_dt}' AS PUBLCT_DT, 
            VALUESMOOTHED AS INDEX,
			PAIRCOUNT AS SLS_PAIR_CNT, 
            CURRENT_TIMESTAMP AS INSRT_PROCESS_TMSTMP, 
			CURRENT_TIMESTAMP AS UPDT_PROCESS_TMSTMP
			FROM  '{rundir}/sq036/hpi_data.parquet'
            WHERE CMA11Flag = 'Y'
        ) TO '{curr_mth_pqt}' (FORMAT PARQUET)
    """)

    hook.sql(f"""
        COPY (
            SELECT 
            {prev_mth_tm_id} AS MTH_TM_ID, 
			CASE WHEN PROVINCE = 'CA' THEN 'COMPOSITE' ELSE PROVINCE END AS LABEL_1,
            CASE 
                WHEN DESCRIPTION = 'National Composite' THEN '11' 
                WHEN DESCRIPTION LIKE 'Ottawa%' THEN UPPER('Ottawa_Gatineau')
                ELSE DESCRIPTION 
            END AS LABEL_2, 
            CASE 
                WHEN DESCRIPTION = 'National Composite' THEN '11' 
                WHEN DESCRIPTION LIKE 'Ottawa%' THEN UPPER('Ottawa - Gatineau')
                ELSE UPPER(DESCRIPTION)  
            END AS LABEL_2_ORIG,
            VALUESMOOTHED AS INDEX,
			PAIRCOUNT AS SLS_PAIR_CNT, 
            CURRENT_TIMESTAMP AS INSRT_PROCESS_TMSTMP, 
			CURRENT_TIMESTAMP AS UPDT_PROCESS_TMSTMP
            
			FROM  '{rundir}/sq036/hpi_data.parquet'
        ) TO '{prev_mth_pqt}' (FORMAT PARQUET)
    """)

    files = [ curr_mth_pqt, prev_mth_pqt ]

    schema = pq.ParquetFile(files[0]).schema_arrow
    with pq.ParquetWriter(tmp_pqt, schema=schema) as writer:
        for file in files:
            table = pq.read_table(file, schema=schema)
            writer.write_table(table)

    table=pq.read_table(tmp_pqt)
    column_to_update = 'LABEL_2_ORIG'
    second_column_to_update = 'LABEL_2'
    
    remove_slash = remove_unwanted_string(table, column_to_update, char_to_rmv=' / ', char_to_replace='_')
    remove_underscore = remove_unwanted_string(remove_slash, column_to_update, char_to_rmv=' - ', char_to_replace='_')
    remove_period = remove_unwanted_string(remove_underscore, column_to_update, char_to_rmv='. ', char_to_replace='_')
    remove_space = remove_unwanted_string(remove_period, column_to_update, char_to_rmv=' ', char_to_replace='_')
    remove_quote = remove_unwanted_string(remove_space, column_to_update, char_to_rmv='\'', char_to_replace='')
    remove_second_quote = remove_unwanted_string(remove_quote, second_column_to_update, char_to_rmv='\'', char_to_replace='')
    remove_dash = remove_unwanted_string(remove_second_quote, column_to_update, char_to_rmv='-', char_to_replace='_')
    new_table = remove_dash

    #insert composite 6 row
    current_time = pendulum.now()
    composite_6 = {
        "MTH_TM_ID": [mth_tm_id],
        "LABEL_1": ['COMPOSITE'],
        "LABEL_2": ['6'],
        "LABEL_2_ORIG": ['6'],
        "INDEX":[None],
        "SLS_PAIR_CNT":[None],
        "INSRT_PROCESS_TMSTMP" : [current_time],
        "UPDT_PROCESS_TMSTMP" : [current_time]

    }
    prev_composite_6 = {
        "MTH_TM_ID": [prev_mth_tm_id],
        "LABEL_1": ['COMPOSITE'],
        "LABEL_2": ['6'],
        "LABEL_2_ORIG": ['6'],
        "INDEX":[None],
        "SLS_PAIR_CNT":[None],
        "INSRT_PROCESS_TMSTMP" : [current_time],
        "UPDT_PROCESS_TMSTMP" : [current_time]

    }

    new_row_1 = pa.RecordBatch.from_pydict(composite_6)
    new_row_2 = pa.RecordBatch.from_pydict(prev_composite_6)
    

    new_row_table = pa.Table.from_batches([new_row_1, new_row_2])
    new_row_table = new_row_table.cast(new_table.schema)
    combined_table = pa.concat_tables([new_table, new_row_table])
    pq.write_table(combined_table, final_output_pqt)

    context['task_instance'].xcom_push(key='parquet', value=final_output_pqt)


@task.duckdb(
    task_id="delete_if_exists_cma32",
    duckdb_conn_id="duckdb-conn",
    sql="""
    DELETE FROM ingestion.TERANET_HOUSE_PRC_INDEX_CMA
    WHERE MTH_TM_ID IN ( 
        {{ task_instance.xcom_pull(task_ids='handle_month_context', key='MTH_TM_ID') }},
        {{ task_instance.xcom_pull(task_ids='handle_month_context', key='PREV_MTH_TM_ID') }}
    )
    """,
)
def delete_if_exists_cma32():
    """ This task deletes existing records for the current month and previous month from the ingestion table. """
    pass    


@task.duckdb(
    task_id="load_cma32",
    duckdb_conn_id="duckdb-conn",
    sql="""
    INSERT INTO ingestion.TERANET_HOUSE_PRC_INDEX_CMA BY NAME
    SELECT * FROM '{{ task_instance.xcom_pull(task_ids="sq036.sq036_enrichment.create_parquet_cma32", key="parquet") }}'
    """,
)
def load_cma32():
    """
    This task loads the CMA32 data into the target table in DuckDB.
    """
    pass


@task
def transform_cma11_monthly_data():
    """
    This task applies necessary transformations to the CMA11 monthly data, such as stripping accents and removing unwanted characters.
    """
    hook = DuckLakeHook(duckdb_conn_id="duckdb-conn")
    context = get_current_context()

    # Setup rundir for transformation outputs
    rundir = context['task_instance'].xcom_pull(task_ids='handle_month_context', key='RUNDIR')
    curr_mth_pqt = f"{rundir}/sq036/cma11_monthly.parquet"
    prev_mth_pqt = f"{rundir}/sq036/cma11_monthly_prev.parquet"
    tmp_pqt = f"{rundir}/sq036/tmp_cma11_monthly.parquet"
    final_output_pqt = f"{rundir}/sq036/cma11_data.parquet"

    # setup dt variables for queries
    mth_tm_id = context['task_instance'].xcom_pull(task_ids='handle_month_context', key='MTH_TM_ID')
    prev_mth_tm_id = context['task_instance'].xcom_pull(task_ids='handle_month_context', key='PREV_MTH_TM_ID')
    txn_dt = context['task_instance'].xcom_pull(task_ids='handle_month_context', key='MTH_END_DT').strftime('%Y-%m-%d')
    pub_dt = context['task_instance'].xcom_pull(task_ids='handle_month_context', key='MTH_END_DT').add(months=1).strftime('%Y-%m-%d')

    hook.sql(f"""
        COPY (
            SELECT {mth_tm_id} AS MTH_TM_ID, 
            CASE WHEN PROVINCE = 'CA' THEN 'COMPOSITE' ELSE PROVINCE END AS LABEL_1,
            CASE 
                WHEN DESCRIPTION = 'National Composite' THEN '11' 
                WHEN DESCRIPTION LIKE 'Ottawa%' THEN UPPER('Ottawa - Gatineau')
                ELSE UPPER(DESCRIPTION) 
            END AS LABEL_2,
            '{txn_dt}' AS  TXN_DT,
            '{pub_dt}' AS PUBLCT_DT, 
            VALUESMOOTHED AS INDEX,
            PAIRCOUNT AS SLS_PAIR_CNT, 
            CURRENT_TIMESTAMP AS INSRT_PROCESS_TMSTMP, 
            CURRENT_TIMESTAMP AS UPDT_PROCESS_TMSTMP
            FROM  '{rundir}/sq036/hpi_data.parquet'
            WHERE CMA11Flag = 'Y'
        ) TO '{curr_mth_pqt}' (FORMAT PARQUET)
    """)

    hook.sql(f"""
        COPY (
            SELECT {prev_mth_tm_id} AS MTH_TM_ID, 
            CASE WHEN PROVINCE = 'CA' THEN 'COMPOSITE' ELSE PROVINCE END AS LABEL_1,
            CASE 
                WHEN DESCRIPTION = 'National Composite' THEN '11' 
                WHEN DESCRIPTION LIKE 'Ottawa%' THEN UPPER('Ottawa - Gatineau')
                ELSE UPPER(DESCRIPTION) 
            END AS LABEL_2,
            '{txn_dt}' AS  TXN_DT,
            '{pub_dt}' AS PUBLCT_DT, 
            VALUESMOOTHED AS INDEX,
            PAIRCOUNT AS SLS_PAIR_CNT, 
            CURRENT_TIMESTAMP AS INSRT_PROCESS_TMSTMP, 
            CURRENT_TIMESTAMP AS UPDT_PROCESS_TMSTMP
            FROM  '{rundir}/sq036/hpi_data.parquet'
            WHERE CMA11Flag = 'Y'
        ) TO '{prev_mth_pqt}' (FORMAT PARQUET)"""
    )

    files = [curr_mth_pqt, prev_mth_pqt]

    schema = pq.ParquetFile(files[0]).schema_arrow
    with pq.ParquetWriter(tmp_pqt, schema=schema) as writer:
        for file in files:
            writer.write_table(pq.read_table(file, schema=schema))
    #need to transform some of the data
    table=pq.read_table(tmp_pqt)
    column_to_update = 'LABEL_2'

    #clean up the data

    remove_slash = remove_unwanted_string(table, column_to_update, char_to_rmv=' / ', char_to_replace='_')
    remove_underscore = remove_unwanted_string(remove_slash, column_to_update, char_to_rmv=' - ', char_to_replace='_')
    remove_period = remove_unwanted_string(remove_underscore, column_to_update, char_to_rmv='. ', char_to_replace='_')
    remove_space = remove_unwanted_string(remove_period, column_to_update, char_to_rmv=' ', char_to_replace='_')
    remove_quote = remove_unwanted_string(remove_space, column_to_update, char_to_rmv='\'', char_to_replace='')
    remove_dash = remove_unwanted_string(remove_quote, column_to_update, char_to_rmv='-', char_to_replace='_')
    new_table = remove_dash
            
    pq.write_table(new_table, final_output_pqt)

    context['task_instance'].xcom_push(key='parquet', value=final_output_pqt)


@task.duckdb(
    task_id="delete_if_exists_cma11",
    duckdb_conn_id="duckdb-conn",
    sql="""
    DELETE FROM ingestion.TERANET_HOUSE_PRC_INDEX
    WHERE MTH_TM_ID IN ( 
        {{ task_instance.xcom_pull(task_ids='handle_month_context', key='MTH_TM_ID') }},
        {{ task_instance.xcom_pull(task_ids='handle_month_context', key='PREV_MTH_TM_ID') }}
    )
    """,
)
def delete_if_exists_cma11():
    """ This task deletes existing records for the current month and previous month from the ingestion table. """
    pass


@task.duckdb(
    task_id="load_cma11",
    duckdb_conn_id="duckdb-conn",
    sql="""
    INSERT INTO ingestion.TERANET_HOUSE_PRC_INDEX BY NAME
    SELECT * FROM '{{ task_instance.xcom_pull(task_ids="sq036.sq036_enrichment.transform_cma11_monthly_data", key="parquet") }}'
    """,
)
def load_cma11():
    """
    This task loads the CMA11 data into the target table in DuckDB.
    """
    pass


""" TaskFlow function definitions """
consolidated_load_task = load_consolidated_hpi_data()
cma32_monthly_task = transform_cma32_monthly_data()
delete_cma32_task = delete_if_exists_cma32()
cma32_load_task = load_cma32()
cma11_monthly_task = transform_cma11_monthly_data()
delete_cma11_task = delete_if_exists_cma11()
cma11_load_task = load_cma11()

""" Dependency chaining """
consolidated_load_task >> [
    delete_cma32_task,
    delete_cma11_task,
    cma11_monthly_task,
    cma32_monthly_task
]

[
    cma32_monthly_task,
    delete_cma32_task
] >> cma32_load_task

[ 
    delete_cma11_task,
    cma11_monthly_task
] >> cma11_load_task
