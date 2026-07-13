from airflow.sdk import task


@task.duckdb(
    task_id="delete_cbs_mdm_flags",
    duckdb_conn_id="duckdb-conn",
    sql="""
        DELETE FROM emulated.CBS_MDM_FLAGS
        WHERE EFF_DT = DATE '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="MTH_END_DT") }}'
          AND STREAM = 'CBS'
    """,
)
def delete_cbs_mdm_flags():
    """Clear the (EFF_DT, STREAM) partition before reload."""
    pass


@task.duckdb(
    task_id="load_cbs_mdm_flags",
    duckdb_conn_id="duckdb-conn",
    sql="""
        INSERT INTO emulated.CBS_MDM_FLAGS BY NAME
        SELECT
            *,
            'CBS' AS STREAM,
            {{ task_instance.xcom_pull(task_ids="handle_month_context", key="MTH_TM_ID") }} AS MTH_TM_ID,
            CURRENT_TIMESTAMP AS UPDT_PROCESS_TMSTMP
        FROM '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq084/cbs_mdm_flags.parquet'
    """,
)
def load_cbs_mdm_flags():
    """
    Load extracted MDM flags into emulated.CBS_MDM_FLAGS.
    """
    pass


""" TaskFlow function definitions """
delete_cbs_mdm_flags_task = delete_cbs_mdm_flags()
load_cbs_mdm_flags_task = load_cbs_mdm_flags()

""" Dependency chaining """
delete_cbs_mdm_flags_task >> load_cbs_mdm_flags_task