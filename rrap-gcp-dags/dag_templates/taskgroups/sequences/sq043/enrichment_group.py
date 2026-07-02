from airflow.sdk import task


@task.parquet(
    task_id="include_tm_id",
    duckdb_conn_id="duckdb-conn",
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq043/include_tm_id.parquet",
    sql="""
        SELECT
            j.* EXCLUDE(qtr_end_dt),
            {{ task_instance.xcom_pull(task_ids='handle_month_context', key='MTH_TM_ID') }} as MTH_TM_ID
        FROM {{ task_instance.xcom_pull(task_ids='sq043.sq043_source.get_hh_dspsbl_incm_qtr', key='parquet') }} j
    """,
    export_params={},
    clear_before_write=True,
)
def include_tm_id():
    """This task adds the MTH_TM_ID to the dataset. """
    pass


@task.parquet(
    task_id="join_1",
    duckdb_conn_id="duckdb-conn",
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq043/join_1.parquet",
    sql="""
        SELECT
            j.EFF_FROM_YR_MTH,
            i.EFF_TO_YR_MTH,
            j.HH_DSPSBL_INCM_MILLNTH_AMT,
            j.MTH_TM_ID,
            i.mth_tm_id as mth_tm_id_nz,
            i.eff_from_yr_mth as eff_from_yr_mth_nz,
            i.eff_to_yr_mth as eff_to_yr_mth_nz,
            i.HH_DSPSBL_INCM_MILLNTH_AMT as HH_DSPSBL_INCM_MILLNTH_AMT_nz,
            i.crnt_f as crnt_f_nz,
            i.insrt_process_tmstmp,
            i.updt_process_tmstmp
        from {{ task_instance.xcom_pull(task_ids='sq043.sq043_source.include_tm_id', key='parquet') }} j
        left join {{ task_instance.xcom_pull(task_ids='sq043.sq043_source.get_hh_dspsbl_incm_qtr', key='parquet') }} i
        on j.MTH_TM_ID = i.MTH_TM_ID
    """,
    export_params={},
    clear_before_write=True,
)
def join_1():
    """Joins main dataset with the DuckLake table data on MTH_TM_ID."""
    pass


@task.parquet(
    task_id="update_crnt_f",
    duckdb_conn_id="duckdb-conn",
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq043/update_crnt_f.parquet",
    sql="""
        SELECT
            MTH_TM_ID_NZ as MTH_TM_ID,
            EFF_FROM_YR_MTH_NZ as EFF_FROM_YR_MTH,
            EFF_TO_YR_MTH_NZ as EFF_TO_YR_MTH,
            HH_DSPSBL_INCM_MILLNTH_AMT_NZ as HH_DSPSBL_INCM_MILLNTH_AMT,
            'N' AS CRNT_F,
            INSRT_PROCESS_TMSTMP,  -- keep the original INSRT_PROCESS_TMSTMP
            CURRENT_TIMESTAMP as UPDT_PROCESS_TMSTMP
        from {{ task_instance.xcom_pull(task_ids='sq043.sq043_enrichment.join_1', key='parquet') }}
        where mth_tm_id_nz is not null and HH_DSPSBL_INCM_MILLNTH_AMT != HH_DSPSBL_INCM_MILLNTH_AMT_NZ
    """,
    export_params={},
    clear_before_write=True,
)
def update_crnt_f():
    """The existing records in IIAS that need updates must have their CRNT_F set to 'N'."""
    pass


@task.parquet(
    task_id="get_update_records",
    duckdb_conn_id="duckdb-conn",
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq043/get_update_records.parquet",
    sql="""select
            MTH_TM_ID_NZ as MTH_TM_ID,
            EFF_FROM_YR_MTH_NZ as EFF_FROM_YR_MTH,
            '999912' as EFF_TO_YR_MTH,
            HH_DSPSBL_INCM_MILLNTH_AMT,
            'Y' AS CRNT_F,
            CURRENT_TIMESTAMP as INSRT_PROCESS_TMSTMP,  -- since we're inserting a NEW VERSION of an EXISTING RECORD, new INSRT_PROCESS_TMSTMP
            NULL as UPDT_PROCESS_TMSTMP
        from {{ task_instance.xcom_pull(task_ids='sq043.sq043_enrichment.join_1', key='parquet') }}
        where mth_tm_id_nz is not null and HH_DSPSBL_INCM_MILLNTH_AMT != HH_DSPSBL_INCM_MILLNTH_AMT_NZ
    """,
    export_params={},
    clear_before_write=True,
)
def get_update_records():
    """Gets records for which the `MTH_TM_ID` exists in the DuckLake table, but other columns need update."""
    pass


@task.parquet(
    task_id="get_new_records",
    duckdb_conn_id="duckdb-conn",
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq043/get_new_records.parquet",
    sql="""select
            MTH_TM_ID,
            EFF_FROM_YR_MTH,
            '999912' as EFF_TO_YR_MTH,
            HH_DSPSBL_INCM_MILLNTH_AMT,
            'Y' as CRNT_F,
            CURRENT_TIMESTAMP as INSRT_PROCESS_TMSTMP,
            null as UPDT_PROCESS_TMSTMP
        from {{ task_instance.xcom_pull(task_ids='sq043.sq043_enrichment.join_1', key='parquet') }}
        where mth_tm_id_nz is null
    """,
    export_params={},
    clear_before_write=True,
)
def get_new_records():
    """Gets net new records (specifically those with net new `MTH_TM_ID`)."""
    pass


@task.duckdb(
    task_id="delete_if_exists",
    duckdb_conn_id="duckdb-conn",
    sql="""
        DELETE FROM ingestion.HH_DSPSBL_INCM_QTR
        WHERE MTH_TM_ID IN (SELECT MTH_TM_ID FROM {{ task_instance.xcom_pull(task_ids='sq043.sq043_enrichment.get_update_records', key='parquet') }})
        AND CRNT_F = 'Y';
    """,
)
def delete_if_exists():
    """Deletes existing records in IIAS that need to be updated (i.e. have the same MTH_TM_ID as the new records)."""
    pass


@task.duckdb(
    task_id="load_hh_dspsbl_incm_qtr",
    duckdb_conn_id="duckdb-conn",
    sql="""
        INSERT INTO ingestion.HH_DSPSBL_INCM_QTR BY NAME
        SELECT * FROM {{ task_instance.xcom_pull(task_ids='sq043.sq043_enrichment.update_crnt_f', key='parquet') }}
        UNION ALL
        SELECT * FROM {{ task_instance.xcom_pull(task_ids='sq043.sq043_enrichment.get_update_records', key='parquet') }}
        UNION ALL
        SELECT * FROM {{ task_instance.xcom_pull(task_ids='sq043.sq043_enrichment.get_new_records', key='parquet') }}
    """,
)
def load_hh_dspsbl_incm_qtr():
    """Loads new and updated records into IIAS table EDRTLRP1D.HH_DSPSBL_INCM_QTR."""
    pass


""" TaskFlow function definitions """
include_tm_id = include_tm_id()
join_1 = join_1()
update_crnt_f = update_crnt_f()
get_update_records = get_update_records()
get_new_records = get_new_records()
delete_if_exists = delete_if_exists()
load_hh_dspsbl_incm_qtr = load_hh_dspsbl_incm_qtr()

""" Dependency chaining """
include_tm_id >> join_1
join_1 >> [
    update_crnt_f, 
    get_update_records,
    get_new_records,
    delete_if_exists 
] >> load_hh_dspsbl_incm_qtr
