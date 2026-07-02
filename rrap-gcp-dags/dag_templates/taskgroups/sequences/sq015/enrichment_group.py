from airflow.sdk import task
from bns.rrap.hooks.duckdb import DuckLakeHook


@task
def get_max_org_unit_id():
    """ This task gets the max ORG_UNIT_ID from ingestion table to be used for incrementing new ORG_UNIT_ID's. """
    hook = DuckLakeHook(duckdb_conn_id="duckdb-conn")
    result = hook.sql("""
        SELECT MAX(ORG_UNIT_ID) as max_id
        FROM ingestion.ORG_UNIT_DIM 
    """)
    return result.to_df()["max_id"][0]


@task.parquet(
    task_id="join_org_unit_id",
    duckdb_conn_id="duckdb-conn",
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq015/join_org_unit_id.parquet",
    sql="""
        SELECT
            o.ORG_UNIT_ID,
            o.ORG_UNIT_LVL,
            o.INSRT_PROCESS_TMSTMP,
            rcrr.TRNST_NUM,
            rcrr.ORG_UNIT_NM,
            rcrr.CITY,
            rcrr.OPN_DT,
            rcrr.CLS_DT,
            rcrr.PH_NUM,
            rcrr.POSTAL_ZIP_CD,
            rcrr.PROV_STATE,
            rcrr.STRT_ADDR,
            o.ORG_UNIT_NM as ORG_UNIT_NM_NZ,
            o.CITY as CITY_NZ,
            o.OPN_DT as OPN_DT_NZ,
            o.CLS_DT as CLS_DT_NZ,
            o.PH_NUM as PH_NUM_NZ,
            o.POSTAL_ZIP_CD as POSTAL_ZIP_CD_NZ
        FROM '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq015/rcrr_tnif_hierarchy.parquet' rcrr
        LEFT OUTER JOIN ingestion.ORG_UNIT_DIM o
        ON rcrr.TRNST_NUM = o.trnst_num
    """,
    export_params={},
    clear_before_write=True,
)
def join_org_unit_id():
    """ 
    This task left outer joins numerous columns from the parquet created in source_group and ingestion.ORG_UNIT_DIM on the transit number column. 
    This data is written to sq015_join_org_unit_id.parquet. 
    """
    pass


@task.parquet(
    task_id="org_unit_dim_update",
    duckdb_conn_id="duckdb-conn",
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq015/org_unit_dim_update.parquet",
    sql="""
        SELECT
            ORG_UNIT_ID,
            ORG_UNIT_LVL,
            INSRT_PROCESS_TMSTMP,
            NOW() AS UPDT_PROCESS_TMSTMP,
            TRNST_NUM,
            ORG_UNIT_NM,
            CITY,
            (CASE WHEN OPN_DT = '0001-01-01' THEN NULL ELSE OPN_DT END) AS OPN_DT,
            (CASE WHEN CLS_DT = '0001-01-01' THEN NULL ELSE CLS_DT END) AS CLS_DT,
            trim(replace(PH_NUM, '-', '')) as PH_NUM,
            POSTAL_ZIP_CD,
            PROV_STATE,
            STRT_ADDR
        FROM '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq015/join_org_unit_id.parquet'
        WHERE ORG_UNIT_ID IS NOT NULL
        AND
            (
                TRIM(ORG_UNIT_NM) != TRIM(ORG_UNIT_NM_NZ)
                OR TRIM(CITY) != TRIM(CITY_NZ)
                OR OPN_DT != OPN_DT_NZ
                OR CLS_DT != CLS_DT_NZ
                OR TRIM(PH_NUM) != TRIM(PH_NUM_NZ)
                OR TRIM(POSTAL_ZIP_CD) != TRIM(POSTAL_ZIP_CD_NZ)
            )
        -- Exclude records where both version of the attribute are NULL
        AND NOT (
            (ORG_UNIT_NM IS NULL AND ORG_UNIT_NM_NZ IS NULL)
            AND (CITY IS NULL AND CITY_NZ IS NULL)
            AND (OPN_DT IS NULL AND OPN_DT_NZ IS NULL)
            AND (CLS_DT IS NULL AND CLS_DT_NZ IS NULL)
            AND (PH_NUM IS NULL AND PH_NUM_NZ IS NULL)
            AND (POSTAL_ZIP_CD IS NULL AND POSTAL_ZIP_CD_NZ IS NULL)
        )
    """,
    export_params={},
    clear_before_write=True,
)
def org_unit_dim_update():
    """ This task extracts numerous updated records from join_org_unit_id.parquet where ORG_UNIT_ID is not NULL and other conditions. 
    This data is written to org_unit_dim_update.parquet. """
    pass


@task.parquet(
    task_id="org_unit_dim_new",
    duckdb_conn_id="duckdb-conn",
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq015/org_unit_dim_new.parquet",
    sql="""
        SELECT 
            {{ task_instance.xcom_pull(task_ids='sq015.sq015_enrichment.get_max_org_unit_id') }} + ROW_NUMBER() over (order by TRNST_NUM) as ORG_UNIT_ID,
            NULL AS ORG_UNIT_LVL,
            NOW() AS INSRT_PROCESS_TMSTMP,
            NULL AS UPDT_PROCESS_TMSTMP,
            TRNST_NUM,
            ORG_UNIT_NM,
            CITY,
            (CASE WHEN OPN_DT = '0001-01-01' THEN NULL ELSE OPN_DT END) AS OPN_DT,
            (CASE WHEN CLS_DT = '0001-01-01' THEN NULL ELSE CLS_DT END) AS CLS_DT,
            trim(replace(PH_NUM, '-', '')) as PH_NUM,
            POSTAL_ZIP_CD,
            PROV_STATE,
            STRT_ADDR
        FROM '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq015/join_org_unit_id.parquet'
        WHERE ORG_UNIT_ID IS NULL
    """,
    export_params={},
    clear_before_write=True,
)
def org_unit_dim_new():
    """ This task extracts new records from join_org_unit_id.parquet where ORG_UNIT_ID is NULL. This data is written to org_unit_dim_new.parquet. """
    pass


@task.duckdb(
    task_id="delete_where_org_unit_dim_update",
    duckdb_conn_id="duckdb-conn",
    sql="""
        DELETE FROM ingestion.ORG_UNIT_DIM
        WHERE TRNST_NUM IN (
            SELECT TRNST_NUM 
            FROM '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq015/org_unit_dim_update.parquet'
        )
    """,
)
def delete_where_org_unit_dim_update():
    """ This task deletes records from ingestion.ORG_UNIT_DIM where transit number is in org_unit_dim_update.parquet. 
    This is done to prepare for re-inserting updated records with same ORG_UNIT_ID's. """
    pass


@task.duckdb(
    task_id="insert_updated_records",
    duckdb_conn_id="duckdb-conn",
    sql="""
        INSERT INTO ingestion.ORG_UNIT_DIM BY NAME
        SELECT * FROM '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq015/org_unit_dim_update.parquet'
    """,
)
def insert_updated_records():
    """ This task inserts updated records from org_unit_dim_update.parquet into ingestion.ORG_UNIT_DIM. """
    pass


@task.duckdb(
    task_id="insert_new_records",
    duckdb_conn_id="duckdb-conn",
    sql="""
        INSERT INTO ingestion.ORG_UNIT_DIM BY NAME
        SELECT * FROM '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq015/org_unit_dim_new.parquet'
     """,
)
def insert_new_records():
    """ This task inserts new records from org_unit_dim_new.parquet into ingestion.ORG_UNIT_DIM. """
    pass


""" TaskFlow function definitions """
get_max_org_unit_id = get_max_org_unit_id()
join_org_unit_id = join_org_unit_id()
org_unit_dim_update = org_unit_dim_update()
org_unit_dim_new = org_unit_dim_new()
delete_where_org_unit_dim_update = delete_where_org_unit_dim_update()
insert_updated_records = insert_updated_records()
insert_new_records = insert_new_records()

""" Dependency chaining """
get_max_org_unit_id >> org_unit_dim_new
join_org_unit_id >> [ 
    org_unit_dim_update,
    org_unit_dim_new
]
org_unit_dim_update >> delete_where_org_unit_dim_update
delete_where_org_unit_dim_update >> insert_updated_records
insert_updated_records >> insert_new_records
