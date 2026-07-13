from airflow.sdk import task
from bns.rrap.hooks.duckdb import DuckLakeHook


@task
def get_max_basel_cust_id():
    """
    Task to get the current max BASEL_CUST_ID in BASEL_CUST_DIM, which will be used as the starting point for generating 
    new BASEL_CUST_ID values for net new accounts in the enrichment tasks.
    """
    hook = DuckLakeHook(duckdb_conn_id="duckdb-conn")

    sql = """
        SELECT MAX(BASEL_CUST_ID) as max_id
        FROM ingestion.BASEL_CUST_DIM
    """

    result = hook.duckdb.sql(sql)

    return result.to_df()["max_id"][0]


@task.parquet(
    task_id="join_1",
    duckdb_conn_id="duckdb-conn",
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq002/join_1.parquet",
    sql="""
    SELECT
        A.CUST_ID,
        A.CUST_TP_CD,
        B.BASEL_CUST_ID,
        B.CIF_KEY,
        B.CUST_CID,
        B.CUST_TP_CD_NZ,
        B.IP_ID,
        B.CIS_PURGED_F,
        B.CIS_PURGED_DT,
        B.INSRT_PROCESS_TMSTMP,
        B.UPDT_PROCESS_TMSTMP
    FROM '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq002/airb_cust_dim.parquet' as A
    LEFT OUTER JOIN ingestion.BASEL_CUST_DIM as B
    ON A.CUST_ID = B.CUST_CID
    """,
    export_params={},
    clear_before_write=True,
)
def join_1():
    """
    This task performs a left outer join between the AIRB_CUST_DIM dataset generated from source tasks and the existing BASEL_CUST_DIM table in PROD/IIAS, 
    to identify which accounts are net new (no match in BASEL_CUST_DIM) vs. which accounts are existing but may have changes (matched on CUST_ID but different CUST_TP_CD).
    The result is written to 'join_1.parquet' and will be used as the basis for subsequent enrichment tasks.
    """
    pass


@task.parquet(
    task_id="xfm_01_new",
    duckdb_conn_id="duckdb-conn",
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq002/xfm_01_new.parquet",
    sql="""
    SELECT                 
        {{ task_instance.xcom_pull(task_ids='sq002.sq002_enrichment.get_max_basel_cust_id') }} + ROW_NUMBER() over (order by CUST_ID, CUST_TP_CD) as BASEL_CUST_ID, -- is this the right way to add new acct ids?
        NULL AS CIF_KEY,
        CUST_ID AS CUST_CID,                           -- direct move from AIRB_CUST_DIM
        CUST_TP_CD,        -- direct move from AIRB_CUST_DIM
        NULL AS IP_ID,
        'N' AS CIS_PURGED_F,
        NULL AS CIS_PURGED_DT,
        now() as INSRT_PROCESS_TMSTMP,
        now() as UPDT_PROCESS_TMSTMP
    FROM '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq002/join_1.parquet'
    WHERE BASEL_CUST_ID IS NULL
    """,
    export_params={},
    clear_before_write=True,
)
def xfm_01_new():
    """
    This task pulls 'join_1.parquet' contents to generate new BASEL_CUST_ID values (just an index) where BASEL_CUST_ID is null 
    (i.e. for accounts that failed the 'join_1' join - net new accounts) and writes to 'xfm_01_new.parquet'.

    if IsNull(LNK_JOIN_1.BASEL_CUST_ID)  then generate dataset for new record insertion.
    """
    pass


@task.parquet(
    task_id="xfm_01_update",
    duckdb_conn_id="duckdb-conn",
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq002/xfm_01_update.parquet",
    sql="""
    SELECT                 
        BASEL_CUST_ID,
        CIF_KEY,                            -- same
        CUST_CID,                           -- same
        CUST_TP_CD,        -- updated with AIRB_CUST_DIM
        IP_ID,                              -- same
        CIS_PURGED_F,                       -- same
        CIS_PURGED_DT,                      -- same
        INSRT_PROCESS_TMSTMP,               -- same
        now() as UPDT_PROCESS_TMSTMP        -- updated 
    FROM '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq002/join_1.parquet'
    WHERE BASEL_CUST_ID IS NOT NULL AND -- UPDATING EXISTING NEW CUST_TP_CD
        CUST_TP_CD != CUST_TP_CD_NZ
    """,
    export_params={},
    clear_before_write=True,
)
def xfm_01_update():
    """
    This task pulls 'join_1.parquet' contents and filters for records where 'BASEL_CUST_ID' is not null and customer type code doesn't match 
    existing PROD/IIAS data, and then writes to 'xfm_01_update.parquet'.

    if IsNotNull(LNK_JOIN_1.BASEL_CUST_ID) and (LNK_JOIN_1.CUST_TP_CD <> LNK_JOIN_1.CUST_TP_CD_NZ) then generate dataset for existing record update.
    """
    pass


@task.duckdb(
    task_id="delete_old_basel_cust_dim_records",
    duckdb_conn_id="duckdb-conn",
    sql="""
        DELETE FROM ingestion.BASEL_CUST_DIM
        WHERE BASEL_CUST_ID IN (SELECT BASEL_CUST_ID 
        FROM '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq002/xfm_01_update.parquet')
    """,
)
def delete_old_basel_cust_dim_records():
    """
    Task to delete old records from BASEL_CUST_DIM that are being updated, to prevent duplicates when we re-insert updated records in the next step.
    """
    pass


@task.duckdb(
    task_id="insert_updated_basel_cust_dim_records",
    duckdb_conn_id="duckdb-conn",
    sql="""
        INSERT INTO ingestion.BASEL_CUST_DIM
        SELECT * FROM '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq002/xfm_01_update.parquet'
    """,
)
def insert_updated_basel_cust_dim_records():
    """
    Task to insert updated records into BASEL_CUST_DIM for accounts that had changes (e.g. customer type code changes) but are not net new accounts.
    """
    pass


@task.duckdb(
    task_id="insert_new_basel_cust_dim_records",
    duckdb_conn_id="duckdb-conn",
    sql="""
        INSERT INTO ingestion.BASEL_CUST_DIM
        SELECT * FROM '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq002/xfm_01_new.parquet'
    """,
)
def insert_new_basel_cust_dim_records():
    """
    Task to insert new records into BASEL_CUST_DIM for net new accounts.
    """
    pass


""" TaskFlow function calls """
get_max_basel_cust_id = get_max_basel_cust_id()
join_1 = join_1()
xfm_01_new = xfm_01_new()
xfm_01_update = xfm_01_update()
delete_old_basel_cust_dim_records = delete_old_basel_cust_dim_records()
insert_updated_basel_cust_dim_records = insert_updated_basel_cust_dim_records()
insert_new_basel_cust_dim_records = insert_new_basel_cust_dim_records()

""" Dependency chaining"""
get_max_basel_cust_id >> join_1
join_1 >> [
    xfm_01_new, 
    xfm_01_update
]
xfm_01_update >> delete_old_basel_cust_dim_records >> insert_updated_basel_cust_dim_records
insert_updated_basel_cust_dim_records >> insert_new_basel_cust_dim_records
xfm_01_new >> insert_new_basel_cust_dim_records
