from airflow.sdk import task
from airflow.exceptions import AirflowException
from bns.rrap.hooks.duckdb import DuckLakeHook


@task
def get_max_basel_acct_id():
    """
    This task queries the max BASEL_ACCT_ID from IIAS_BASEL_ACCT_DIM and pushes the value to XCom for downstream use.
    """
    hook = DuckLakeHook(duckdb_conn_id='duckdb-conn')
    result = hook.get_first("SELECT MAX(BASEL_ACCT_ID) AS max_id FROM ingestion.BASEL_ACCT_DIM")
    return result.to_df()['max_id'][0]


@task.parquet(
    task_id="join_01",
    duckdb_conn_id="duckdb-conn",
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq003/join_01.parquet",
    sql="""
    SELECT
        airb.acct_num::text as ACCT_NUM,
        airb.src_sys_cd as SRC_SYS_CD,
        lpad(trim(airb.acct_num), 23, '0') as APP_ID,
        CASE airb.src_sys_cd 
            WHEN 'KQ' THEN 'KS' 
            WHEN 'GZ' THEN 'MO' 
            WHEN 'SL' THEN 'SPL'
            WHEN 'TNG_MTG' THEN 'TNG-MOR'
            WHEN 'TSYS' THEN 'KS'
            WHEN 'KQ_TSYS' THEN 'KS'
        END as APP_CD,
        iias.basel_acct_id as BASEL_ACCT_ID
    FROM '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq003/airb_acct_dim.parquet' as airb
    LEFT JOIN ingestion.BASEL_ACCT_DIM as iias
        ON lpad(trim(airb.acct_num), 23, '0') = lpad(trim(iias.app_id), 23, '0')
        AND trim(airb.src_sys_cd) = trim(iias.app_cd)
    WHERE iias.basel_acct_id is null
    """,
    export_params={},
    clear_before_write=True,
)
def join_01():
    """
    This task filters out any existing accounts (i.e. accounts with existing BASEL_ACCT_IDs) by joining airb_acct_dim.parquet 
    with BASEL_ACCT_DIM on account number (APP_ID) and source system code (APP_CD).
    """
    pass


@task.parquet(
    task_id="join_02",
    duckdb_conn_id="duckdb-conn",
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq003/join_02.parquet",
    sql="""
    SELECT j.* 
    FROM '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq003/join_01.parquet' as j
    LEFT JOIN  '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq003/kq_tkq_ks_tsys_xref.parquet' as m1
        ON lpad(trim(j.ACCT_NUM), 23, '0') = lpad(trim(m1.bcm_acct_num), 23, '0')
    LEFT JOIN  '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq003/kq_tkq_ks_tsys_xref.parquet' as m2
        ON lpad(trim(j.ACCT_NUM), 23, '0') = lpad(trim(m2.tsys_acct_id), 23, '0')
    WHERE m1.bcm_acct_num is null AND m2.tsys_acct_id is null
    """,
    export_params={},
    clear_before_write=True,
)
def join_02():
    """
    Similar to 'join_xref' in jb0031 TSYS_net_new_accts_report, new records with missing account numbers in the migrated account/cross-reference 
    table are **filtered out** and written to 'join_02.parquet'.
    """
    pass


@task.parquet(
    task_id="xfm_01",
    duckdb_conn_id="duckdb-conn",
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq003/basel_acct_dim.parquet",
    sql="""
    SELECT
    {{ task_instance.xcom_pull(task_ids='sq003.sq003_enrichment.get_max_basel_acct_id') }} + ROW_NUMBER() over (order by ACCT_NUM, SRC_SYS_CD) as BASEL_ACCT_ID,
    null as CIS_PRD_CD,
    CASE WHEN SRC_SYS_CD = 'TNG_MTG' THEN ACCT_NUM ELSE RTRIM(APP_ID) END as ACCT_NUM,
    CASE WHEN SRC_SYS_CD = 'TNG_MTG' THEN ACCT_NUM ELSE APP_ID END as SRC_APP_ID,
    null as INTG_LAYER_SRC_ID,
    '9999-12-31' as SRC_SYS_DEL_DT,
    APP_CD as SRC_APP_CD,
    null as INTG_LAYER_SRC_TBL_NM,
    'N' as SRC_SYS_DEL_F,
    now() as INSRT_PROCESS_TMSTMP,
    now() as UPDT_PROCESS_TMSTMP
    FROM '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq003/join_02.parquet'
    """,
    export_params={},
    clear_before_write=True,
)
def xfm_01():
    """ 
    This task generates new BASEL_ACCT_ID values for new records from 'join_02.parquet' using the max BASEL_ACCT_ID from BASEL_ACCT_DIM
     as a starting point, and writes to 'basel_acct_dim.parquet'.
    """


@task
def approve_load() -> None:
    """ Auto-failing task to prevent BASEL_ACCT_DIM from being loaded before review. """
    raise AirflowException("Approval required to load data to BASEL_ACCT_DIM. Please review the contents of 'basel_acct_dim.parquet' before approving.")


@task.duckdb(
    task_id="load_to_base_acct_dim",
    duckdb_conn_id="duckdb-conn",
    sql="""
    INSERT INTO ingestion.BASEL_ACCT_DIM
    SELECT * FROM '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq003/basel_acct_dim.parquet'
    """,
)
def load_to_base_acct_dim():
    """
    This task loads the new BASEL_ACCT_DIM records from 'basel_acct_dim.parquet' into the BASEL_ACCT_DIM table in DuckDB.
    The task is set to run only after manual approval to ensure data quality checks can be performed on the generated parquet file before loading.
    """
    pass


""" TaskFlow function calling """
get_max_basel_acct_id = get_max_basel_acct_id()
join_01 = join_01()
join_02 = join_02()
xfm_01 = xfm_01()
approve_load = approve_load()
load_to_base_acct_dim = load_to_base_acct_dim()

""" Dependency chaining """
join_01 >> join_02 >> xfm_01 >> load_to_base_acct_dim
get_max_basel_acct_id >> xfm_01
approve_load >> load_to_base_acct_dim
