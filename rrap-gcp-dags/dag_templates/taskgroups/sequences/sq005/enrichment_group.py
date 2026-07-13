from airflow.sdk import task
from bns.rrap.hooks.duckdb import DuckLakeHook


@task.parquet(
    task_id="join_1_mth_tm_id",
    duckdb_conn_id="duckdb-conn",
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq005/join_1_mth_tm_id.parquet",
    sql="""
    SELECT 
    A.*, -- will also include MTH_END_DT which we will remove before upload
    B.TM_ID AS MTH_TM_ID
    FROM "{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq005/airb_step_pln_mth_snapshot.parquet" A
    INNER JOIN ingestion.TM_DIM as B
    ON A.MTH_END_DT = B.TM_LVL_END_DT
    """,
    export_params={},
    clear_before_write=True,
)
def join_1_mth_tm_id():
    pass


@task.parquet(
    task_id="join_2_cust_id",
    duckdb_conn_id="duckdb-conn",
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq005/join_2_cust_id.parquet",
    # TODO: should we be using -1 or 0 - in IIAS it uses 0... but the BSTM says to use -1
    sql="""
    SELECT 
    A.*,
    CASE 
        WHEN B.BASEL_CUST_ID IS NULL                               THEN -1  -- No match
        WHEN A.PRIM_CUST_CID IS NULL OR TRIM(A.PRIM_CUSTCID) = '' THEN -2  -- Match on a NULL or empty PRIM_CUST_CID
        ELSE B.BASEL_CUST_ID                                                -- Match, and PRIM_CUST_CID is valid
    END AS PRIM_BASEL_CUST_ID
    FROM "{{ task_instance.xcom_pull(task_ids='sq005.sq005.join_1_mth_tm_id', key='parquet') }}" AS A
    LEFT JOIN ingestion.BASEL_CUST_DIM AS B
    ON lpad(trim(A.PRIM_CUST_CID), 23, '0') = lpad(trim(B.CUST_CID), 23, '0') -- CUST_CID is already trimmed
    """,
    export_params={},
    clear_before_write=True,
)
def join_2_cust_id():
    pass


@task.parquet(
    task_id="join_3_br_loctn_ou_id",
    duckdb_conn_id="duckdb-conn",
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq005/join_3_br_loctn_ou_id.parquet",
    sql="""
    SELECT
        A.*,
        CASE 
            WHEN B.ORG_UNIT_ID IS NULL THEN -1
            ELSE B.ORG_UNIT_ID
        END AS BR_LOCTN_OU_ID
    FROM '{{ task_instance.xcom_pull(task_ids='sq005.sq005.join_2_cust_id', key='parquet') }}' A
    LEFT JOIN ingestion.ORG_UNIT_DIM B
    ON trim(A.BR_LOCTN_TRNST) = trim(CAST(B.TRNST_NUM AS VARCHAR))
    """,
    export_params={},
    clear_before_write=True,
)


@task
def get_max_step_plan_snapshot_id():
    """ This task extracts the max STEP_PLN_SNAPSHOT_ID to be used for ID generation """
    hook = DuckLakeHook(duckdb_conn_id="duckdb-conn")
    result = hook.execute("""
    SELECT 
        MAX(STEP_PLN_SNAPSHOT_ID) AS max_id
    FROM ingestion.BASEL_STEP_PLN_MTH_SNAPSHOT
    WHERE
        MTH_TM_ID = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="PREV_MTH_TM_ID") }}'
    """)
    return result.to_df()["max_id"][0]


@task.parquet(
    task_id="join_3_br_loctn_ou_id_with_id",
    duckdb_conn_id="duckdb-conn",
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq005/join_3_br_loctn_ou_id_with_id.parquet",
    sql="""
    SELECT
        {{{{ task_instance.xcom_pull(task_ids='sq005.sq005_enrichment.get_max_step_plan_snapshot_id') }}}} + row_number() over (order by PRIM_BASEL_CUST_ID) as STEP_PLN_SNAPSHOT_ID,
        A.*,
        CASE
            WHEN B.ORG_UNIT_ID IS NULL THEN -1
            ELSE B.ORG_UNIT_ID
        END AS BR_LOCTN_OU_ID,
        FROM '{{ task_instance.xcom_pull(task_ids='sq005.sq005_enrichment.join_2_cust_id', key='parquet') }}' A
        LEFT JOIN ingestion.ORG_UNIT_DIM B
        ON trim(A.BR_LOCTN_TRNST) = trim(CAST(B.TRNST_NUM AS VARCHAR))
    """,
    export_params={},
    clear_before_write=True,
)
def join_3_br_loctn_ou_id_with_id():
    pass


@task.update(
    task_id="update_basel_step_pln_mth_snapshot",
    duckdb_conn_id="duckdb-conn",
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq005/basel_step_pln_mth_snapshot.parquet",
    source="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq005/join_3_br_loctn_ou_id_with_id.parquet",
    sql="""
    SET 
        CRFC_NUM = COALESCE(CRFC_NUM, ''),
        INSURER_CD = COALESCE(INSURER_CD, ''),
        ALI_PRD_CD = COALESCE(ALI_PRD_CD, ''),
        PRPTY_PROV_CD = COALESCE(PRPTY_PROV_CD, '')
    WHERE MTH_TM_ID = {{ task_instance.xcom_pull(task_ids='handle_month_context', key='MTH_TM_ID') }} 
    AND (
        CRFC_NUM IS NULL
        OR INSURER_CD IS NULL
        OR ALI_PRD_CD IS NULL
        OR PRPTY_PROV_CD IS NULL
    )
    """,
    export_params={},
    clear_before_write=True,
)
def update_basel_step_pln_mth_snapshot():
    pass


@task.duckdb(
    task_id="load_basel_step_pln_mth_snapshot",
    duckdb_conn_id="duckdb-conn",
    sql="""
    INSERT INTO ingestion.BASEL_STEP_PLN_MTH_SNAPSHOT BY NAME
    SELECT * EXCEPT (MTH_END_DT)
    FROM '{{ task_instance.xcom_pull(task_ids='sq005.sq005_enrichment.update_basel_step_pln_mth_snapshot', key='parquet') }}'
    """,
)
def load_basel_step_pln_mth_snapshot():
    pass


""" TaskFlow function calls"""
join_1_mth_tm_id = join_1_mth_tm_id()
join_2_cust_id = join_2_cust_id()
get_max_step_plan_snapshot_id = get_max_step_plan_snapshot_id()
join_3_br_loctn_ou_id_with_id = join_3_br_loctn_ou_id_with_id()
update_basel_step_pln_mth_snapshot = update_basel_step_pln_mth_snapshot()
load_basel_step_pln_mth_snapshot = load_basel_step_pln_mth_snapshot()

""" Dependency chaining """
join_1_mth_tm_id >> join_2_cust_id
[
    join_2_cust_id, 
    get_max_step_plan_snapshot_id
] >> join_3_br_loctn_ou_id_with_id
join_3_br_loctn_ou_id_with_id >> update_basel_step_pln_mth_snapshot >> load_basel_step_pln_mth_snapshot
