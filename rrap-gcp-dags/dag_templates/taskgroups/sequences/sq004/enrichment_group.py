import pyarrow as pa
from airflow.sdk import task


@task.parquet(
    task_id="join_1_cust_id",
    duckdb_conn_id="duckdb-conn",
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq004/join_1_cust_id.parquet",
    sql="""
    SELECT acct_num, src_sys_cd, mth_end_dt, primary_acct_holder_f as prim_cust_f,
            cust_acct_rltnp_type_cd as rel_cd, B.BASEL_CUST_ID 
    FROM "{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq004/airb_cust_acct_rltnp.parquet" AS A -- comes from jb0042
    LEFT OUTER JOIN ingestion.BASEL_CUST_DIM AS B ON A.cust_id = B.CUST_CID
    """,
    export_params={},
    clear_before_write=True,
)
def join_1_cust_id():
    pass


@task.beeline(
    task_id="converted_ks_to_tsys_accts",
    beeline_conn_id="edlr-conn",
    sql="""
    SELECT 
        bcm_acct_num,
        tsys_acct_id,
        client_prod_cd,
        tsys_prod_cd,
        emp_cd,
        ks_plastic_card_num,
        bcm_prod_cd,
        bcm_sub_prod_cd,
        bcm_block_reclass,
        tsys_cust_id,
        tsys_cust_type_cd,
        tsys_plastic_card_num,
        transfer_from_acct_num,
        bns_cust_id,
        conversion_dt,
        end_of_chain_indicator,
        businesseffectivedate
    FROM {{ params.EDL_schema_tsz }}.kq_tkq_ks_tsys_xref
    WHERE businesseffectivedate IN ('2024-11-09', '2025-08-16') AND end_of_chain_indicator='Y';
    """,
    schema=pa.schema([
        ('bcm_acct_num', pa.string()),
        ('tsys_acct_id', pa.string()),
        ('client_prod_cd', pa.string()),
        ('tsys_prod_cd', pa.string()),
        ('emp_cd', pa.string()),
        ('ks_plastic_card_num', pa.string()),
        ('bcm_prod_cd', pa.string()),
        ('bcm_sub_prod_cd', pa.string()),
        ('bcm_block_reclass', pa.string()),
        ('tsys_cust_id', pa.string()),
        ('tsys_cust_type_cd', pa.string()),
        ('tsys_plastic_card_num', pa.string()),
        ('transfer_from_acct_num', pa.string()),
        ('bns_cust_id', pa.string()),
        ('conversion_dt', pa.string()), 
        ('end_of_chain_indicator', pa.string()),
        ('businesseffectivedate', pa.date64())
    ]),
    target="converted_ks_to_tsys_accts.parquet",
    rundir="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq004",
    to_parquet=True,
    tmpfileloc="/bns/rrap/data/tmp"
)
def converted_ks_to_tsys_accts():
    pass


@task.parquet(
    task_id="exclude_converted_tsys_accts",
    duckdb_conn_id="duckdb-conn",
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq004/exclude_converted_tsys_accts.parquet",
    sql="""
    SELECT * 
    FROM "{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq004/join_1_cust_id.parquet"
    WHERE lpad(trim(acct_num), 23, '0') NOT IN
    (
        SELECT lpad(trim(tsys_acct_id), 23, '0') 
        FROM "{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq004/converted_ks_to_tsys_accts.parquet"
    )
    """,
    export_params={},
    clear_before_write=True,
)
def exclude_converted_tsys_accts():
    pass


@task.parquet(
    task_id="replace_tsys_acct_ids",
    duckdb_conn_id="duckdb-conn",
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq004/replace_tsys_acct_ids.parquet",
    sql="""
    SELECT
        COALESCE(xref.bcm_acct_num, a.acct_num) as acct_num,
        a.src_sys_cd, 
        a.mth_end_dt, 
        a.prim_cust_f, 
        a.rel_cd,
        a.BASEL_CUST_ID 
    FROM "{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq004/exclude_converted_tsys_accts.parquet" as a
    LEFT JOIN (
            SELECT bcm_acct_num, tsys_acct_id  -- no need to do anyval bc of AND end_of_chain_indicator='Y';
            FROM "{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq004/converted_ks_to_tsys_accts.parquet"
            GROUP BY bcm_acct_num, tsys_acct_id
        ) as xref
    ON 
        lpad(trim(a.acct_num), 23, '0') = lpad(trim(xref.tsys_acct_id), 23, '0')
    """,
    export_params={},
    clear_before_write=True,
)
def replace_tsys_acct_ids():
    pass


@task.parquet(
    task_id="join_2_acct_id",
    duckdb_conn_id="duckdb-conn",
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq004/join_2_acct_id.parquet",
    sql="""
    SELECT A.*, B.BASEL_ACCT_ID
    FROM "{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq004/replace_tsys_acct_ids.parquet" AS A
    LEFT OUTER JOIN ingestion.BASEL_ACCT_DIM AS B
    ON 
    lpad(trim(A.acct_num), 23, '0') = lpad(trim(B.SRC_APP_ID), 23, '0')
    AND 
    (CASE A.src_sys_cd 
        WHEN 'KQ' THEN 'KS'
        WHEN 'KQ_TSYS' THEN 'KS'
        WHEN 'GZ' THEN 'MO'
        WHEN 'SL' THEN 'SPL'
        END) = trim(B.SRC_APP_CD)
    WHERE 
        B.BASEL_ACCT_ID IS NOT NULL
    """,
    export_params={},
    clear_before_write=True,
)
def join_2_acct_id():
    pass


@task.parquet(
    task_id="join_3_mth_tm",
    duckdb_conn_id="duckdb-conn",
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq004/join_3_mth_tm.parquet",
    sql="""
    SELECT A.*, B.TM_ID AS MTH_TM_ID
    FROM "{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq004/join_2_acct_id.parquet" AS A
    INNER JOIN ingestion.TM_DIM as B
    ON A.mth_end_dt = B.TM_LVL_END_DT
    """,
    export_params={},
    clear_before_write=True,
)
def join_3_mth_tm():
    pass


@task.parquet(
    task_id="cleanup_tables",
    duckdb_conn_id="duckdb-conn",
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq004/BASEL_CUST_ACCT_RLTNP_SNAPSHOT.parquet",
    sql="""
    SELECT COALESCE(BASEL_CUST_ID, -1) AS BASEL_CUST_ID, COALESCE(BASEL_ACCT_ID, 1) AS BASEL_ACCT_ID, 
    COALESCE(MTH_TM_ID, -1) AS MTH_TM_ID, PRIM_CUST_F, REL_CD, CURRENT_TIMESTAMP AS INSRT_PROCESS_TMSTMP,
    CURRENT_TIMESTAMP AS UPDT_PROCESS_TMSTMP
    FROM "{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq004/join_3_mth_tm.parquet"
    WHERE NOT (
        BASEL_CUST_ID is NULL AND MTH_TM_ID IN 
            ( 
            SELECT DISTINCT MTH_TM_ID
            FROM "{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq004/join_3_mth_tm.parquet"
            WHERE MTH_END_DT = '{{ var.value.MTH_END_DT }}' AND MTH_TM_ID IS NOT NULL
            )
    )
    """,
    export_params={},
    clear_before_write=True,
)
def cleanup_tables():
    pass


@task.duckdb(
    task_id="load_basel_cust_acct_rltnp_snapshot",
    duckdb_conn_id="duckdb-conn",
    sql="""
    INSERT INTO ingestion.BASEL_CUST_ACCT_RLTNP_SNAPSHOT BY NAME
    SELECT * FROM "{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq004/BASEL_CUST_ACCT_RLTNP_SNAPSHOT.parquet"
    """,
)
def load_basel_cust_acct_rltnp_snapshot():
    pass


""" TaskFlow function calls """
join_1_cust_id = join_1_cust_id()
converted_ks_to_tsys_accts = converted_ks_to_tsys_accts()
exclude_converted_tsys_accts = exclude_converted_tsys_accts()
replace_tsys_acct_ids = replace_tsys_acct_ids()
join_2_acct_id = join_2_acct_id()
join_3_mth_tm = join_3_mth_tm()
cleanup_tables = cleanup_tables()
load_basel_cust_acct_rltnp_snapshot = load_basel_cust_acct_rltnp_snapshot()

""" Dependency chaining """
[ 
    converted_ks_to_tsys_accts, 
    join_1_cust_id 
] >> exclude_converted_tsys_accts

exclude_converted_tsys_accts >> replace_tsys_acct_ids
replace_tsys_acct_ids >> join_2_acct_id
join_2_acct_id >> join_3_mth_tm
join_3_mth_tm >> cleanup_tables
cleanup_tables >> load_basel_cust_acct_rltnp_snapshot

