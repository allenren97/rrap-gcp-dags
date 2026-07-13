from airflow.sdk import task
from airflow.exceptions import AirflowException
from bns.rrap.hooks.duckdb import DuckLakeHook


@task.parquet(
    task_id="extract_basel_acct_dim_cols",
    duckdb_conn_id="duckdb-conn",
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq0051/basel_acct_dim.parquet",
    sql="""
        SELECT acct_num, min(basel_acct_id) FROM (SELECT lpad(trim(acct_num),23,'0') as acct_num, basel_acct_id
        FROM ingestion.BASEL_ACCT_DIM 
        WHERE SRC_SYS_DEL_F = 'N' AND 
            src_app_cd IN ('KS','MO','SPL','TNG-MOR') AND 
            src_sys_del_dt = '9999-12-31 00:00:00' GROUP BY acct_num, basel_acct_id) GROUP BY acct_num;
    """,
    export_params={},
    clear_before_write=True,
)
def extract_basel_acct_dim_cols():
    """    This task pulls Account Numbers and Basel Account IDs for all portfolios from ingestion.BASEL_ACCT_DIM' and writes to 'basel_acct_dim.parquet'."""
    pass


@task.parquet(
    task_id="join_1_acct_id",
    duckdb_conn_id="duckdb-conn",
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq0051/join_1_acct_id.parquet",
    sql="""
        SELECT A.*, B.basel_acct_id 
        FROM '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq0051/basel_acct_dim.parquet' AS B
        JOIN '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq0051/airb_ifrs9_ecl_profile_fact.parquet' AS A
        ON lpad(trim(A.acct_num),23,'0') = lpad(trim(B.acct_num),23,'0'))
    """,
    export_params={},
    clear_before_write=True,
)
def join_1_acct_id():
    """ This task INNER JOINs 'basel_acct_dim.parquet' with 'airb_ifrs9_ecl_profile_fact.parquet' table on account number to pull in `basel_acct_id` column. """
    pass


@task.parquet(
    task_id="join_2_mth_tm",
    duckdb_conn_id="duckdb-conn",
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq0051/join_2_mth_tm.parquet",
    sql="""
        SELECT A.*, B.TM_ID AS MTH_TM_ID
        FROM '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq0051/join_1_acct_id.parquet' AS A
        JOIN ingestion.TM_DIM as B
        ON A.MTH_END_DT = B.TM_LVL_END_DT
        WHERE B.TM_LVL='Month' AND B.TM_LVL_END_DT='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="PREV_MTH_END_DT") }}';
    """,
    export_params={},
    clear_before_write=True,
)
def join_2_mth_tm():
    """ This task INNER JOINs 'join_1_acct_id.parquet' with ingestion.TM_DIM on month end date to pull in `mth_tm_id` column.  """
    pass


@task.parquet(
    task_id="extract_secrtztn_os_adj_factr",
    duckdb_conn_id="duckdb-conn",
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq0051/secrtztn_os_adj_factr.parquet",
    sql="""
        SELECT A.BASEL_ACCT_ID_CCAR_MATCHED, B.SECRTZTN_OS_ADJ_FACTR
        FROM ingestion.BASEL_CC_SEC_ACCT_MTH_SNAP A
        LEFT JOIN ingestion.BASEL_SEC_ADJ_FACTR_MTH_SNAP B
        ON CAST(B.SECRTZTN_TP_CD AS VARCHAR(20)) = A.SECRTZTN_TP_CD
        WHERE A.MTH_TM_ID = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="PREV_MTH_TM_ID") }} and
        B.MTH_TM_ID = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="PREV_MTH_TM_ID") }};
    """,
    export_params={},
    clear_before_write=True,
)
def extract_secrtztn_os_adj_factr():
    """ This task pulls `SECRTZTN_OS_ADJ_FACTR` and `BASEL_ACCT_ID_CCAR_MATCHED` columns from ingestion.BASEL_SEC_ADJ_FACTR_MTH_SNAP 
    and ingestion.BASEL_CC_SEC_ACCT_MTH_SNAP (respectively), and joins the two tables on securitization type code. """
    pass


@task.parquet(
    task_id="join_3_final_ecl_cad_drawn",
    duckdb_conn_id="duckdb-conn",
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq0051/join_3_final_ecl_cad_drawn.parquet",
    sql="""
        SELECT
            cntry_cd,
            case when cpp_entity_folio_cd = 'CAN_BNK' then lpad(trim(acct_num),23,'0') else acct_num end as acct_num,
            basel_acct_id,
            cpp_entity_folio_cd,
            cpp_prd_folio_cd,
            cpp_quali_sub_cd,
            cpp_quanti_sub_cd,
            pit_stat_cd,
            stg3_ind,
            os_bal_amt,
            final_ecl_stage,
            final_ecl_cad,
            final_ecl_cad_drawn,
            final_ecl_cad_undrawn,
            crnt_auth_lmt_amt,
            undrawn_amt,
            scored_unscored_ind,
            mth_tm_id,
            src_sys_cd,
            coalesce(round(final_ecl_cad_drawn * (1-DB2.SECRTZTN_OS_ADJ_FACTR), 10), final_ecl_cad_drawn) AS final_ecl_cad_drawn_postsec,
            CURRENT_TIMESTAMP AS INSRT_PROCESS_TMSTMP,
            CURRENT_TIMESTAMP AS UPDT_PROCESS_TMSTMP
        FROM '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq0051/join_2_mth_tm.parquet' AS A
        LEFT JOIN '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq0051/secrtztn_os_adj_factr.parquet' AS DB2
        ON A.basel_acct_id = DB2.BASEL_ACCT_ID_CCAR_MATCHED
    """,
    export_params={},
    clear_before_write=True,
)
def join_3_final_ecl_cad_drawn():
    """ This task LEFT JOINs 'join_2_mth_tm.parquet' with 'secrtztn_os_adj_factr.parquet' on Basel account ID to pull the Final 
    ECL CAD Drawn Post-Securitization field from the latter. """
    pass


@task.duckdb(
    task_id="delete_if_exists",
    duckdb_conn_id="duckdb-conn",
    sql="""
        DELETE FROM ingestion.BASEL_IFRS9_ECL_PROFILE_FACT
        WHERE MTH_TM_ID = {{ task_instance.xcom_pull(task_ids="handle_month_context", key="MTH_TM_ID") }};
    """,
)
def delete_if_exists():
    """ This task deletes any existing records in the target table (ingestion.BASEL_IFRS9_ECL_PROFILE_FACT) matching the month being processed. 
    This is a precaution to prevent duplicates in case of re-runs."""
    pass


@task.duckdb(
    task_id="load_basel_ifrs9_ecl_profile_fact",
    duckdb_conn_id="duckdb-conn",
    sql="""
        INSERT INTO ingestion.BASEL_IFRS9_ECL_PROFILE_FACT BY NAME
        SELECT * FROM '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq0051/join_3_final_ecl_cad_drawn.parquet'
    """,
)
def load_basel_ifrs9_ecl_profile_fact():
    """ This task loads the final output parquet from 'join_3_final_ecl_cad_drawn.parquet' into the ingestion.BASEL_IFRS9_ECL_PROFILE_FACT table in DuckDB."""
    pass


@task.parquet(
    task_id="get_duplicate_basel_acct_id",
    duckdb_conn_id="duckdb-conn",
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq0051/dupe_check_basel_acct_id.parquet",
    sql="""
        SELECT BASEL_ACCT_ID, COUNT(*) as cnt
        FROM '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq0051/join_3_final_ecl_cad_drawn.parquet'
        WHERE BASEL_ACCT_ID IS NOT NULL
        GROUP BY BASEL_ACCT_ID
        HAVING COUNT(*) > 1
    """,
    export_params={},
    clear_before_write=True,
)
def check_duplicate_basel_acct_id():
    pass


@task.parquet(
    task_id="get_duplicate_acct_num",
    duckdb_conn_id="duckdb-conn",
    target="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq0051/dupe_check_acct_num.parquet",
    sql="""
        SELECT ACCT_NUM, COUNT(*) as cnt
        FROM '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq0051/join_3_final_ecl_cad_drawn.parquet'
        WHERE ACCT_NUM IS NOT NULL
        GROUP BY ACCT_NUM
        HAVING COUNT(*) > 1
    """,
    export_params={},
    clear_before_write=True,
)
def check_duplicate_acct_num():
    pass


@task
def check_duplicate_results():
    """ Check the results of the duplicate check tasks and raise an exception if duplicates are found."""
    hook = DuckLakeHook(duckdb_conn_id="duckdb-conn")
    results = hook.sql("""
    SELECT COUNT(*) as cnt FROM (
        SELECT * FROM '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq0051/dupe_check_basel_acct_id.parquet'
        UNION ALL
        SELECT * FROM '{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq0051/dupe_check_acct_num.parquet'
    )""")
    count = results.to_df()["cnt"][0]
    if count > 0:
        raise AirflowException(f"Duplicate records found in duplicate check tasks. Total duplicates: {count}")


""" TaskFlow function definitions """
extract_basel_acct_dim_cols = extract_basel_acct_dim_cols()
join_1_acct_id = join_1_acct_id()
join_2_mth_tm = join_2_mth_tm()
extract_secrtztn_os_adj_factr = extract_secrtztn_os_adj_factr
join_3_final_ecl_cad_drawn = join_3_final_ecl_cad_drawn()
delete_if_exists = delete_if_exists()
load_basel_ifrs9_ecl_profile_fact = load_basel_ifrs9_ecl_profile_fact()
check_duplicate_basel_acct_id = check_duplicate_basel_acct_id()
check_duplicate_acct_num = check_duplicate_acct_num()
check_duplicate_results = check_duplicate_results()

""" Dependency chaining """
extract_basel_acct_dim_cols >> join_1_acct_id >> join_2_mth_tm
join_2_mth_tm >> extract_secrtztn_os_adj_factr >> join_3_final_ecl_cad_drawn >> delete_if_exists >> load_basel_ifrs9_ecl_profile_fact
join_3_final_ecl_cad_drawn >> check_duplicate_basel_acct_id >> check_duplicate_results
join_3_final_ecl_cad_drawn >> check_duplicate_acct_num >> check_duplicate_results
