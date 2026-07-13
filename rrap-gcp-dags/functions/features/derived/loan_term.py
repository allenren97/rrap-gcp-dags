UPSTREAM_ASSET = [ 'ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT' ]
DOWNSTREAM_ASSET = 'features.LOAN_TERM'
DEPENDENCIES = {
    'duckdb_clear_loan_term': ['duckdb_derive_loan_term'],
}


def duckdb_clear_loan_term(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
        DELETE FROM { DOWNSTREAM_ASSET } 
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass


def duckdb_derive_loan_term(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    INSERT INTO { DOWNSTREAM_ASSET }
    BY NAME FROM (
        SELECT
            BASEL_ACCT_ID,
            LOAN_TERM,
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' as OBSN_DT
        FROM { UPSTREAM_ASSET[0] }
        WHERE MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    )
    """
):
    pass

