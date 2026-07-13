UPSTREAM_ASSET = [
    "ingestion.BASEL_ACCT_PRFM_FACT",
    "ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT",
    "features.BASEL_ACCT_ID",
    "ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT",
    "ingestion.MORT_MTH_SNAPSHOT",
    "ingestion.TNG_ACCT_MO",
    "ingestion.BASEL_ACCT_DIM",
]

DOWNSTREAM_ASSET = "instruments.RNTL_PRPTY_F"
DEPENDENCIES = {
    "export_result": ["duckdb_clear"],
    "duckdb_clear": ["duckdb_load"],
}


def export_result(
    duckdb_conn_id="duckdb-conn",
    config_file="rntl_prpty_f.export_result.sql",
    config_type="instrument",
):
    pass


def duckdb_clear(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    DELETE FROM {DOWNSTREAM_ASSET} 
    WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    AND STREAM = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}'
    """,
):
    pass


def duckdb_load(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} BY NAME
    FROM (
        SELECT
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' as OBSN_DT,
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}' as STREAM,
            SRC_SYS_CD,
            BASEL_ACCT_ID,
            RNTL_PRPTY_F
        FROM 
            '{{{{ task_instance.xcom_pull(task_ids="fact__rntl_prpty_f.export_result", key="parquet") }}}}'
        )
    """,
):
    pass
