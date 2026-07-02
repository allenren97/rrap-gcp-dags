UPSTREAM_ASSET = [
    "features.BASEL_ACCT_ID",
    "features.HELOC_F",
    "features.LTV_TP_CD",
    "features.SCRTY_TP_DESC",
    "features.PRD_ID",
    "features.MAX_ACCT_BAL_AMT",
    "features.ACCT_SENRTY_CD",
    "ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT",
    "ingestion.MORT_MTH_SNAPSHOT",
    "ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT",
    "ingestion.TNG_ACCT_MO",
    "ingestion.BASEL_ACCT_DIM",
    "instruments.CRNT_PRPTY_VAL_AMT",
    "instruments.EXPSR_AT_DFT_RTO",
    "instruments.CRNT_LTV_RTO",
]
DOWNSTREAM_ASSET = "instruments.INDEXED_CCLTV_RTO"
DEPENDENCIES = {
    "duckdb_clear": ["export_result"],
    "export_result": ["duckdb_load"],
}


def duckdb_clear(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    DELETE FROM {DOWNSTREAM_ASSET} 
    WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    AND STREAM =  '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}'
    
    """,
):
    pass


def export_result(
    duckdb_conn_id="duckdb-conn",
    config_file="indexed_ccltv_rto.export_result.sql",
    config_type="instrument",
):
    pass


def duckdb_load(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} BY NAME
    FROM (
        select
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' as OBSN_DT,
            BASEL_ACCT_ID,
            INDEXED_CCLTV_RTO,
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}' as STREAM
        from '{{{{ task_instance.xcom_pull(task_ids="fact__indexed_ccltv_rto.export_result", key="parquet") }}}}'
        )
    """,
):
    pass
